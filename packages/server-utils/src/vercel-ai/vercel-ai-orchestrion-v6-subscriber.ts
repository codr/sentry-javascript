import type { Span } from '@sentry/core';
import { debug, getActiveSpan, SPAN_STATUS_ERROR, withActiveSpan } from '@sentry/core';
import { DEBUG_BUILD } from '../debug-build';
import { CHANNELS } from '../orchestrion/channels';
import { bindTracingChannelToSpan, type TracingChannelPayloadWithSpan } from '../tracing-channel';
import {
  clearOperationId,
  createSpanFromMessage,
  enrichSpanOnEnd,
  type VercelAiChannelMessage,
  type VercelAiChannelOptions,
  type VercelAiTracingChannelFactory,
} from './vercel-ai-dc-subscriber';

/**
 * v6 channel adapter for the Vercel AI (`ai`) SDK.
 *
 * `ai` >= 7 publishes a normalized `ai:telemetry` tracing channel natively
 * (consumed by `subscribeVercelAiTracingChannel`). v6 has no such channel, so
 * orchestrion injects `orchestrion:ai:*` channels around the top-level
 * functions (see `orchestrion/config.ts`). The injected channels carry only the
 * wrapped call's `{ arguments, result, error }` — NOT v7's normalized `event`
 * object — so this adapter reconstructs an equivalent {@link VercelAiChannelMessage}
 * from v6's argument/result shapes and delegates to the SAME span-building core
 * (`createSpanFromMessage` / `enrichSpanOnEnd`) the v7 subscriber uses, so the
 * emitted spans are identical between v6 and v7.
 *
 * Like the v7 subscriber, each operation channel is wired up via
 * {@link bindTracingChannelToSpan}, which binds the opened span into the runtime's
 * async context for the duration of the traced call and ends it when the call
 * settles. That binding is what makes nested operations (the model call below)
 * parent to the enclosing `invoke_agent` span without any manual span-stack
 * bookkeeping.
 *
 * The model call (`languageModelCall` / `generate_content` span) has no
 * injectable definition in `ai`, so we instead wrap `resolveLanguageModel` (the
 * single chokepoint every model call flows through) and monkey-patch
 * `doGenerate`/`doStream` on the returned model. `resolveLanguageModel` runs
 * synchronously inside the enclosing operation's body, so the active span at
 * that point is the operation span — we capture it as the explicit parent for
 * the model's (async) `doGenerate`/`doStream` spans, which run after the bound
 * context has unwound.
 */

/** Shape orchestrion's transform attaches to the tracing-channel context. */
interface OrchestrionContext {
  arguments: unknown[];
  result?: unknown;
  error?: unknown;
}

/** Builds the normalized message for a channel from the wrapped call's first-arg options. */
type MessageBuilder = (options: Record<string, unknown>, telemetry: Record<string, unknown>) => VercelAiChannelMessage;

/** A resolved `ai` language model — has `doGenerate`/`doStream` and identity fields. */
interface ResolvedModel {
  modelId?: string;
  provider?: string;
  doGenerate?: (...args: unknown[]) => Promise<unknown>;
  doStream?: (...args: unknown[]) => Promise<unknown>;
}

const PATCHED = Symbol('SentryVercelAiModelPatched');
const PARENT = Symbol('SentryVercelAiModelParent');

/** A resolved model with our patch bookkeeping (idempotency flag + captured parent span). */
type PatchableModel = ResolvedModel & { [PATCHED]?: boolean; [PARENT]?: Span };

// Per-operation correlation id. No Date/random (unavailable / non-deterministic) — a counter is enough.
let callIdCounter = 0;
function nextCallId(): string {
  return `v6-${++callIdCounter}`;
}

// The message built on `start` for each operation, keyed by the (stable-identity) channel context, so
// the `beforeSpanEnd` handler can enrich the span from the settled result and clear the `callId` maps.
const messages = new WeakMap<object, VercelAiChannelMessage>();

let subscribed = false;

/**
 * Subscribe the v6 orchestrion channel adapter. Safe to always call: inert on
 * `ai` >= 7 (those channels are never published) and when orchestrion injection
 * isn't active. Idempotent.
 *
 * `tracingChannel` is the platform-provided factory (the same one passed to
 * `subscribeVercelAiTracingChannel`); `options` pins the recording settings at
 * subscribe time so we never look the integration up per event.
 */
export function subscribeVercelAiOrchestrionChannels(
  tracingChannel: VercelAiTracingChannelFactory,
  options: VercelAiChannelOptions = {},
): void {
  if (subscribed) {
    return;
  }
  subscribed = true;

  try {
    bindOperation(tracingChannel, CHANNELS.VERCEL_AI_GENERATE_TEXT, buildTextMessage('generateText'), options);
    bindOperation(tracingChannel, CHANNELS.VERCEL_AI_STREAM_TEXT, buildTextMessage('streamText'), options);
    bindOperation(
      tracingChannel,
      CHANNELS.VERCEL_AI_EMBED,
      (callOptions, telemetry) => ({
        type: 'embed',
        event: {
          callId: nextCallId(),
          ...modelFields(callOptions.model),
          maxRetries: callOptions.maxRetries,
          value: callOptions.value,
          ...recording(telemetry),
        },
      }),
      options,
    );
    bindOperation(
      tracingChannel,
      CHANNELS.VERCEL_AI_EXECUTE_TOOL_CALL,
      (callOptions, telemetry) => ({
        type: 'executeTool',
        // v6 carries the tool definitions on the executeToolCall args (a record keyed by name);
        // the shared core reads the matching tool's `description` for the span.
        event: {
          callId: nextCallId(),
          toolCall: callOptions.toolCall,
          tools: callOptions.tools,
          ...recording(telemetry),
        },
      }),
      options,
    );
    subscribeResolveLanguageModel(tracingChannel, CHANNELS.VERCEL_AI_RESOLVE_LANGUAGE_MODEL, options);
  } catch {
    DEBUG_BUILD && debug.log('Vercel AI orchestrion channel subscription failed.');
  }
}

/**
 * Bind one operation channel: `getSpan` opens a span from the message reconstructed out of the wrapped
 * call's first argument; `beforeSpanEnd` enriches it from the settled result (tokens, output messages,
 * finish reasons, …) and drops the per-operation `callId` maps before the helper ends the span.
 */
function bindOperation(
  tracingChannel: VercelAiTracingChannelFactory,
  channelName: string,
  build: MessageBuilder,
  options: VercelAiChannelOptions,
): void {
  bindTracingChannelToSpan(
    tracingChannel<OrchestrionContext>(channelName),
    (data: TracingChannelPayloadWithSpan<OrchestrionContext>) => {
      const callOptions = isRecord(data.arguments[0]) ? data.arguments[0] : {};
      const telemetry = isRecord(callOptions.experimental_telemetry) ? callOptions.experimental_telemetry : {};
      const message = build(callOptions, telemetry);
      messages.set(data, message);
      return createSpanFromMessage(message, options);
    },
    {
      beforeSpanEnd: (span, data) => {
        const message = messages.get(data);
        if (!message) {
          return;
        }
        // The helper's `error` handler already set the span status; only enrich from a successful result.
        if (!('error' in data)) {
          message.result = data.result;
          enrichSpanOnEnd(span, message, options);
        }
        clearOperationId(message);
        messages.delete(data);
      },
    },
  );
}

/**
 * `resolveLanguageModel` returns the model every call flows through. We don't span it — on `end` we
 * monkey-patch `doGenerate`/`doStream` on the returned model so each invocation produces a
 * `languageModelCall` span parented to the enclosing invoke_agent span (the active span here, since
 * this runs synchronously inside the operation body bound by `bindTracingChannelToSpan`).
 */
function subscribeResolveLanguageModel(
  tracingChannel: VercelAiTracingChannelFactory,
  channelName: string,
  options: VercelAiChannelOptions,
): void {
  tracingChannel<OrchestrionContext>(channelName).subscribe({
    end(rawCtx) {
      const ctx = rawCtx as OrchestrionContext;
      if (!isRecord(ctx.result)) {
        return;
      }
      const model = ctx.result as PatchableModel;
      // Capture/refresh the parent for this resolve, so a model reused across operations spans its
      // calls under the right invoke_agent span.
      const parent = getActiveSpan();
      if (parent) {
        model[PARENT] = parent;
      }
      if (!model[PATCHED]) {
        model[PATCHED] = true;
        patchModelMethod(model, 'doGenerate', options);
        patchModelMethod(model, 'doStream', options);
      }
    },
    start() {
      /* no-op */
    },
    asyncStart() {
      /* no-op */
    },
    asyncEnd() {
      /* no-op */
    },
    error() {
      /* no-op */
    },
  });
}

function patchModelMethod(
  model: PatchableModel,
  method: 'doGenerate' | 'doStream',
  options: VercelAiChannelOptions,
): void {
  const original = model[method];
  if (typeof original !== 'function') {
    return;
  }
  model[method] = function (this: unknown, ...args: unknown[]): Promise<unknown> {
    const parent = model[PARENT];
    const callArgs = isRecord(args[0]) ? args[0] : {};
    const message: VercelAiChannelMessage = {
      type: 'languageModelCall',
      event: { provider: model.provider, modelId: model.modelId, tools: callArgs.tools, messages: callArgs.prompt },
    };
    const span = parent
      ? withActiveSpan(parent, () => createSpanFromMessage(message, options))
      : createSpanFromMessage(message, options);
    // `languageModelCall` always opens a span; the guard just keeps the wrapper safe if that changes.
    if (!span) {
      return Promise.resolve(original.apply(this, args));
    }

    let result: Promise<unknown>;
    try {
      result = Promise.resolve(original.apply(this, args));
    } catch (error) {
      span.setStatus({ code: SPAN_STATUS_ERROR, message: error instanceof Error ? error.message : 'unknown_error' });
      span.end();
      throw error;
    }
    // `doStream` resolves to `{ stream, ... }` before the stream is consumed; we end here (start/end
    // bracket the call) to match the channel timing.
    return result.then(
      value => {
        message.result = value;
        enrichSpanOnEnd(span, message, options);
        span.end();
        return value;
      },
      error => {
        span.setStatus({ code: SPAN_STATUS_ERROR, message: error instanceof Error ? error.message : 'unknown_error' });
        span.end();
        throw error;
      },
    );
  };
}

function buildTextMessage(type: 'generateText' | 'streamText'): MessageBuilder {
  return (options, telemetry) => ({
    type,
    event: {
      callId: nextCallId(),
      operationId: type === 'streamText' ? 'ai.streamText' : 'ai.generateText',
      functionId: asString(telemetry.functionId),
      ...modelFields(options.model),
      maxRetries: options.maxRetries,
      messages: options.messages,
      prompt: options.prompt,
      ...recording(telemetry),
    },
  });
}

function recording(telemetry: Record<string, unknown>): { recordInputs: unknown; recordOutputs: unknown } {
  return { recordInputs: telemetry.recordInputs, recordOutputs: telemetry.recordOutputs };
}

function modelFields(model: unknown): { provider?: string; modelId?: string } {
  return { provider: modelField(model, 'provider'), modelId: modelField(model, 'modelId') };
}

function modelField(model: unknown, field: 'modelId' | 'provider'): string | undefined {
  return isRecord(model) ? asString(model[field]) : undefined;
}

function asString(value: unknown): string | undefined {
  return typeof value === 'string' ? value : undefined;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}
