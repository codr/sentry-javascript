/**
 * Server-only utilities shared across Sentry server SDKs.
 *
 * @module
 */

export {
  IOREDIS_DC_CHANNEL_COMMAND,
  IOREDIS_DC_CHANNEL_CONNECT,
  REDIS_DC_CHANNEL_BATCH,
  REDIS_DC_CHANNEL_COMMAND,
  REDIS_DC_CHANNEL_CONNECT,
  subscribeRedisDiagnosticChannels,
} from './redis/redis-dc-subscriber';
export type {
  IORedisCommandData,
  RedisBatchData,
  RedisCommandData,
  RedisConnectData,
  RedisDiagnosticChannelResponseHook,
  RedisTracingChannel,
  RedisTracingChannelContextWithSpan,
  RedisTracingChannelFactory,
  RedisTracingChannelSubscribers,
} from './redis/redis-dc-subscriber';

export {
  fastifyIntegration,
  // oxlint-disable-next-line typescript/no-deprecated
  handleFastifyError,
  // oxlint-disable-next-line typescript/no-deprecated
  instrumentFastifyV5,
} from './integrations/tracing-channel/fastify';
