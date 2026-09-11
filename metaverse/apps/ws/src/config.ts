import { randomUUID } from "node:crypto";

function integer(name: string, fallback: number, minimum = 1): number {
  const value = Number(process.env[name] ?? fallback);
  if (!Number.isInteger(value) || value < minimum) {
    throw new Error(`${name} must be an integer greater than or equal to ${minimum}`);
  }
  return value;
}

export const config = {
  nodeEnv: process.env.NODE_ENV ?? "development",
  port: integer("PORT", 8000),
  instanceId: process.env.INSTANCE_ID ?? process.env.HOSTNAME ?? randomUUID(),
  jwtPassword: process.env.JWT_PASSWORD ?? "development-only-secret",
  redisUrl: process.env.REDIS_URL ?? "redis://127.0.0.1:6379",
  heartbeatIntervalMs: integer("HEARTBEAT_INTERVAL_MS", 15_000, 1_000),
  presenceLeaseMs: integer("PRESENCE_LEASE_MS", 45_000, 3_000),
  presenceMetadataGraceMs: integer("PRESENCE_METADATA_GRACE_MS", 60_000, 5_000),
  cleanupIntervalMs: integer("CLEANUP_INTERVAL_MS", 5_000, 1_000),
  dedupTtlMs: integer("DEDUP_TTL_MS", 10 * 60_000, 10_000),
  chatStreamMaxLength: integer("CHAT_STREAM_MAX_LENGTH", 10_000, 100),
  replayLimit: integer("CHAT_REPLAY_LIMIT", 500, 1),
  syncBufferMaxEvents: integer("WS_SYNC_BUFFER_MAX_EVENTS", 1_000, 10),
  maxPayloadBytes: integer("WS_MAX_PAYLOAD_BYTES", 64 * 1024, 1_024),
  maxBufferedBytes: integer("WS_MAX_BUFFERED_BYTES", 1024 * 1024, 1_024),
  drainDeadlineMs: integer("DRAIN_DEADLINE_MS", 20_000, 1_000),
  logLevel: process.env.LOG_LEVEL ?? "info",
  otlpEndpoint: process.env.OTEL_EXPORTER_OTLP_TRACES_ENDPOINT,
  allowTestSpaces: process.env.ALLOW_TEST_SPACES === "true",
};

if (config.nodeEnv === "production" && config.jwtPassword === "development-only-secret") {
  throw new Error("JWT_PASSWORD is required in production");
}

export const JWT_PASSWORD = config.jwtPassword;
export const PORT = config.port;
export const NODE_ENV = config.nodeEnv;
