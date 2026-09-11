import { Counter, Gauge, Histogram, Registry, collectDefaultMetrics } from "prom-client";

export const registry = new Registry();
collectDefaultMetrics({ register: registry, prefix: "skyscope_ws_" });

export const activeConnections = new Gauge({
  name: "skyscope_ws_connections",
  help: "Current open WebSocket connections",
  labelNames: ["state"] as const,
  registers: [registry],
});

export const eventsTotal = new Counter({
  name: "skyscope_ws_events_total",
  help: "WebSocket events handled by type and outcome",
  labelNames: ["type", "outcome"] as const,
  registers: [registry],
});

export const eventDuration = new Histogram({
  name: "skyscope_ws_event_duration_seconds",
  help: "Time spent handling WebSocket events",
  labelNames: ["type"] as const,
  buckets: [0.001, 0.003, 0.01, 0.03, 0.1, 0.3, 1, 3],
  registers: [registry],
});

export const duplicatesTotal = new Counter({
  name: "skyscope_ws_duplicates_total",
  help: "Events suppressed by idempotency checks",
  labelNames: ["type"] as const,
  registers: [registry],
});

export const redisErrorsTotal = new Counter({
  name: "skyscope_ws_redis_errors_total",
  help: "Redis operation and connection errors",
  labelNames: ["operation"] as const,
  registers: [registry],
});

export const staleSessionsTotal = new Counter({
  name: "skyscope_ws_stale_sessions_total",
  help: "Expired presence sessions removed by cleanup",
  registers: [registry],
});

export const replayedEventsTotal = new Counter({
  name: "skyscope_ws_replayed_events_total",
  help: "Durable chat events replayed after reconnect",
  registers: [registry],
});

export const outboundDroppedTotal = new Counter({
  name: "skyscope_ws_outbound_dropped_total",
  help: "Outbound messages dropped due to closed or slow sockets",
  labelNames: ["reason"] as const,
  registers: [registry],
});

export const shutdownDuration = new Histogram({
  name: "skyscope_ws_shutdown_duration_seconds",
  help: "Graceful shutdown drain duration",
  buckets: [0.1, 0.5, 1, 2, 5, 10, 20, 30],
  registers: [registry],
});
