import pino from "pino";
import { trace } from "@opentelemetry/api";
import { config } from "./config";

export const logger = pino({
  level: config.logLevel,
  base: { service: "skyscope-ws", instanceId: config.instanceId },
  redact: {
    paths: ["token", "*.token", "payload.token", "payload.message", "payload.offer", "payload.ans", "payload.candidate"],
    censor: "[REDACTED]",
  },
  mixin() {
    const context = trace.getActiveSpan()?.spanContext();
    return context ? { traceId: context.traceId, spanId: context.spanId } : {};
  },
});
