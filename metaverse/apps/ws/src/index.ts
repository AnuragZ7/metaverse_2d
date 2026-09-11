import { createServer } from "node:http";
import { randomUUID } from "node:crypto";
import { performance } from "node:perf_hooks";
import { SpanStatusCode, trace } from "@opentelemetry/api";
import { PrismaClient } from "@prisma/client";
import jwt, { type JwtPayload } from "jsonwebtoken";
import { WebSocketServer } from "ws";
import { config } from "./config";
import { ConnectionRegistry, type LocalConnection } from "./connectionRegistry";
import { DistributedStore } from "./distributedStore";
import { logger } from "./logger";
import {
  duplicatesTotal,
  eventDuration,
  eventsTotal,
  registry as metricsRegistry,
  replayedEventsTotal,
  shutdownDuration,
  staleSessionsTotal,
} from "./metrics";
import { startTelemetry, stopTelemetry } from "./telemetry";
import { incomingMessageSchema, type DistributedEvent, type IncomingMessage, type OutgoingMessage } from "./types";

startTelemetry();

const tracer = trace.getTracer("skyscope-ws");
const prisma = new PrismaClient();
const store = new DistributedStore();
const connections = new ConnectionRegistry();
let draining = false;
let initialized = false;
let shutdownPromise: Promise<void> | undefined;

function json(response: import("node:http").ServerResponse, status: number, body: unknown): void {
  response.writeHead(status, { "content-type": "application/json" });
  response.end(JSON.stringify(body));
}

async function pingDatabase(): Promise<boolean> {
  try {
    await prisma.$queryRaw`SELECT 1`;
    return true;
  } catch {
    return false;
  }
}

const httpServer = createServer(async (request, response) => {
  const path = new URL(request.url ?? "/", "http://localhost").pathname;
  if (path === "/livez") return json(response, 200, { status: "alive" });
  if (path === "/readyz") {
    const redisReady = initialized && store.isReady() && await store.ping();
    const databaseReady = initialized && (config.allowTestSpaces || await pingDatabase());
    const ready = !draining && redisReady && databaseReady;
    return json(response, ready ? 200 : 503, {
      status: ready ? "ready" : "not-ready",
      draining,
      redis: redisReady,
      database: databaseReady,
    });
  }
  if (path === "/metrics") {
    response.writeHead(200, { "content-type": metricsRegistry.contentType });
    response.end(await metricsRegistry.metrics());
    return;
  }
  json(response, 404, { error: "not-found" });
});

const wss = new WebSocketServer({ noServer: true, maxPayload: config.maxPayloadBytes, perMessageDeflate: false });

httpServer.on("upgrade", (request, socket, head) => {
  const path = new URL(request.url ?? "/", "http://localhost").pathname;
  if (draining || (path !== "/" && path !== "/ws")) {
    socket.write("HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\n\r\n");
    socket.destroy();
    return;
  }
  wss.handleUpgrade(request, socket, head, (webSocket) => wss.emit("connection", webSocket, request));
});

wss.on("connection", (ws) => {
  const connection: LocalConnection = {
    connectionId: randomUUID(), x: 0, y: 0, isAlive: true, closed: false, syncing: false, syncBuffer: [], ws,
  };
  connections.addOpen(connection);
  logger.info({ connectionId: connection.connectionId }, "WebSocket connected");

  let processing = Promise.resolve();
  ws.on("pong", () => {
    connection.isAlive = true;
    if (!connection.sessionId || !connection.userId || !connection.spaceId) return;
    void store.heartbeat(identity(connection)).then((owned) => {
      if (!owned && ws.readyState === ws.OPEN) ws.close(1012, "Session ownership changed");
    }).catch((error) => logger.warn({ err: error, connectionId: connection.connectionId }, "Heartbeat update failed"));
  });

  ws.on("message", (data, isBinary) => {
    processing = processing.then(() => handleRawMessage(connection, data, isBinary)).catch((error) => {
      logger.error({ err: error, connectionId: connection.connectionId }, "Unhandled WebSocket message error");
      sendError(connection, "internal-error", "The event could not be processed", true);
    });
  });
  ws.on("error", (error) => logger.warn({ err: error, connectionId: connection.connectionId }, "WebSocket error"));
  ws.once("close", (code) => void closeConnection(connection, code));
});

async function handleRawMessage(connection: LocalConnection, data: import("ws").RawData, isBinary: boolean): Promise<void> {
  if (isBinary) return sendError(connection, "unsupported-data", "Binary messages are not supported");
  let candidate: unknown;
  try {
    candidate = JSON.parse(data.toString());
  } catch {
    eventsTotal.inc({ type: "unknown", outcome: "invalid" });
    return sendError(connection, "invalid-json", "Message must be valid JSON");
  }

  const parsed = incomingMessageSchema.safeParse(candidate);
  if (!parsed.success) {
    const type = typeof candidate === "object" && candidate && "type" in candidate ? String(candidate.type) : "unknown";
    eventsTotal.inc({ type, outcome: "invalid" });
    return sendError(connection, "invalid-event", parsed.error.issues[0]?.message ?? "Invalid event");
  }

  const message = parsed.data;
  const startedAt = performance.now();
  await tracer.startActiveSpan(`ws.${message.type}`, async (span) => {
    span.setAttribute("messaging.operation.type", "process");
    span.setAttribute("messaging.destination.name", message.type);
    try {
      if (message.type === "join") await handleJoin(connection, message);
      else {
        requireJoined(connection);
        await handleJoinedEvent(connection, message);
      }
      eventsTotal.inc({ type: message.type, outcome: "accepted" });
    } catch (error) {
      const publicMessage = error instanceof ClientError ? error.message : "The event could not be processed";
      const code = error instanceof ClientError ? error.code : "internal-error";
      eventsTotal.inc({ type: message.type, outcome: "rejected" });
      span.recordException(error as Error);
      span.setStatus({ code: SpanStatusCode.ERROR, message: code });
      logger.warn({ err: error, eventType: message.type, eventId: "eventId" in message ? message.eventId : undefined }, "Event rejected");
      sendError(connection, code, publicMessage, code === "internal-error");
    } finally {
      eventDuration.observe({ type: message.type }, (performance.now() - startedAt) / 1000);
      span.end();
    }
  });
}

async function handleJoin(connection: LocalConnection, message: Extract<IncomingMessage, { type: "join" }>): Promise<void> {
  if (connection.sessionId) throw new ClientError("already-joined", "This connection already joined a space");
  const payload = message.payload;
  let userId: string;
  try {
    const verified = jwt.verify(payload.token, config.jwtPassword) as JwtPayload | string;
    if (typeof verified === "string" || typeof verified.userId !== "string") throw new Error("JWT has no userId");
    userId = verified.userId;
  } catch {
    throw new ClientError("unauthorized", "The authentication token is invalid");
  }

  const space = config.allowTestSpaces && payload.spaceId.startsWith("loadtest-")
    ? { width: 100, height: 100 }
    : await prisma.space.findFirst({ where: { id: payload.spaceId }, select: { width: true, height: true } });
  if (!space) throw new ClientError("space-not-found", "The requested space does not exist");

  const sessionId = payload.sessionId ?? randomUUID();
  const preferredX = validCoordinate(payload.x, space.width) ? payload.x : Math.floor(space.width / 2);
  const preferredY = validCoordinate(payload.y, space.height) ? payload.y : Math.floor(space.height / 2);
  const claimed = await store.claimSession({ sessionId, connectionId: connection.connectionId, userId, spaceId: payload.spaceId, x: preferredX, y: preferredY });

  connection.sessionId = sessionId;
  connection.userId = userId;
  connection.spaceId = payload.spaceId;
  connection.width = space.width;
  connection.height = space.height;
  connection.x = claimed.x;
  connection.y = claimed.y;
  if (claimed.previousConnectionId && claimed.previousConnectionId !== connection.connectionId) connections.replace(claimed.previousConnectionId);
  connections.beginSync(connection);
  connections.join(connection);

  try {
    // Everything published after joining the local room is buffered. Durable chat
    // up to this high-water mark is replayed first, then duplicate copies are removed.
    const replayHighWater = await store.chatHighWater(payload.spaceId);
    const users = (await store.snapshot(payload.spaceId)).filter((user) => user.userId !== userId);
    connections.send(connection, {
      type: "space-joined",
      payload: { spawn: { x: connection.x, y: connection.y }, userId, sessionId, resumed: claimed.resumed, users },
    });

    const replayedEventIds = new Set<string>();
    if (payload.lastEventId && replayHighWater) {
      const replay = await store.replayChat(payload.spaceId, payload.lastEventId, replayHighWater);
      if (replay.truncated || replay.limited) {
        const reason = replay.truncated ? "chat-retention-exceeded" : "chat-replay-limit-exceeded";
        connections.send(connection, { type: "resync-required", payload: { reason } });
      }
      for (const event of replay.events) {
        if (!canReceiveReplayedEvent(event, userId)) continue;
        connections.send(connection, { ...event, payload: { ...event.payload, replayed: true } });
        if (event.eventId) replayedEventIds.add(event.eventId);
        replayedEventsTotal.inc();
      }
    }
    connections.finishSync(connection, replayedEventIds);
  } catch (error) {
    connections.abortSync(connection);
    connection.ws.close(1011, "Join synchronization failed");
    throw error;
  }

  if (!claimed.alreadyOnline) {
    await publish({ roomId: payload.spaceId, excludeSessionId: sessionId, event: { type: "user-joined", payload: { userId, x: connection.x, y: connection.y } } });
  }
  logger.info({ connectionId: connection.connectionId, sessionId, userId, spaceId: payload.spaceId, resumed: claimed.resumed }, "Session joined");
}

async function handleJoinedEvent(connection: LocalConnection, message: Exclude<IncomingMessage, { type: "join" }>): Promise<void> {
  if (message.type === "move") return handleMove(connection, message);
  if (message.type === "groupChat" || message.type === "privateChat") return handleChat(connection, message);
  return handleSignaling(connection, message);
}

async function handleMove(connection: LocalConnection, message: Extract<IncomingMessage, { type: "move" }>): Promise<void> {
  const { x, y } = message.payload;
  if (!validCoordinate(x, connection.width!) || !validCoordinate(y, connection.height!)) {
    connections.send(connection, { type: "movement-rejected", eventId: message.eventId, payload: { x: connection.x, y: connection.y, reason: "outside-space" } });
    return;
  }
  if (Math.abs(connection.x - x) > 2 || Math.abs(connection.y - y) > 2) {
    connections.send(connection, { type: "movement-rejected", eventId: message.eventId, payload: { x: connection.x, y: connection.y, reason: "too-far" } });
    return;
  }
  const accepted = await store.acceptTransient(connection.userId!, connection.spaceId!, message.eventId, message);
  if (accepted === "duplicate") {
    duplicatesTotal.inc({ type: message.type });
    return sendAck(connection, message.eventId, "duplicate");
  }
  if (!await store.updatePosition(identity(connection), x, y)) throw new ClientError("stale-session", "This session was replaced; reconnect to continue");
  connection.x = x;
  connection.y = y;
  await publish({ roomId: connection.spaceId!, excludeSessionId: connection.sessionId, event: { type: "movement", eventId: message.eventId, payload: { userId: connection.userId, x, y } } });
  sendAck(connection, message.eventId, "accepted");
}

async function handleChat(connection: LocalConnection, message: Extract<IncomingMessage, { type: "groupChat" | "privateChat" }>): Promise<void> {
  const privateTarget = message.type === "privateChat" ? message.payload.userId : undefined;
  if (privateTarget) {
    const users = await store.snapshot(connection.spaceId!);
    if (!users.some((user) => user.userId === privateTarget)) {
      throw new ClientError("recipient-unavailable", "The private-message recipient is not present in this space");
    }
  }
  const payload = message.type === "privateChat" ? { message: message.payload.message, recipientId: privateTarget } : { message: message.payload.message };
  const result = await store.appendChat(connection.userId!, connection.spaceId!, message.eventId, message.type, payload, privateTarget);
  if (result.status === "duplicate") duplicatesTotal.inc({ type: message.type });
  sendAck(connection, message.eventId, result.status, result.streamId);
}

async function handleSignaling(connection: LocalConnection, message: Exclude<IncomingMessage, { type: "join" | "move" | "groupChat" | "privateChat" }>): Promise<void> {
  const accepted = await store.acceptTransient(connection.userId!, connection.spaceId!, message.eventId, message);
  if (accepted === "duplicate") {
    duplicatesTotal.inc({ type: message.type });
    return sendAck(connection, message.eventId, "duplicate");
  }
  const target = "to" in message.payload ? message.payload.to : undefined;
  if (!target) return sendAck(connection, message.eventId, "accepted");
  const typeMap: Record<string, string> = {
    "user:call": "incomming:call", "call:accepted": "call:accepted", "peer:nego:needed": "peer:nego:needed",
    "peer:nego:done": "peer:nego:final", "ice:candidate": "ice:candidate", "call:ended": "call:ended",
  };
  const forwardedPayload = { ...message.payload, from: connection.userId };
  delete (forwardedPayload as { to?: string }).to;
  await publish({ roomId: connection.spaceId!, targetUserId: target, event: { type: typeMap[message.type], eventId: message.eventId, payload: forwardedPayload } });
  sendAck(connection, message.eventId, "accepted");
}

async function closeConnection(connection: LocalConnection, code: number): Promise<void> {
  if (connection.closed) return;
  connection.closed = true;
  connections.remove(connection);
  if (connection.sessionId && connection.userId && connection.spaceId) {
    try {
      const result = await store.releaseSession(identity(connection));
      if (result.offline) await publish({ roomId: connection.spaceId, event: { type: "user-left", payload: { userId: connection.userId } } });
    } catch (error) {
      logger.warn({ err: error, connectionId: connection.connectionId }, "Session release failed; expiry cleanup will retry");
    }
  }
  logger.info({ connectionId: connection.connectionId, sessionId: connection.sessionId, code }, "WebSocket disconnected");
}

function identity(connection: LocalConnection) {
  return { sessionId: connection.sessionId!, connectionId: connection.connectionId, userId: connection.userId!, spaceId: connection.spaceId! };
}

function requireJoined(connection: LocalConnection): void {
  if (!connection.sessionId || !connection.userId || !connection.spaceId) throw new ClientError("join-required", "Join a space before sending this event");
}

function validCoordinate(value: number | undefined, limit: number): value is number {
  return typeof value === "number" && Number.isInteger(value) && value >= 0 && value < limit;
}

function canReceiveReplayedEvent(event: OutgoingMessage, userId: string): boolean {
  if (event.type !== "privateChat") return true;
  return event.payload.userId === userId || event.payload.recipientId === userId;
}

function sendAck(connection: LocalConnection, eventId: string, status: string, streamId?: string): void {
  connections.send(connection, { type: "event-ack", eventId, streamId, payload: { status } });
}

function sendError(connection: LocalConnection, code: string, message: string, retryable = false): void {
  connections.send(connection, { type: "error", payload: { code, message, retryable } });
}

async function publish(event: Omit<DistributedEvent, "origin">): Promise<void> {
  await store.publish({ ...event, origin: config.instanceId });
}

class ClientError extends Error {
  constructor(public readonly code: string, message: string) { super(message); }
}

const heartbeatTimer = setInterval(() => {
  for (const connection of connections.values()) {
    if (!connection.isAlive) {
      connection.ws.terminate();
      continue;
    }
    connection.isAlive = false;
    connection.ws.ping();
  }
}, config.heartbeatIntervalMs);
heartbeatTimer.unref();

const cleanupTimer = setInterval(() => {
  void store.cleanupExpired().then(async (released) => {
    for (const session of released) {
      staleSessionsTotal.inc();
      if (session.offline && session.spaceId && session.userId) await publish({ roomId: session.spaceId, event: { type: "user-left", payload: { userId: session.userId } } });
    }
  }).catch((error) => logger.warn({ err: error }, "Stale-session cleanup failed"));
}, config.cleanupIntervalMs);
cleanupTimer.unref();

async function shutdown(signal: string): Promise<void> {
  if (shutdownPromise) return shutdownPromise;
  shutdownPromise = (async () => {
    const endTimer = shutdownDuration.startTimer();
    draining = true;
    logger.info({ signal, activeConnections: connections.size }, "Graceful shutdown started");
    httpServer.close();
    for (const connection of connections.values()) {
      connections.send(connection, { type: "server-draining", payload: { reconnect: true, retryAfterMs: 250 + Math.floor(Math.random() * 1_000) } });
    }
    const closeDelay = Math.min(5_000, Math.floor(config.drainDeadlineMs / 3));
    const closeTimer = setTimeout(() => {
      for (const connection of connections.values()) connection.ws.close(1012, "Server restarting");
    }, closeDelay);
    await new Promise<void>((resolve) => {
      const started = Date.now();
      const poll = setInterval(() => {
        if (connections.size === 0 || Date.now() - started >= config.drainDeadlineMs) {
          clearInterval(poll);
          resolve();
        }
      }, 50);
    });
    clearTimeout(closeTimer);
    for (const connection of connections.values()) connection.ws.terminate();
    clearInterval(heartbeatTimer);
    clearInterval(cleanupTimer);
    await Promise.allSettled([store.stop(), prisma.$disconnect(), stopTelemetry()]);
    endTimer();
    logger.info({ signal }, "Graceful shutdown completed");
  })();
  return shutdownPromise;
}

process.once("SIGTERM", () => void shutdown("SIGTERM"));
process.once("SIGINT", () => void shutdown("SIGINT"));

store.start((event) => connections.deliver(event)).then(() => {
  initialized = true;
  httpServer.listen(config.port, () => logger.info({ port: config.port }, "WebSocket server ready"));
}).catch(async (error) => {
  logger.fatal({ err: error }, "WebSocket server initialization failed");
  await Promise.allSettled([store.stop(), prisma.$disconnect(), stopTelemetry()]);
  process.exitCode = 1;
});
