import { createHash } from "node:crypto";
import { context, propagation, trace } from "@opentelemetry/api";
import Redis from "ioredis";
import { config } from "./config";
import { logger } from "./logger";
import { redisErrorsTotal } from "./metrics";
import type { DistributedEvent, OutgoingMessage, PresenceSnapshot } from "./types";

const EVENT_CHANNEL = "skyscope:v1:events";
const SPACES_KEY = "skyscope:v1:presence:spaces";
const tracer = trace.getTracer("skyscope-ws");

const claimSessionScript = `
local existingUser = redis.call('HGET', KEYS[1], 'userId')
local existingSpace = redis.call('HGET', KEYS[1], 'spaceId')
if existingUser and (existingUser ~= ARGV[2] or existingSpace ~= ARGV[3]) then
  return {'forbidden'}
end
local oldConnection = redis.call('HGET', KEYS[1], 'connectionId') or ''
local oldX = redis.call('HGET', KEYS[1], 'x')
local oldY = redis.call('HGET', KEYS[1], 'y')
redis.call('ZREMRANGEBYSCORE', KEYS[3], '-inf', ARGV[8])
local alreadyOnline = redis.call('ZCARD', KEYS[3]) > 0 and '1' or '0'
local x = oldX or ARGV[6]
local y = oldY or ARGV[7]
redis.call('HSET', KEYS[1],
  'sessionId', ARGV[1], 'userId', ARGV[2], 'spaceId', ARGV[3],
  'connectionId', ARGV[4], 'instanceId', ARGV[5], 'x', x, 'y', y,
  'updatedAt', ARGV[8], 'expiresAt', ARGV[9])
redis.call('PEXPIRE', KEYS[1], ARGV[10])
redis.call('ZADD', KEYS[2], ARGV[9], ARGV[1])
redis.call('ZADD', KEYS[3], ARGV[9], ARGV[1])
redis.call('SADD', KEYS[4], ARGV[2])
redis.call('SADD', KEYS[5], ARGV[3])
return {'ok', x, y, alreadyOnline, oldConnection, existingUser and '1' or '0'}
`;

const heartbeatScript = `
if redis.call('HGET', KEYS[1], 'connectionId') ~= ARGV[1] then return 0 end
redis.call('HSET', KEYS[1], 'updatedAt', ARGV[3], 'expiresAt', ARGV[4])
redis.call('PEXPIRE', KEYS[1], ARGV[5])
redis.call('ZADD', KEYS[2], ARGV[4], ARGV[2])
redis.call('ZADD', KEYS[3], ARGV[4], ARGV[2])
return 1
`;

const updatePositionScript = `
if redis.call('HGET', KEYS[1], 'connectionId') ~= ARGV[1] then return 0 end
redis.call('HSET', KEYS[1], 'x', ARGV[2], 'y', ARGV[3], 'updatedAt', ARGV[4])
return 1
`;

const releaseSessionScript = `
if redis.call('HGET', KEYS[1], 'connectionId') ~= ARGV[1] then return {'not-owner'} end
local userId = redis.call('HGET', KEYS[1], 'userId') or ''
local spaceId = redis.call('HGET', KEYS[1], 'spaceId') or ''
redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[2], ARGV[2])
redis.call('ZREM', KEYS[3], ARGV[2])
redis.call('ZREMRANGEBYSCORE', KEYS[3], '-inf', ARGV[3])
local offline = '0'
if redis.call('ZCARD', KEYS[3]) == 0 then
  redis.call('SREM', KEYS[4], userId)
  offline = '1'
end
return {'ok', userId, spaceId, offline}
`;

const appendChatScript = `
local existing = redis.call('GET', KEYS[1])
if existing then
  local separator = string.find(existing, '|', 1, true)
  local cursor = string.sub(existing, 1, separator - 1)
  local oldHash = string.sub(existing, separator + 1)
  if oldHash ~= ARGV[2] then return {'conflict'} end
  return {'duplicate', cursor}
end
local cursor = redis.call('XADD', KEYS[2], 'MAXLEN', '~', ARGV[3], '*',
  'eventId', ARGV[1], 'type', ARGV[4], 'userId', ARGV[5],
  'payload', ARGV[6], 'createdAt', ARGV[7])
redis.call('SET', KEYS[1], cursor .. '|' .. ARGV[2], 'PX', ARGV[8])
local eventPayload = cjson.decode(ARGV[6])
eventPayload['userId'] = ARGV[5]
eventPayload['timestamp'] = ARGV[7]
local envelope = {
  origin = ARGV[9],
  roomId = ARGV[10],
  event = {type = ARGV[4], eventId = ARGV[1], streamId = cursor, payload = eventPayload}
}
if ARGV[11] ~= '' then envelope['targetUserId'] = ARGV[11] end
if ARGV[12] ~= '' then envelope['traceparent'] = ARGV[12] end
redis.call('PUBLISH', ARGV[13], cjson.encode(envelope))
return {'accepted', cursor}
`;

type SessionIdentity = {
  sessionId: string;
  connectionId: string;
  userId: string;
  spaceId: string;
};

type ReleasedSession = { released: boolean; userId?: string; spaceId?: string; offline?: boolean };

export class DistributedStore {
  private readonly command = new Redis(config.redisUrl, { lazyConnect: true, maxRetriesPerRequest: 2 });
  private readonly publisher = new Redis(config.redisUrl, { lazyConnect: true, maxRetriesPerRequest: 2 });
  private readonly subscriber = new Redis(config.redisUrl, { lazyConnect: true, maxRetriesPerRequest: null });
  private eventHandler?: (event: DistributedEvent) => void;

  constructor() {
    for (const [name, client] of [["command", this.command], ["publisher", this.publisher], ["subscriber", this.subscriber]] as const) {
      client.on("error", (error) => {
        redisErrorsTotal.inc({ operation: name });
        logger.error({ err: error, redisClient: name }, "Redis error");
      });
    }
  }

  async start(handler: (event: DistributedEvent) => void): Promise<void> {
    this.eventHandler = handler;
    await Promise.all([this.command.connect(), this.publisher.connect(), this.subscriber.connect()]);
    const serverInfo = await this.command.info("server");
    const version = /redis_version:(\d+)\./.exec(serverInfo);
    if (!version || Number(version[1]) < 6) {
      throw new Error(`Redis 6 or newer is required; detected ${version?.[1] ?? "unknown"}`);
    }
    this.subscriber.on("message", (_channel, value) => this.onMessage(value));
    await this.subscriber.subscribe(EVENT_CHANNEL);
  }

  isReady(): boolean {
    return this.command.status === "ready" && this.publisher.status === "ready" && this.subscriber.status === "ready";
  }

  async ping(): Promise<boolean> {
    try {
      return (await this.command.ping()) === "PONG";
    } catch {
      redisErrorsTotal.inc({ operation: "ping" });
      return false;
    }
  }

  async claimSession(input: SessionIdentity & { x: number; y: number }): Promise<{
    x: number;
    y: number;
    alreadyOnline: boolean;
    previousConnectionId?: string;
    resumed: boolean;
  }> {
    const now = Date.now();
    const expiresAt = now + config.presenceLeaseMs;
    const result = await this.command.eval(
      claimSessionScript,
      5,
      this.sessionKey(input.sessionId),
      this.roomSessionsKey(input.spaceId),
      this.userSessionsKey(input.spaceId, input.userId),
      this.roomUsersKey(input.spaceId),
      SPACES_KEY,
      input.sessionId,
      input.userId,
      input.spaceId,
      input.connectionId,
      config.instanceId,
      String(input.x),
      String(input.y),
      String(now),
      String(expiresAt),
      String(config.presenceLeaseMs + config.presenceMetadataGraceMs),
    ) as string[];
    if (result[0] !== "ok") throw new Error("Session belongs to a different user or space");
    return {
      x: Number(result[1]),
      y: Number(result[2]),
      alreadyOnline: result[3] === "1",
      previousConnectionId: result[4] || undefined,
      resumed: result[5] === "1",
    };
  }

  async heartbeat(identity: SessionIdentity): Promise<boolean> {
    const now = Date.now();
    const expiresAt = now + config.presenceLeaseMs;
    const result = await this.command.eval(
      heartbeatScript,
      3,
      this.sessionKey(identity.sessionId),
      this.roomSessionsKey(identity.spaceId),
      this.userSessionsKey(identity.spaceId, identity.userId),
      identity.connectionId,
      identity.sessionId,
      String(now),
      String(expiresAt),
      String(config.presenceLeaseMs + config.presenceMetadataGraceMs),
    );
    return Number(result) === 1;
  }

  async updatePosition(identity: SessionIdentity, x: number, y: number): Promise<boolean> {
    const result = await this.command.eval(
      updatePositionScript,
      1,
      this.sessionKey(identity.sessionId),
      identity.connectionId,
      String(x),
      String(y),
      String(Date.now()),
    );
    return Number(result) === 1;
  }

  async releaseSession(identity: SessionIdentity): Promise<ReleasedSession> {
    const result = await this.command.eval(
      releaseSessionScript,
      4,
      this.sessionKey(identity.sessionId),
      this.roomSessionsKey(identity.spaceId),
      this.userSessionsKey(identity.spaceId, identity.userId),
      this.roomUsersKey(identity.spaceId),
      identity.connectionId,
      identity.sessionId,
      String(Date.now()),
    ) as string[];
    return result[0] === "ok"
      ? { released: true, userId: result[1], spaceId: result[2], offline: result[3] === "1" }
      : { released: false };
  }

  async snapshot(spaceId: string): Promise<PresenceSnapshot[]> {
    const ids = await this.command.zrangebyscore(this.roomSessionsKey(spaceId), Date.now(), "+inf");
    if (ids.length === 0) return [];
    const pipeline = this.command.pipeline();
    ids.forEach((id) => pipeline.hgetall(this.sessionKey(id)));
    const rows = await pipeline.exec();
    const users = new Map<string, PresenceSnapshot & { updatedAt: number }>();
    rows?.forEach(([error, value]) => {
      if (error || !value) return;
      const data = value as Record<string, string>;
      if (!data.userId || Number(data.expiresAt) <= Date.now()) return;
      const current = users.get(data.userId);
      if (!current || Number(data.updatedAt) >= current.updatedAt) {
        users.set(data.userId, { userId: data.userId, x: Number(data.x), y: Number(data.y), updatedAt: Number(data.updatedAt) });
      }
    });
    return [...users.values()].map(({ userId, x, y }) => ({ userId, x, y }));
  }

  async acceptTransient(userId: string, spaceId: string, eventId: string, message: unknown): Promise<"accepted" | "duplicate"> {
    const hash = this.hash(message);
    const key = `skyscope:v1:dedup:${spaceId}:${userId}:${eventId}`;
    const accepted = await this.command.set(key, hash, "PX", config.dedupTtlMs, "NX");
    if (accepted === "OK") return "accepted";
    if ((await this.command.get(key)) !== hash) throw new Error("eventId was reused with different content");
    return "duplicate";
  }

  async appendChat(
    userId: string,
    spaceId: string,
    eventId: string,
    type: "groupChat" | "privateChat",
    payload: Record<string, unknown>,
    targetUserId?: string,
  ): Promise<{ status: "accepted" | "duplicate"; streamId: string }> {
    const hash = this.hash({ type, payload });
    const carrier: Record<string, string> = {};
    propagation.inject(context.active(), carrier);
    const result = await this.command.eval(
      appendChatScript,
      2,
      `skyscope:v1:chat-dedup:${spaceId}:${userId}:${eventId}`,
      this.chatStreamKey(spaceId),
      eventId,
      hash,
      String(config.chatStreamMaxLength),
      type,
      userId,
      JSON.stringify(payload),
      new Date().toISOString(),
      String(config.dedupTtlMs),
      config.instanceId,
      spaceId,
      targetUserId ?? "",
      carrier.traceparent ?? "",
      EVENT_CHANNEL,
    ) as string[];
    if (result[0] === "conflict") throw new Error("eventId was reused with different content");
    return { status: result[0] as "accepted" | "duplicate", streamId: result[1] };
  }

  async chatHighWater(spaceId: string): Promise<string | undefined> {
    const rows = await this.command.xrevrange(this.chatStreamKey(spaceId), "+", "-", "COUNT", 1);
    return rows[0]?.[0];
  }

  async replayChat(spaceId: string, after: string, upTo = "+"): Promise<{ events: OutgoingMessage[]; truncated: boolean; limited: boolean }> {
    const firstRows = await this.command.xrange(this.chatStreamKey(spaceId), "-", "+", "COUNT", 1);
    const truncated = after !== "0-0" && firstRows.length > 0 && compareStreamIds(after, firstRows[0][0]) < 0;
    const replayRows = await this.command.xrange(this.chatStreamKey(spaceId), `(${after}`, upTo, "COUNT", config.replayLimit + 1);
    const limited = replayRows.length > config.replayLimit;
    const rows = replayRows.slice(0, config.replayLimit);
    const events = rows.map(([streamId, fields]) => {
      const record: Record<string, string> = {};
      for (let index = 0; index < fields.length; index += 2) record[fields[index]] = fields[index + 1];
      const storedPayload = JSON.parse(record.payload ?? "{}") as Record<string, unknown>;
      return {
        type: record.type,
        eventId: record.eventId,
        streamId,
        payload: { ...storedPayload, userId: record.userId, timestamp: record.createdAt },
      };
    });
    return { events, truncated, limited };
  }

  async publish(event: DistributedEvent): Promise<void> {
    const carrier: Record<string, string> = {};
    propagation.inject(context.active(), carrier);
    await this.publisher.publish(EVENT_CHANNEL, JSON.stringify({ ...event, traceparent: carrier.traceparent }));
  }

  async cleanupExpired(): Promise<ReleasedSession[]> {
    const now = Date.now();
    const spaces = await this.command.smembers(SPACES_KEY);
    const released: ReleasedSession[] = [];
    for (const spaceId of spaces) {
      const sessionIds = await this.command.zrangebyscore(this.roomSessionsKey(spaceId), "-inf", now, "LIMIT", 0, 100);
      for (const sessionId of sessionIds) {
        const data = await this.command.hgetall(this.sessionKey(sessionId));
        if (!data.userId || !data.connectionId) {
          await this.command.zrem(this.roomSessionsKey(spaceId), sessionId);
          continue;
        }
        if (Number(data.expiresAt) > now) continue;
        const result = await this.releaseSession({ sessionId, connectionId: data.connectionId, userId: data.userId, spaceId });
        if (result.released) released.push(result);
      }
    }
    return released;
  }

  async stop(): Promise<void> {
    await Promise.allSettled([this.subscriber.quit(), this.publisher.quit(), this.command.quit()]);
  }

  private onMessage(value: string): void {
    try {
      const event = JSON.parse(value) as DistributedEvent;
      const parent = event.traceparent ? propagation.extract(context.active(), { traceparent: event.traceparent }) : context.active();
      context.with(parent, () => tracer.startActiveSpan(`redis.receive ${event.event.type}`, (span) => {
        try { this.eventHandler?.(event); } finally { span.end(); }
      }));
    } catch (error) {
      logger.warn({ err: error }, "Ignored invalid distributed event");
    }
  }

  private hash(value: unknown): string {
    return createHash("sha256").update(JSON.stringify(value)).digest("hex");
  }

  private sessionKey(sessionId: string): string { return `skyscope:v1:presence:session:${sessionId}`; }
  private roomSessionsKey(spaceId: string): string { return `skyscope:v1:presence:space:${spaceId}:sessions`; }
  private roomUsersKey(spaceId: string): string { return `skyscope:v1:presence:space:${spaceId}:users`; }
  private userSessionsKey(spaceId: string, userId: string): string { return `skyscope:v1:presence:space:${spaceId}:user:${userId}`; }
  private chatStreamKey(spaceId: string): string { return `skyscope:v1:chat:${spaceId}`; }
}

function compareStreamIds(left: string, right: string): number {
  const [leftMs, leftSequence] = left.split("-").map(BigInt);
  const [rightMs, rightSequence] = right.split("-").map(BigInt);
  if (leftMs !== rightMs) return leftMs > rightMs ? 1 : -1;
  if (leftSequence === rightSequence) return 0;
  return leftSequence > rightSequence ? 1 : -1;
}
