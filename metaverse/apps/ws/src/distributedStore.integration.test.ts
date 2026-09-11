import { randomUUID } from "node:crypto";
import Redis from "ioredis";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { DistributedStore } from "./distributedStore";

const redisUrl = process.env.REDIS_URL;
const describeRedis = redisUrl ? describe : describe.skip;

describeRedis("DistributedStore with Redis", () => {
  const store = new DistributedStore();
  const cleanup = new Redis(redisUrl!);
  const unique = randomUUID();
  const spaceId = `test-space-${unique}`;
  const userId = `test-user-${unique}`;
  const sessionId = randomUUID();
  const firstConnection = randomUUID();
  const secondConnection = randomUUID();

  beforeAll(async () => store.start(() => undefined));
  afterAll(async () => {
    let cursor = "0";
    do {
      const [next, keys] = await cleanup.scan(cursor, "MATCH", `skyscope:v1:*${unique}*`, "COUNT", 100);
      cursor = next;
      if (keys.length) await cleanup.del(...keys);
    } while (cursor !== "0");
    await cleanup.srem("skyscope:v1:presence:spaces", spaceId);
    await Promise.all([store.stop(), cleanup.quit()]);
  });

  it("fences an old connection when a session resumes", async () => {
    await store.claimSession({ sessionId, connectionId: firstConnection, userId, spaceId, x: 4, y: 5 });
    const resumed = await store.claimSession({ sessionId, connectionId: secondConnection, userId, spaceId, x: 0, y: 0 });
    expect(resumed.resumed).toBe(true);
    expect([resumed.x, resumed.y]).toEqual([4, 5]);
    expect(await store.releaseSession({ sessionId, connectionId: firstConnection, userId, spaceId })).toEqual({ released: false });
    expect(await store.heartbeat({ sessionId, connectionId: secondConnection, userId, spaceId })).toBe(true);
  });

  it("deduplicates transient events and rejects conflicting reuse", async () => {
    const eventId = `${unique}-movement`;
    const event = { type: "move", payload: { x: 5, y: 5 } };
    expect(await store.acceptTransient(userId, spaceId, eventId, event)).toBe("accepted");
    expect(await store.acceptTransient(userId, spaceId, eventId, event)).toBe("duplicate");
    await expect(store.acceptTransient(userId, spaceId, eventId, { ...event, payload: { x: 6, y: 5 } })).rejects.toThrow(/different content/);
  });

  it("atomically appends one chat record and can replay it", async () => {
    const eventId = `${unique}-chat`;
    const first = await store.appendChat(userId, spaceId, eventId, "groupChat", { message: "hello" });
    const duplicate = await store.appendChat(userId, spaceId, eventId, "groupChat", { message: "hello" });
    expect(first.status).toBe("accepted");
    expect(duplicate).toEqual({ status: "duplicate", streamId: first.streamId });
    const replay = await store.replayChat(spaceId, "0-0");
    expect(replay.truncated).toBe(false);
    expect(replay.limited).toBe(false);
    expect(replay.events).toHaveLength(1);
    expect(replay.events[0]).toMatchObject({ eventId, streamId: first.streamId, payload: { message: "hello", userId } });
  });
});
