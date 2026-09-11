import { spawn, type ChildProcess } from "node:child_process";
import { randomUUID } from "node:crypto";
import path from "node:path";
import Redis from "ioredis";
import jwt from "jsonwebtoken";
import { WebSocket } from "ws";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

const enabled = process.env.RUN_MULTI_REPLICA_TESTS === "true" && Boolean(process.env.REDIS_URL);
const describeReplicas = enabled ? describe : describe.skip;

describeReplicas("two WebSocket replicas", () => {
  const secret = "multi-replica-test-secret";
  const unique = randomUUID();
  const spaceId = `loadtest-${unique}`;
  const redis = new Redis(process.env.REDIS_URL!);
  const processes: ChildProcess[] = [];
  const clients: TestClient[] = [];

  beforeAll(async () => {
    processes.push(startReplica(8101, "test-replica-a"), startReplica(8102, "test-replica-b"));
    await Promise.all([waitUntilReady(8101), waitUntilReady(8102)]);
  }, 20_000);

  afterAll(async () => {
    clients.forEach((client) => client.close());
    processes.forEach((child) => child.kill("SIGTERM"));
    await Promise.all(processes.map(waitForExit));
    let cursor = "0";
    do {
      const [next, keys] = await redis.scan(cursor, "MATCH", `skyscope:v1:*${unique}*`, "COUNT", 100);
      cursor = next;
      if (keys.length) await redis.del(...keys);
    } while (cursor !== "0");
    await redis.srem("skyscope:v1:presence:spaces", spaceId);
    await redis.quit();
  }, 20_000);

  it("routes across replicas and suppresses a retried chat event", async () => {
    const senderA = await connect(8101, "sender-user");
    const senderB = await connect(8102, "sender-user");
    const observerB = await connect(8102, "observer-user");
    clients.push(senderA, senderB, observerB);

    const moveId = randomUUID();
    senderA.send({ type: "move", eventId: moveId, payload: { x: 51, y: 50 } });
    await expect(observerB.next((message) => message.type === "movement" && message.eventId === moveId)).resolves.toMatchObject({
      payload: { userId: "sender-user", x: 51, y: 50 },
    });

    const chatId = randomUUID();
    const chat = { type: "groupChat", eventId: chatId, payload: { message: "one durable message" } };
    senderA.send(chat);
    await expect(senderA.next((message) => message.type === "event-ack" && message.eventId === chatId)).resolves.toMatchObject({ payload: { status: "accepted" } });
    await expect(observerB.next((message) => message.type === "groupChat" && message.eventId === chatId)).resolves.toMatchObject({ payload: { message: "one durable message" } });

    senderB.send(chat);
    await expect(senderB.next((message) => message.type === "event-ack" && message.eventId === chatId)).resolves.toMatchObject({ payload: { status: "duplicate" } });
    await expect(observerB.expectNoMessage((message) => message.type === "groupChat" && message.eventId === chatId, 250)).resolves.toBe(true);

    const matchingEntries = await redis.xrange(`skyscope:v1:chat:${spaceId}`, "-", "+");
    expect(matchingEntries.filter(([, fields]) => field(fields, "eventId") === chatId)).toHaveLength(1);
  });

  it("replays chat missed while reconnecting to a different replica", async () => {
    const sessionId = randomUUID();
    const sender = await connect(8101, "recovery-sender");
    const observer = await connect(8102, "recovery-observer", { sessionId });
    clients.push(sender, observer);

    const baselineId = randomUUID();
    sender.send({ type: "groupChat", eventId: baselineId, payload: { message: "before disconnect" } });
    const baseline = await observer.next((message) => message.type === "groupChat" && message.eventId === baselineId);
    expect(baseline.streamId).toMatch(/^\d+-\d+$/);
    await observer.closeAndWait();

    const missedId = randomUUID();
    sender.send({ type: "groupChat", eventId: missedId, payload: { message: "while disconnected" } });
    await sender.next((message) => message.type === "event-ack" && message.eventId === missedId);

    const recovered = await connect(8101, "recovery-observer", { sessionId, lastEventId: baseline.streamId });
    clients.push(recovered);
    await expect(recovered.next((message) => message.type === "groupChat" && message.eventId === missedId)).resolves.toMatchObject({
      streamId: expect.stringMatching(/^\d+-\d+$/),
      payload: { message: "while disconnected", replayed: true },
    });
  });

  function startReplica(port: number, instanceId: string): ChildProcess {
    const entry = path.resolve(process.cwd(), "dist/index.js");
    const child = spawn(process.execPath, [entry], {
      cwd: process.cwd(),
      env: {
        ...process.env,
        PORT: String(port),
        INSTANCE_ID: instanceId,
        JWT_PASSWORD: secret,
        DATABASE_URL: "postgresql://unused:unused@127.0.0.1:1/unused",
        ALLOW_TEST_SPACES: "true",
        HEARTBEAT_INTERVAL_MS: "1000",
        PRESENCE_LEASE_MS: "3000",
        LOG_LEVEL: "silent",
      },
      stdio: "ignore",
    });
    return child;
  }

  async function connect(port: number, userId: string, recovery: { sessionId?: string; lastEventId?: string } = {}): Promise<TestClient> {
    const client = new TestClient(`ws://127.0.0.1:${port}/ws`);
    await client.open();
    client.send({ type: "join", payload: { token: jwt.sign({ userId }, secret), spaceId, ...recovery } });
    await client.next((message) => message.type === "space-joined");
    return client;
  }
});

type Message = { type?: string; eventId?: string; streamId?: string; payload?: Record<string, unknown> };

class TestClient {
  private readonly socket: WebSocket;
  private readonly messages: Message[] = [];
  private readonly listeners = new Set<() => void>();

  constructor(url: string) {
    this.socket = new WebSocket(url);
    this.socket.on("message", (raw) => {
      this.messages.push(JSON.parse(raw.toString()) as Message);
      this.listeners.forEach((listener) => listener());
    });
  }

  open(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.socket.once("open", resolve);
      this.socket.once("error", reject);
    });
  }

  send(message: object): void { this.socket.send(JSON.stringify(message)); }

  next(predicate: (message: Message) => boolean, timeoutMs = 3_000): Promise<Message> {
    return new Promise((resolve, reject) => {
      const inspect = () => {
        const index = this.messages.findIndex(predicate);
        if (index < 0) return;
        cleanup();
        resolve(this.messages.splice(index, 1)[0]);
      };
      const timeout = setTimeout(() => {
        cleanup();
        reject(new Error(`Timed out waiting for message; queued=${JSON.stringify(this.messages)}`));
      }, timeoutMs);
      const cleanup = () => { clearTimeout(timeout); this.listeners.delete(inspect); };
      this.listeners.add(inspect);
      inspect();
    });
  }

  async expectNoMessage(predicate: (message: Message) => boolean, durationMs: number): Promise<boolean> {
    try {
      await this.next(predicate, durationMs);
      return false;
    } catch {
      return true;
    }
  }

  close(): void { this.socket.close(1000); }

  closeAndWait(): Promise<void> {
    if (this.socket.readyState === WebSocket.CLOSED) return Promise.resolve();
    return new Promise((resolve) => {
      this.socket.once("close", () => resolve());
      this.socket.close(1000);
    });
  }
}

async function waitUntilReady(port: number): Promise<void> {
  const deadline = Date.now() + 10_000;
  while (Date.now() < deadline) {
    try {
      if ((await fetch(`http://127.0.0.1:${port}/readyz`)).status === 200) return;
    } catch { /* replica is still starting */ }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error(`Replica on port ${port} did not become ready`);
}

function waitForExit(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null) return Promise.resolve();
  return new Promise((resolve) => {
    const timeout = setTimeout(() => {
      child.kill("SIGKILL");
      resolve();
    }, 5_000);
    child.once("exit", () => { clearTimeout(timeout); resolve(); });
  });
}

function field(fields: string[], name: string): string | undefined {
  const index = fields.indexOf(name);
  return index >= 0 ? fields[index + 1] : undefined;
}
