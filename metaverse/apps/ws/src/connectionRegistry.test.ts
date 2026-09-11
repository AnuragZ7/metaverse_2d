import { WebSocket } from "ws";
import { describe, expect, it } from "vitest";
import { ConnectionRegistry, type LocalConnection } from "./connectionRegistry";
import type { DistributedEvent } from "./types";

describe("ConnectionRegistry reconnect synchronization", () => {
  it("buffers live events and removes durable events already sent by replay", () => {
    const sent: string[] = [];
    const socket = {
      readyState: WebSocket.OPEN,
      bufferedAmount: 0,
      send: (value: string) => sent.push(value),
      close: () => undefined,
    } as unknown as WebSocket;
    const connection: LocalConnection = {
      connectionId: "connection-1",
      sessionId: "session-1",
      userId: "user-1",
      spaceId: "space-1",
      x: 0,
      y: 0,
      isAlive: true,
      closed: false,
      syncing: false,
      syncBuffer: [],
      ws: socket,
    };
    const registry = new ConnectionRegistry();
    registry.addOpen(connection);
    registry.beginSync(connection);
    registry.join(connection);

    registry.deliver(event("chat-1", "groupChat"));
    registry.deliver(event("move-1", "movement"));
    expect(sent).toEqual([]);

    registry.finishSync(connection, new Set(["chat-1"]));
    expect(sent.map((value) => JSON.parse(value))).toEqual([
      { type: "movement", eventId: "move-1", payload: {} },
    ]);
  });
});

function event(eventId: string, type: string): DistributedEvent {
  return { origin: "replica-a", roomId: "space-1", event: { type, eventId, payload: {} } };
}
