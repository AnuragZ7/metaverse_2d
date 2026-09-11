import { WebSocket } from "ws";
import { config } from "./config";
import { activeConnections, outboundDroppedTotal } from "./metrics";
import type { DistributedEvent, OutgoingMessage } from "./types";

export type LocalConnection = {
  connectionId: string;
  sessionId?: string;
  userId?: string;
  spaceId?: string;
  width?: number;
  height?: number;
  x: number;
  y: number;
  isAlive: boolean;
  closed: boolean;
  syncing: boolean;
  syncBuffer: DistributedEvent[];
  ws: WebSocket;
};

export class ConnectionRegistry {
  private readonly all = new Map<string, LocalConnection>();
  private readonly rooms = new Map<string, Map<string, LocalConnection>>();

  addOpen(connection: LocalConnection): void {
    this.all.set(connection.connectionId, connection);
    this.updateMetrics();
  }

  join(connection: LocalConnection): void {
    if (!connection.spaceId) return;
    const room = this.rooms.get(connection.spaceId) ?? new Map<string, LocalConnection>();
    room.set(connection.connectionId, connection);
    this.rooms.set(connection.spaceId, room);
    this.updateMetrics();
  }

  beginSync(connection: LocalConnection): void {
    connection.syncing = true;
    connection.syncBuffer = [];
  }

  finishSync(connection: LocalConnection, replayedEventIds: ReadonlySet<string>): void {
    const buffered = connection.syncBuffer;
    connection.syncBuffer = [];
    connection.syncing = false;
    for (const distributed of buffered) {
      const eventId = distributed.event.eventId;
      if (eventId && replayedEventIds.has(eventId)) continue;
      this.send(connection, distributed.event);
    }
  }

  abortSync(connection: LocalConnection): void {
    connection.syncing = false;
    connection.syncBuffer = [];
  }

  remove(connection: LocalConnection): void {
    this.all.delete(connection.connectionId);
    this.abortSync(connection);
    if (connection.spaceId) {
      const room = this.rooms.get(connection.spaceId);
      room?.delete(connection.connectionId);
      if (room?.size === 0) this.rooms.delete(connection.spaceId);
    }
    this.updateMetrics();
  }

  replace(connectionId: string): void {
    const previous = this.all.get(connectionId);
    if (previous && previous.ws.readyState === WebSocket.OPEN) previous.ws.close(1012, "Session resumed elsewhere");
  }

  deliver(distributed: DistributedEvent): void {
    const room = this.rooms.get(distributed.roomId);
    if (!room) return;
    for (const connection of room.values()) {
      if (distributed.excludeSessionId === connection.sessionId) continue;
      if (distributed.targetUserId && distributed.targetUserId !== connection.userId) continue;
      if (connection.syncing) {
        if (connection.syncBuffer.length >= config.syncBufferMaxEvents) {
          outboundDroppedTotal.inc({ reason: "sync_buffer_overflow" });
          connection.ws.close(1013, "Reconnect buffer overflow; retry shortly");
          continue;
        }
        connection.syncBuffer.push(distributed);
        continue;
      }
      this.send(connection, distributed.event);
    }
  }

  send(connection: LocalConnection, message: OutgoingMessage): boolean {
    if (connection.ws.readyState !== WebSocket.OPEN) {
      outboundDroppedTotal.inc({ reason: "closed" });
      return false;
    }
    if (connection.ws.bufferedAmount > config.maxBufferedBytes) {
      outboundDroppedTotal.inc({ reason: "slow_consumer" });
      connection.ws.close(1008, "Client is not consuming messages");
      return false;
    }
    connection.ws.send(JSON.stringify(message));
    return true;
  }

  values(): IterableIterator<LocalConnection> { return this.all.values(); }
  get size(): number { return this.all.size; }

  private updateMetrics(): void {
    activeConnections.set({ state: "open" }, this.all.size);
    activeConnections.set({ state: "joined" }, [...this.all.values()].filter((connection) => connection.sessionId).length);
  }
}
