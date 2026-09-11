type ServerMessage = {
  type?: string;
  eventId?: string;
  streamId?: string;
  payload?: Record<string, unknown>;
};

type ClientEvent = {
  type: string;
  eventId?: string;
  payload: Record<string, unknown>;
};

type PendingEvent = {
  event: ClientEvent;
  queuedAt: number;
};

type Options = {
  url: string;
  spaceId: string;
  token: string;
  position?: () => { x?: number; y?: number } | null;
  onMessage: (message: ServerMessage) => void;
  onConnectionChange: (connected: boolean) => void;
  onError?: (error: Event) => void;
};

const MAX_RECONNECT_MS = 30_000;
const configuredPendingTtl = Number(import.meta.env.VITE_WS_PENDING_TTL_MS);
const PENDING_EVENT_MAX_AGE_MS = Number.isFinite(configuredPendingTtl) && configuredPendingTtl > 0
  ? configuredPendingTtl
  : 5 * 60_000;

export class ResilientWebSocket {
  private socket: WebSocket | null = null;
  private stopped = false;
  private joined = false;
  private reconnectAttempt = 0;
  private reconnectTimer: number | undefined;
  private readonly pending = new Map<string, PendingEvent>();
  private readonly sessionKey: string;
  private readonly cursorKey: string;
  private readonly pendingKey: string;

  constructor(private readonly options: Options) {
    this.sessionKey = `office_session_${options.spaceId}`;
    this.cursorKey = `office_cursor_${options.spaceId}`;
    this.pendingKey = `office_pending_${options.spaceId}`;
    this.restorePending();
    this.connect();
  }

  send(raw: string): void {
    let message: ClientEvent;
    try {
      message = JSON.parse(raw) as ClientEvent;
    } catch {
      throw new Error("WebSocket events must be valid JSON");
    }
    if (!message.type || !message.payload) throw new Error("WebSocket event requires type and payload");
    if (message.type !== "join") message.eventId ??= createEventId();
    if (message.eventId && (message.type === "groupChat" || message.type === "privateChat")) {
      this.pending.set(message.eventId, { event: message, queuedAt: Date.now() });
      this.persistPending();
    }
    if (this.joined && this.socket?.readyState === WebSocket.OPEN) this.socket.send(JSON.stringify(message));
  }

  close(): void {
    this.stopped = true;
    this.joined = false;
    if (this.reconnectTimer !== undefined) window.clearTimeout(this.reconnectTimer);
    this.socket?.close(1000, "Page closed");
    this.socket = null;
    this.options.onConnectionChange(false);
  }

  private connect(): void {
    if (this.stopped) return;
    const socket = new WebSocket(this.options.url);
    this.socket = socket;
    this.joined = false;

    socket.onopen = () => {
      if (socket !== this.socket || this.stopped) return;
      const position = this.options.position?.();
      socket.send(JSON.stringify({
        type: "join",
        payload: {
          spaceId: this.options.spaceId,
          token: this.options.token,
          sessionId: localStorage.getItem(this.sessionKey) || undefined,
          lastEventId: localStorage.getItem(this.cursorKey) || undefined,
          x: position?.x,
          y: position?.y,
        },
      }));
    };

    socket.onmessage = (event) => {
      if (socket !== this.socket) return;
      try {
        const message = JSON.parse(event.data) as ServerMessage;
        if (message.streamId && (message.type === "groupChat" || message.type === "privateChat")) {
          this.saveCursor(message.streamId);
        }
        if (message.type === "event-ack" && message.eventId) {
          this.pending.delete(message.eventId);
          this.persistPending();
        }
        if (message.type === "space-joined") {
          const sessionId = message.payload?.sessionId;
          if (typeof sessionId === "string") localStorage.setItem(this.sessionKey, sessionId);
          this.joined = true;
          this.reconnectAttempt = 0;
          this.options.onConnectionChange(true);
          this.expirePending();
          for (const pending of this.pending.values()) socket.send(JSON.stringify(pending.event));
        }
        this.options.onMessage(message);
      } catch (error) {
        console.error("Invalid WebSocket response", error);
      }
    };

    socket.onerror = (error) => this.options.onError?.(error);
    socket.onclose = (event) => {
      if (socket !== this.socket) return;
      this.joined = false;
      this.options.onConnectionChange(false);
      if (this.stopped || event.code === 1000) return;
      const ceiling = Math.min(MAX_RECONNECT_MS, 500 * (2 ** this.reconnectAttempt));
      const delay = Math.floor(Math.random() * ceiling);
      this.reconnectAttempt += 1;
      this.reconnectTimer = window.setTimeout(() => this.connect(), delay);
    };
  }

  private restorePending(): void {
    try {
      const values = JSON.parse(sessionStorage.getItem(this.pendingKey) ?? "[]") as Array<PendingEvent | ClientEvent>;
      for (const value of values) {
        const pending = "event" in value ? value : { event: value, queuedAt: 0 };
        if (pending.event.eventId) this.pending.set(pending.event.eventId, pending);
      }
      this.expirePending();
    } catch {
      sessionStorage.removeItem(this.pendingKey);
    }
  }

  private persistPending(): void {
    sessionStorage.setItem(this.pendingKey, JSON.stringify([...this.pending.values()]));
  }

  private expirePending(): void {
    const oldestAllowed = Date.now() - PENDING_EVENT_MAX_AGE_MS;
    let changed = false;
    for (const [eventId, pending] of this.pending) {
      if (pending.queuedAt >= oldestAllowed) continue;
      this.pending.delete(eventId);
      changed = true;
    }
    if (changed) this.persistPending();
  }

  private saveCursor(next: string): void {
    const current = localStorage.getItem(this.cursorKey);
    if (!current || compareStreamIds(next, current) > 0) localStorage.setItem(this.cursorKey, next);
  }
}

export function createEventId(): string {
  return typeof crypto.randomUUID === "function"
    ? crypto.randomUUID()
    : `${Date.now()}-${crypto.getRandomValues(new Uint32Array(2)).join("-")}`;
}

function compareStreamIds(left: string, right: string): number {
  const [leftMs, leftSequence] = left.split("-").map(BigInt);
  const [rightMs, rightSequence] = right.split("-").map(BigInt);
  if (leftMs !== rightMs) return leftMs > rightMs ? 1 : -1;
  if (leftSequence === rightSequence) return 0;
  return leftSequence > rightSequence ? 1 : -1;
}
