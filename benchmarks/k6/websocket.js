import { check } from 'k6';
import crypto from 'k6/crypto';
import encoding from 'k6/encoding';
import ws from 'k6/ws';
import { Counter, Rate, Trend } from 'k6/metrics';

const chatRoundTrip = new Trend('chat_round_trip_ms', true);
const joinedConnections = new Counter('joined_connections');
const logicalFailures = new Rate('logical_failure_rate');
const connectionFailures = new Rate('connection_failure_rate');
const duplicateDeliveries = new Counter('duplicate_deliveries');

const duration = __ENV.DURATION || '10m';
const durationMs = parseDuration(duration);

export const options = {
  scenarios: {
    websocket_connections: {
      executor: 'constant-vus',
      vus: Number(__ENV.CONNECTIONS || 100),
      duration,
    },
  },
  thresholds: {
    chat_round_trip_ms: ['p(95)<200'],
    logical_failure_rate: ['rate<0.001'],
    connection_failure_rate: ['rate<0.001'],
  },
};

export default function () {
  const url = __ENV.WS_URL || 'ws://localhost:8000/ws';
  const secret = __ENV.JWT_PASSWORD || 'local-shared-jwt-secret';
  const userId = `load-user-${__VU}`;
  const token = signJwt({ userId, exp: Math.floor(Date.now() / 1000) + 3600 }, secret);
  const pending = new Map();
  const received = new Set();
  let joined = false;
  let sequence = 0;

  const response = ws.connect(url, {}, (socket) => {
    socket.on('open', () => {
      socket.send(JSON.stringify({ type: 'join', payload: { token, spaceId: __ENV.SPACE_ID || 'loadtest-main' } }));
    });

    socket.on('message', (raw) => {
      let message;
      try {
        message = JSON.parse(raw);
      } catch {
        logicalFailures.add(true);
        return;
      }
      if (message.type === 'space-joined') {
        joined = true;
        joinedConnections.add(1);
        return;
      }
      if (message.type === 'groupChat' && message.eventId) {
        if (received.has(message.eventId)) duplicateDeliveries.add(1);
        received.add(message.eventId);
        const started = pending.get(message.eventId);
        if (started !== undefined) {
          chatRoundTrip.add(Date.now() - started);
          logicalFailures.add(false);
          pending.delete(message.eventId);
        }
      }
    });

    socket.setInterval(() => {
      if (!joined) return;
      sequence += 1;
      const eventId = `${__VU}-${__ITER}-${sequence}-${Date.now()}`;
      pending.set(eventId, Date.now());
      socket.send(JSON.stringify({ type: 'groupChat', eventId, payload: { message: `load message ${sequence}` } }));
    }, Number(__ENV.CHAT_INTERVAL_MS || 10000));

    socket.setInterval(() => {
      if (!joined) return;
      sequence += 1;
      socket.send(JSON.stringify({
        type: 'move',
        eventId: `${__VU}-${__ITER}-move-${sequence}-${Date.now()}`,
        payload: { x: 50 + (sequence % 2), y: 50 },
      }));
    }, Number(__ENV.MOVE_INTERVAL_MS || 500));

    socket.setTimeout(() => {
      for (const started of pending.values()) {
        if (Date.now() - started >= 5000) logicalFailures.add(true);
      }
      if (!joined) logicalFailures.add(true);
      socket.close();
    }, durationMs - 250);
  });

  connectionFailures.add(response?.status !== 101 || !joined);
  check(response, { 'WebSocket upgraded': (result) => result?.status === 101 });
}

function signJwt(payload, secret) {
  const header = encoding.b64encode(JSON.stringify({ alg: 'HS256', typ: 'JWT' }), 'rawurl');
  const body = encoding.b64encode(JSON.stringify(payload), 'rawurl');
  const unsigned = `${header}.${body}`;
  return `${unsigned}.${crypto.hmac('sha256', secret, unsigned, 'base64rawurl')}`;
}

function parseDuration(value) {
  const match = /^(\d+)(ms|s|m|h)$/.exec(value);
  if (!match) throw new Error(`Unsupported DURATION: ${value}`);
  const factor = { ms: 1, s: 1000, m: 60000, h: 3600000 }[match[2]];
  return Number(match[1]) * factor;
}
