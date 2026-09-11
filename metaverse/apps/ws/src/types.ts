import { z } from "zod";

const eventId = z.string().min(8).max(128);
const target = z.string().min(1).max(128);
const opaqueRtc = z.unknown();

export const incomingMessageSchema = z.discriminatedUnion("type", [
  z.object({
    type: z.literal("join"),
    payload: z.object({
      token: z.string().min(1),
      spaceId: z.string().min(1).max(128),
      sessionId: z.string().uuid().optional(),
      lastEventId: z.string().regex(/^\d+-\d+$/).optional(),
      x: z.number().int().nonnegative().optional(),
      y: z.number().int().nonnegative().optional(),
    }),
  }),
  z.object({ type: z.literal("move"), eventId, payload: z.object({ x: z.number().int(), y: z.number().int() }) }),
  z.object({ type: z.literal("groupChat"), eventId, payload: z.object({ message: z.string().trim().min(1).max(2_000) }) }),
  z.object({ type: z.literal("privateChat"), eventId, payload: z.object({ userId: target, message: z.string().trim().min(1).max(2_000) }) }),
  z.object({ type: z.literal("user:call"), eventId, payload: z.object({ to: target, offer: opaqueRtc }) }),
  z.object({ type: z.literal("call:accepted"), eventId, payload: z.object({ to: target, ans: opaqueRtc }) }),
  z.object({ type: z.literal("peer:nego:needed"), eventId, payload: z.object({ to: target, offer: opaqueRtc }) }),
  z.object({ type: z.literal("peer:nego:done"), eventId, payload: z.object({ to: target, ans: opaqueRtc }) }),
  z.object({ type: z.literal("ice:candidate"), eventId, payload: z.object({ to: target, candidate: opaqueRtc }) }),
  z.object({ type: z.literal("call:ended"), eventId, payload: z.object({ to: target.optional() }) }),
]);

export type IncomingMessage = z.infer<typeof incomingMessageSchema>;

export type OutgoingMessage = {
  type: string;
  eventId?: string;
  streamId?: string;
  payload: Record<string, unknown>;
};

export type DistributedEvent = {
  origin: string;
  roomId: string;
  excludeSessionId?: string;
  targetUserId?: string;
  event: OutgoingMessage;
  traceparent?: string;
};

export type PresenceSnapshot = { userId: string; x: number; y: number };
