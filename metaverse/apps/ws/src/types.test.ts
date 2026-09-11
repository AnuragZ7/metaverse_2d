import { describe, expect, it } from "vitest";
import { incomingMessageSchema } from "./types";

describe("WebSocket protocol", () => {
  it("accepts a versioned, idempotent chat event", () => {
    const result = incomingMessageSchema.safeParse({
      type: "groupChat",
      eventId: "018f47a6-test-event",
      payload: { message: "hello" },
    });
    expect(result.success).toBe(true);
  });

  it("rejects mutating events without an event id", () => {
    const result = incomingMessageSchema.safeParse({
      type: "move",
      payload: { x: 1, y: 2 },
    });
    expect(result.success).toBe(false);
  });

  it("rejects unknown payload fields after parsing", () => {
    const result = incomingMessageSchema.parse({
      type: "move",
      eventId: "018f47a6-test-event",
      payload: { x: 1, y: 2, userId: "spoofed" },
    });
    expect("userId" in result.payload).toBe(false);
  });
});
