import { describe, expect, it } from "vitest";
import { generateClientId, isValidClientId, resolveClientId } from "./client-id.js";

describe("client-id helpers", () => {
  it("generates a valid UUID", () => {
    const clientId = generateClientId();
    expect(isValidClientId(clientId)).toBe(true);
  });

  it("accepts a provided valid UUID", () => {
    const provided = "550e8400-e29b-41d4-a716-446655440000";
    expect(resolveClientId(provided)).toBe(provided);
  });

  it("rejects invalid UUIDs", () => {
    expect(() => resolveClientId("not-a-uuid")).toThrow("Invalid clientId");
  });
});
