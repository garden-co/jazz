import { describe, expect, it } from "vitest";
import { jwtPayload } from "./jwt-payload.js";

describe("jwtPayload", () => {
  it("returns a plain object", () => {
    const alice = { id: "alice-user-id-123" };
    const payload = jwtPayload({ user: alice });
    expect(payload).toBeTypeOf("object");
  });
});
