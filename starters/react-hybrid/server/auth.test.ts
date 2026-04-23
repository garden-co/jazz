import { describe, expect, it } from "vitest";
import { jwtPayload } from "./jwt-payload.js";

describe("jwtPayload", () => {
  it("does not include jazz_principal_id in the JWT payload", () => {
    const alice = { id: "alice-user-id-123" };
    const payload = jwtPayload({ user: alice });
    expect(payload).not.toHaveProperty("jazz_principal_id");
  });
});
