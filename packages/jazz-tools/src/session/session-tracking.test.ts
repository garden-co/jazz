import { describe, expect, it } from "vitest";
import { ownJazzSession } from "./jazz-session.js";

// This file never imports `jazz-tools/testing/sessions`.
describe("session tracking", () => {
  it("tracks nothing until the testing module turns it on", () => {
    ownJazzSession(
      {},
      async () => {
        throw new Error("not started");
      },
      { start: false },
    );
    expect(
      (globalThis as Record<symbol, unknown>)[Symbol.for("jazz-tools.testing.openSessions")],
    ).toBeUndefined();
  });
});
