import { describe, it, expect } from "vitest";

describe("createDb — anonymous mode", () => {
  it("mints an anonymous JWT when no credential is provided", async () => {
    const { createDb } = await import("./db.js");
    const db = await createDb({
      appId: "test-app",
      driver: { type: "memory" },
      serverUrl: "ws://example.invalid",
    });
    const session = db.getAuthState().session;
    expect(session?.authMode).toBe("anonymous");
    await db.shutdown();
  });

  it("keeps the anonymous identity stable across multiple getAuthState() calls", async () => {
    const { createDb } = await import("./db.js");
    const db = await createDb({
      appId: "test-app",
      driver: { type: "memory" },
      serverUrl: "ws://example.invalid",
    });
    const first = db.getAuthState().session?.user_id;
    const second = db.getAuthState().session?.user_id;
    expect(first).toBe(second);
    expect(first).toBeTruthy();
    await db.shutdown();
  });
});
