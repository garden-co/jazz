import { describe, it, expect } from "vitest";
import { schema as s } from "../index.js";
import { AnonymousWriteDeniedError } from "./anonymous-write-denied-error.js";

const todoApp = s.defineApp({
  todos: s.table({
    title: s.string(),
  }),
});

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

  it("rejects writes with AnonymousWriteDeniedError", async () => {
    const { createDb } = await import("./db.js");
    const db = await createDb({
      appId: "test-app",
      driver: { type: "memory" },
      serverUrl: "ws://example.invalid",
    });

    try {
      await db.insertDurable(todoApp.todos, { title: "write me" }, { tier: "local" });
      throw new Error("expected insertDurable to throw");
    } catch (error) {
      expect(error).toBeInstanceOf(AnonymousWriteDeniedError);
      const typed = error as AnonymousWriteDeniedError;
      expect(typed.operation).toBe("insert");
      expect(typed.table).toBe("todos");
    } finally {
      await db.shutdown();
    }
  });
});
