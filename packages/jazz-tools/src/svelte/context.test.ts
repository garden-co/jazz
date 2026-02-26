import { describe, it, expect, vi } from "vitest";

// Mock svelte context functions
const contextStore = new Map<unknown, unknown>();
vi.mock("svelte", () => ({
  setContext: (key: unknown, value: unknown) => contextStore.set(key, value),
  getContext: (key: unknown) => contextStore.get(key),
}));

describe("context", () => {
  it("initJazzContext sets a context and returns reactive object", async () => {
    contextStore.clear();
    const { initJazzContext, getJazzContext } = await import("./context.svelte.js");

    const ctx = initJazzContext();

    expect(ctx.db).toBe(null);
    expect(ctx.session).toBe(null);

    const retrieved = getJazzContext();
    expect(retrieved).toBe(ctx);
  });

  it("getJazzContext throws when no context is set", async () => {
    contextStore.clear();
    const { getJazzContext } = await import("./context.svelte.js");

    expect(() => getJazzContext()).toThrow("getDb/getSession must be used within");
  });

  it("getDb throws when db is null", async () => {
    contextStore.clear();
    const { initJazzContext, getDb } = await import("./context.svelte.js");

    initJazzContext();
    expect(() => getDb()).toThrow("Jazz database is not yet initialised");
  });

  it("getDb returns db when set", async () => {
    contextStore.clear();
    const { initJazzContext, getDb } = await import("./context.svelte.js");

    const ctx = initJazzContext();
    const mockDb = { shutdown: vi.fn() } as any;
    ctx.db = mockDb;

    expect(getDb()).toBe(mockDb);
  });

  it("getSession returns null initially", async () => {
    contextStore.clear();
    const { initJazzContext, getSession } = await import("./context.svelte.js");

    initJazzContext();
    expect(getSession()).toBe(null);
  });

  it("getSession returns session when set", async () => {
    contextStore.clear();
    const { initJazzContext, getSession } = await import("./context.svelte.js");

    const ctx = initJazzContext();
    const session = { user_id: "alice", claims: { role: "admin" } };
    ctx.session = session;

    expect(getSession()).toBe(session);
  });
});
