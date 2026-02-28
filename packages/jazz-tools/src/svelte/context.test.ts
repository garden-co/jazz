import { beforeAll, describe, expect, it, vi } from "vitest";

// Mock svelte context functions
const contextStore = new Map<unknown, unknown>();
vi.mock("svelte", () => ({
  setContext: (key: unknown, value: unknown) => contextStore.set(key, value),
  getContext: (key: unknown) => contextStore.get(key),
}));

let contextModule: typeof import("./context.svelte.js");

beforeAll(async () => {
  contextModule = await import("./context.svelte.js");
});

describe("context", () => {
  it("initJazzContext sets a context and returns reactive object", () => {
    contextStore.clear();
    const { initJazzContext, getJazzContext } = contextModule;

    const ctx = initJazzContext();

    expect(ctx.db).toBe(null);
    expect(ctx.session).toBe(null);

    const retrieved = getJazzContext();
    expect(retrieved).toBe(ctx);
  });

  it("getJazzContext throws when no context is set", () => {
    contextStore.clear();
    const { getJazzContext } = contextModule;

    expect(() => getJazzContext()).toThrow("getDb/getSession must be used within");
  });

  it("getDb throws when db is null", () => {
    contextStore.clear();
    const { initJazzContext, getDb } = contextModule;

    initJazzContext();
    expect(() => getDb()).toThrow("Jazz database is not yet initialised");
  });

  it("getDb returns db when set", () => {
    contextStore.clear();
    const { initJazzContext, getDb } = contextModule;

    const ctx = initJazzContext();
    const mockDb = { shutdown: vi.fn() } as any;
    ctx.db = mockDb;

    expect(getDb()).toBe(mockDb);
  });

  it("getSession returns null initially", () => {
    contextStore.clear();
    const { initJazzContext, getSession } = contextModule;

    initJazzContext();
    expect(getSession()).toBe(null);
  });

  it("getSession returns session when set", () => {
    contextStore.clear();
    const { initJazzContext, getSession } = contextModule;

    const ctx = initJazzContext();
    const session = { user_id: "alice", claims: { role: "admin" } };
    ctx.session = session;

    expect(getSession()).toBe(session);
  });
});
