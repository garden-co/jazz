import { beforeEach, describe, expect, it, vi } from "vitest";
import { flushSync } from "svelte";
import { contextStore } from "./test-helpers.svelte.js";

const { initJazzContext, getJazzContext, getDb, getSession } = await import("./context.svelte.js");

describe("context with real $state", () => {
  beforeEach(() => {
    contextStore.clear();
  });

  it("initJazzContext creates a reactive object", () => {
    const ctx = initJazzContext();

    expect(ctx.db).toBe(null);
    expect(ctx.session).toBe(null);
    expect(ctx.manager).toBe(null);
  });

  it("getJazzContext round-trips through set/get", () => {
    const ctx = initJazzContext();
    expect(getJazzContext()).toBe(ctx);
  });

  it("getJazzContext throws when no context is set", () => {
    expect(() => getJazzContext()).toThrow("getDb/getSession must be used within");
  });

  it("mutations to $state proxy are visible through getters", () => {
    const ctx = initJazzContext();

    const mockDb = { shutdown: vi.fn() } as any;
    ctx.db = mockDb;

    // $state wraps objects in a proxy, so reference identity with the
    // original won't hold — but the proxy faithfully reflects the value.
    expect(getDb()).toStrictEqual(mockDb);
  });

  it("getDb throws when db is null", () => {
    initJazzContext();
    expect(() => getDb()).toThrow("Jazz database is not yet initialised");
  });

  it("getSession returns null initially, then reflects updates", () => {
    const ctx = initJazzContext();
    expect(getSession()).toBe(null);

    const session = { user_id: "alice", claims: { role: "admin" } };
    ctx.session = session;

    expect(getSession()).toStrictEqual(session);
  });

  it("reading back a set value returns the proxy-wrapped equivalent", () => {
    const ctx = initJazzContext();

    const mockDb = { shutdown: vi.fn() } as any;
    const mockSession = { user_id: "bob", claims: {} };
    ctx.db = mockDb;
    ctx.session = mockSession;

    // Values survive the $state proxy round-trip
    expect(ctx.db).toStrictEqual(mockDb);
    expect(ctx.session).toStrictEqual(mockSession);
  });
});
