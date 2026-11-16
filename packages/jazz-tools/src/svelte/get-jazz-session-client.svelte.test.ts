import { beforeEach, describe, expect, it, vi } from "vitest";
import { get } from "svelte/store";
import { contextStore } from "./test-helpers.svelte.js";

const { initJazzContext, getSession } = await import("./context.svelte.js");
const { getJazzSession, hideJazzSession, setJazzSession } = await import("./session-state.js");

const session = {
  user: "00000000-0000-4000-8000-000000000002",
  identity: { issuer: "https://issuer.example", subject: "alice" },
  claims: { role: "admin" },
  authMode: "external" as const,
};

describe("getJazzSession under <JazzSvelteClientProvider client>", () => {
  beforeEach(() => contextStore.clear());

  it("is starting until the provider has a client, then ready with its user", () => {
    const ctx = initJazzContext();
    const jazzSession = getJazzSession();
    expect(get(jazzSession).status).toBe("starting");
    const client = { db: {}, session };
    ctx.client = client;
    ctx.session = session;
    const snapshot = get(jazzSession);
    expect(snapshot.status).toBe("ready");
    expect(snapshot.user).toEqual({
      accountId: session.user,
      kind: "backend",
      identity: session.identity,
      claims: { role: "admin" },
    });
    ctx.session = null;
    expect(get(jazzSession).user).toBeNull();
  });

  it("refuses session operations, naming why", async () => {
    initJazzContext();
    await expect(getJazzSession().signOut()).rejects.toThrow(/outside a Jazz session/);
  });

  it("a nested client provider hides the enclosing <JazzProvider>'s session", async () => {
    const outerSignOut = vi.fn(async () => {});
    setJazzSession({
      getSnapshot: () => ({ status: "ready", user: null, localFirst: false }),
      subscribe: () => () => {},
      signOut: outerSignOut,
    } as never);
    expect(getJazzSession().signOut).toBe(outerSignOut);
    // What <JazzSvelteClientProvider client> does unless <JazzProvider> renders it.
    hideJazzSession();
    const ctx = initJazzContext();
    ctx.client = { db: {}, session };
    ctx.session = session;
    const nested = getJazzSession();
    expect(get(nested).user?.accountId).toBe(session.user);
    await expect(nested.signOut()).rejects.toThrow(/outside a Jazz session/);
    expect(outerSignOut).not.toHaveBeenCalled();
  });

  it("still throws outside any provider", () => {
    expect(() => getJazzSession()).toThrow(/<JazzProvider> or <JazzSvelteClientProvider>/);
  });

  it("getSession() still works and warns once, naming getJazzSession", () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const ctx = initJazzContext();
    ctx.session = session;
    expect(getSession().current?.user).toBe(session.user);
    getSession();
    const calls = warn.mock.calls.filter((call) => String(call[0]).includes("getSession()"));
    expect(calls).toHaveLength(1);
    expect(String(calls[0]![0])).toContain("getJazzSession()");
    warn.mockRestore();
  });
});
