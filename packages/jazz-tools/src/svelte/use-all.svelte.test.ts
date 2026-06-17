import { beforeEach, describe, expect, it, vi } from "vitest";
import { flushSync } from "svelte";
import "./test-helpers.svelte.js";

const mocks = vi.hoisted(() => {
  const unsubscribe = vi.fn();
  const subscribe = vi.fn(() => unsubscribe);
  const makeQueryKey = vi.fn((q: any) => `key:${q?._marker ?? "?"}`);
  const getCacheEntry = vi.fn(() => ({
    state: { status: "fulfilled", data: [] },
    subscribe,
  }));
  const applySnapshot = vi.fn();

  return {
    makeQueryKey,
    getCacheEntry,
    subscribe,
    unsubscribe,
    applySnapshot,
    reset() {
      unsubscribe.mockReset();
      subscribe.mockReset().mockReturnValue(unsubscribe);
      makeQueryKey.mockReset().mockImplementation((q: any) => `key:${q?._marker ?? "?"}`);
      getCacheEntry.mockReset().mockReturnValue({
        state: { status: "fulfilled", data: [] },
        subscribe,
      });
      applySnapshot.mockReset();
    },
  };
});

vi.mock("./context.svelte.js", () => ({
  getJazzContext: () => ({
    db: null,
    session: null,
    manager: {
      makeQueryKey: mocks.makeQueryKey,
      getCacheEntry: mocks.getCacheEntry,
    },
  }),
}));

vi.mock("../ssr/apply-snapshot.js", () => ({ applySnapshot: mocks.applySnapshot }));
vi.mock("../drivers/schema-wire.js", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../drivers/schema-wire.js")>()),
  computeSchemaFingerprint: () => "fp",
}));

const { QuerySubscription } = await import("./use-all.svelte.js");

function makeQuery(marker = "todos") {
  return { _marker: marker } as any;
}

async function settle() {
  await Promise.resolve();
  flushSync();
}

describe("svelte/QuerySubscription", () => {
  beforeEach(() => {
    mocks.reset();
  });

  it("subscribes when given a plain QueryBuilder", async () => {
    const query = makeQuery("inbox");
    const cleanup = $effect.root(() => {
      new QuerySubscription(query);
    });
    await settle();

    expect(mocks.makeQueryKey).toHaveBeenCalledWith(query, undefined);
    expect(mocks.subscribe).toHaveBeenCalledTimes(1);

    cleanup();
  });

  it("does not subscribe when given undefined", async () => {
    const cleanup = $effect.root(() => {
      new QuerySubscription(undefined);
    });
    await settle();

    expect(mocks.makeQueryKey).not.toHaveBeenCalled();
    expect(mocks.subscribe).not.toHaveBeenCalled();

    cleanup();
  });

  it("accepts a getter and subscribes with the resolved query", async () => {
    const query = makeQuery("inbox");
    const cleanup = $effect.root(() => {
      new QuerySubscription(() => query);
    });
    await settle();

    expect(mocks.makeQueryKey).toHaveBeenCalledWith(query, undefined);
    expect(mocks.subscribe).toHaveBeenCalledTimes(1);

    cleanup();
  });

  it("getter returning undefined does not subscribe", async () => {
    const cleanup = $effect.root(() => {
      new QuerySubscription(() => undefined);
    });
    await settle();

    expect(mocks.makeQueryKey).not.toHaveBeenCalled();
    expect(mocks.subscribe).not.toHaveBeenCalled();

    cleanup();
  });

  it("getter reading $state re-subscribes when state changes", async () => {
    let filter = $state<string | null>(null);
    const inboxQuery = makeQuery("inbox");
    const filteredQuery = makeQuery("filtered");

    const cleanup = $effect.root(() => {
      new QuerySubscription(() => (filter ? filteredQuery : inboxQuery));
    });
    await settle();

    expect(mocks.makeQueryKey).toHaveBeenLastCalledWith(inboxQuery, undefined);
    expect(mocks.subscribe).toHaveBeenCalledTimes(1);

    filter = "alice";
    await settle();

    expect(mocks.unsubscribe).toHaveBeenCalledTimes(1);
    expect(mocks.makeQueryKey).toHaveBeenLastCalledWith(filteredQuery, undefined);
    expect(mocks.subscribe).toHaveBeenCalledTimes(2);

    cleanup();
  });

  it("getter flipping undefined → query subscribes only after the flip", async () => {
    let filter = $state<string | null>(null);
    const filteredQuery = makeQuery("filtered");

    const cleanup = $effect.root(() => {
      new QuerySubscription(() => (filter ? filteredQuery : undefined));
    });
    await settle();

    expect(mocks.makeQueryKey).not.toHaveBeenCalled();
    expect(mocks.subscribe).not.toHaveBeenCalled();

    filter = "alice";
    await settle();

    expect(mocks.makeQueryKey).toHaveBeenCalledWith(filteredQuery, undefined);
    expect(mocks.subscribe).toHaveBeenCalledTimes(1);

    cleanup();
  });

  it("getter flipping query → undefined unsubscribes", async () => {
    let active = $state(true);
    const query = makeQuery("inbox");

    const cleanup = $effect.root(() => {
      new QuerySubscription(() => (active ? query : undefined));
    });
    await settle();

    expect(mocks.subscribe).toHaveBeenCalledTimes(1);

    active = false;
    await settle();

    expect(mocks.unsubscribe).toHaveBeenCalledTimes(1);

    cleanup();
  });

  it("clears a stale error when the getter flips back to undefined", async () => {
    let active = $state(true);
    const query = makeQuery("inbox");

    let capturedOnError: ((err: unknown) => void) | undefined;
    mocks.getCacheEntry.mockReturnValue({
      state: { status: "fulfilled" as const, data: [] },
      subscribe: (callbacks: any) => {
        capturedOnError = callbacks.onError;
        return mocks.unsubscribe;
      },
    } as any);

    const sub = (() => {
      let ref!: InstanceType<typeof QuerySubscription<{ id: string }>>;
      const cleanup = $effect.root(() => {
        ref = new QuerySubscription(() => (active ? query : undefined));
      });
      return { ref, cleanup };
    })();
    await settle();

    capturedOnError!(new Error("boom"));
    await settle();
    expect(sub.ref.error).toBeInstanceOf(Error);

    active = false;
    await settle();

    expect(sub.ref.error).toBeNull();

    sub.cleanup();
  });

  it("options accepts a getter and forwards the resolved value", async () => {
    const query = makeQuery("inbox");

    const cleanup = $effect.root(() => {
      new QuerySubscription(query, () => ({ tier: "edge" as const }));
    });
    await settle();

    expect(mocks.makeQueryKey).toHaveBeenCalledWith(query, { tier: "edge" });

    cleanup();
  });

  it("starts with current undefined regardless of options.tier", () => {
    // An unseeded query (no snapshot) has no fulfilled entry yet, so the
    // constructor's synchronous first read leaves current untouched.
    mocks.getCacheEntry.mockReturnValue({
      state: { status: "pending" as const, data: undefined },
      subscribe: mocks.subscribe,
    } as any);

    let withoutTier!: InstanceType<typeof QuerySubscription<{ id: string }>>;
    let withTier!: InstanceType<typeof QuerySubscription<{ id: string }>>;
    const cleanup = $effect.root(() => {
      withoutTier = new QuerySubscription(makeQuery());
      withTier = new QuerySubscription(makeQuery(), { tier: "edge" as const });
    });

    expect(withoutTier.current).toBeUndefined();
    expect(withTier.current).toBeUndefined();

    cleanup();
  });

  it("reads a fulfilled entry synchronously, before the effect subscribes", async () => {
    const query = makeQuery("inbox");
    const seeded = [{ id: "1", title: "seeded" }];
    mocks.getCacheEntry.mockReturnValue({
      state: { status: "fulfilled" as const, data: seeded },
      subscribe: mocks.subscribe,
    } as any);

    let ref!: InstanceType<typeof QuerySubscription<{ id: string }>>;
    const cleanup = $effect.root(() => {
      ref = new QuerySubscription(query);
    });

    // Synchronous: the SSR render (no $effect) already has the rows, unsubscribed.
    expect(ref.current).toEqual(seeded);
    expect(ref.loading).toBe(false);
    expect(mocks.subscribe).not.toHaveBeenCalled();

    await settle();
    expect(mocks.subscribe).toHaveBeenCalledTimes(1);

    cleanup();
  });

  it("applies a snapshot from a getter once, and keeps the options reactive", async () => {
    const query = makeQuery("inbox");
    const snapshot = {} as any;
    let tier = $state<"edge" | "global">("edge");

    const cleanup = $effect.root(() => {
      new QuerySubscription(query, () => ({ tier, snapshot }));
    });
    await settle();

    expect(mocks.applySnapshot).toHaveBeenCalledTimes(1);
    expect(mocks.makeQueryKey).toHaveBeenLastCalledWith(query, { tier: "edge" });

    tier = "global";
    await settle();

    // The snapshot is one-shot (not re-applied); the tier reactively updates the key.
    expect(mocks.applySnapshot).toHaveBeenCalledTimes(1);
    expect(mocks.makeQueryKey).toHaveBeenLastCalledWith(query, { tier: "global" });

    cleanup();
  });

  it("warns once and ignores a snapshot that changes after the first render", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const query = makeQuery("inbox");
    let snapshot = $state<any>({ id: "a" });

    const cleanup = $effect.root(() => {
      new QuerySubscription(query, () => ({ snapshot }));
    });
    await settle();
    expect(mocks.applySnapshot).toHaveBeenCalledTimes(1);
    expect(warn).not.toHaveBeenCalled();

    snapshot = { id: "b" };
    await settle();

    // The changed snapshot is ignored (not re-applied) and flagged exactly once.
    expect(mocks.applySnapshot).toHaveBeenCalledTimes(1);
    expect(warn).toHaveBeenCalledTimes(1);
    expect(String(warn.mock.calls[0]?.[0])).toContain("snapshot");

    warn.mockRestore();
    cleanup();
  });
});
