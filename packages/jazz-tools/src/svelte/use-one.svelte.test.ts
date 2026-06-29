import { beforeEach, describe, expect, it, vi } from "vitest";
import { flushSync } from "svelte";
import "./test-helpers.svelte.js";

type Row = { id: string; title?: string };

const mocks = vi.hoisted(() => {
  const unsubscribe = vi.fn();
  const subscribe = vi.fn(() => unsubscribe);
  // Derive the key from the built query so distinct queries get distinct keys
  // even after `limitQueryToOne` strips the `_marker` field.
  const makeQueryKey = vi.fn((q: any) => `key:${q?._build?.() ?? "?"}`);
  const getCacheEntry = vi.fn(() => ({
    state: { status: "fulfilled", data: [] as Row[] },
    subscribe,
  }));

  return {
    makeQueryKey,
    getCacheEntry,
    subscribe,
    unsubscribe,
    reset() {
      unsubscribe.mockReset();
      subscribe.mockReset().mockReturnValue(unsubscribe);
      makeQueryKey.mockReset().mockImplementation((q: any) => `key:${q?._build?.() ?? "?"}`);
      getCacheEntry.mockReset().mockReturnValue({
        state: { status: "fulfilled", data: [] as Row[] },
        subscribe,
      });
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

const { SingleRowSubscription } = await import("./use-one.svelte.js");

function makeQuery(marker = "todos") {
  return { _marker: marker, _build: () => JSON.stringify({ table: marker }) } as any;
}

async function settle() {
  await Promise.resolve();
  flushSync();
}

describe("svelte/SingleRowSubscription", () => {
  beforeEach(() => {
    mocks.reset();
  });

  it("subscribes with the query limited to one row", async () => {
    const query = makeQuery("inbox");
    const cleanup = $effect.root(() => {
      new SingleRowSubscription(query);
    });
    await settle();

    expect(mocks.subscribe).toHaveBeenCalledTimes(1);
    expect(mocks.makeQueryKey).toHaveBeenCalledTimes(1);

    const builtQuery = mocks.makeQueryKey.mock.calls[0]![0];
    expect(JSON.parse(builtQuery._build())).toMatchObject({ limit: 1 });

    cleanup();
  });

  it("accepts a getter and subscribes with the resolved query limited to one row", async () => {
    const query = makeQuery("inbox");
    const cleanup = $effect.root(() => {
      new SingleRowSubscription(() => query);
    });
    await settle();

    expect(mocks.subscribe).toHaveBeenCalledTimes(1);
    const builtQuery = mocks.makeQueryKey.mock.calls[0]![0];
    expect(JSON.parse(builtQuery._build())).toMatchObject({ limit: 1 });

    cleanup();
  });

  it("does not subscribe when given undefined", async () => {
    const cleanup = $effect.root(() => {
      new SingleRowSubscription(undefined);
    });
    await settle();

    expect(mocks.makeQueryKey).not.toHaveBeenCalled();
    expect(mocks.subscribe).not.toHaveBeenCalled();

    cleanup();
  });

  it("reflects the row from the initial fulfilled cache", async () => {
    const row: Row = { id: "t1", title: "first" };
    mocks.getCacheEntry.mockReturnValue({
      state: { status: "fulfilled" as const, data: [row] },
      subscribe: mocks.subscribe,
    } as any);

    let ref!: InstanceType<typeof SingleRowSubscription<Row>>;
    const cleanup = $effect.root(() => {
      ref = new SingleRowSubscription(makeQuery());
    });
    await settle();

    expect(ref.loading).toBe(false);
    expect(ref.error).toBeNull();
    expect(ref.current).toEqual({ id: "t1", title: "first" });

    cleanup();
  });

  it("reflects later deltas to the single row", async () => {
    const row: Row = { id: "t1", title: "first" };
    let captured: any;
    mocks.getCacheEntry.mockReturnValue({
      state: { status: "fulfilled" as const, data: [row] },
      subscribe: (callbacks: any) => {
        captured = callbacks;
        return mocks.unsubscribe;
      },
    } as any);

    let ref!: InstanceType<typeof SingleRowSubscription<Row>>;
    const cleanup = $effect.root(() => {
      ref = new SingleRowSubscription(makeQuery());
    });
    await settle();

    expect(ref.current).toMatchObject({ id: "t1", title: "first" });

    captured.onDelta({
      all: [{ id: "t1", title: "updated" }],
      delta: [{ kind: 2, id: "t1" }],
    });
    await settle();

    expect(ref.current).toMatchObject({ id: "t1", title: "updated" });

    cleanup();
  });

  it("yields null (distinct from undefined) once resolved with no row", async () => {
    let ref!: InstanceType<typeof SingleRowSubscription<Row>>;
    const cleanup = $effect.root(() => {
      ref = new SingleRowSubscription(makeQuery());
    });
    await settle();

    // fulfilled cache with empty data → resolved with no match → null
    expect(ref.loading).toBe(false);
    expect(ref.current).toBeNull();
    expect(ref.current).not.toBeUndefined();

    cleanup();
  });

  it("current is undefined while loading (pending cache)", async () => {
    mocks.getCacheEntry.mockReturnValue({
      state: { status: "pending" as const },
      subscribe: mocks.subscribe,
    } as any);

    let ref!: InstanceType<typeof SingleRowSubscription<Row>>;
    const cleanup = $effect.root(() => {
      ref = new SingleRowSubscription(makeQuery());
    });
    await settle();

    expect(ref.loading).toBe(true);
    expect(ref.current).toBeUndefined();

    cleanup();
  });

  it("surfaces subscription errors", async () => {
    let captured: any;
    mocks.getCacheEntry.mockReturnValue({
      state: { status: "fulfilled" as const, data: [] as Row[] },
      subscribe: (callbacks: any) => {
        captured = callbacks;
        return mocks.unsubscribe;
      },
    } as any);

    let ref!: InstanceType<typeof SingleRowSubscription<Row>>;
    const cleanup = $effect.root(() => {
      ref = new SingleRowSubscription(makeQuery());
    });
    await settle();

    captured.onError(new Error("boom"));
    await settle();

    expect(ref.error).toBeInstanceOf(Error);
    expect(ref.error?.message).toBe("boom");
    expect(ref.current).toBeUndefined();

    cleanup();
  });
});
