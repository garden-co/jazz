import { describe, expect, it, vi } from "vitest";
import { flushSync } from "svelte";
import { reconcileArray } from "../reconcile-array.js";
import "./test-helpers.svelte.js";

// ── reconcileArray through $state proxy ────────────────────────────
// Prove that reconcileArray's in-place mutations propagate correctly
// through Svelte's reactive proxy.

describe("reconcileArray through $state proxy", () => {
  it("preserves object identity after reconciliation", () => {
    let items: Array<{ id: string; name: string }> = $state([{ id: "1", name: "Alice" }]);

    const ref = items[0];
    reconcileArray(items, [{ id: "1", name: "Alice (updated)" }]);
    flushSync();

    expect(items[0]).toBe(ref);
    expect(items[0].name).toBe("Alice (updated)");
  });

  it("appends new items and removes stale ones", () => {
    let items: Array<{ id: string; name: string }> = $state([
      { id: "1", name: "Alice" },
      { id: "2", name: "Bob" },
    ]);

    reconcileArray(items, [
      { id: "2", name: "Bob" },
      { id: "3", name: "Carol" },
    ]);
    flushSync();

    expect(items).toHaveLength(2);
    expect(items[0].name).toBe("Bob");
    expect(items[1].name).toBe("Carol");
  });

  it("$effect observes property changes from reconciliation", async () => {
    let items: Array<{ id: string; name: string }> = $state([{ id: "1", name: "Alice" }]);

    const observed: string[] = [];
    const cleanup = $effect.root(() => {
      $effect(() => {
        observed.push(items[0]?.name ?? "empty");
      });
    });

    await Promise.resolve();
    flushSync();
    expect(observed).toEqual(["Alice"]);

    reconcileArray(items, [{ id: "1", name: "Alice (v2)" }]);
    await Promise.resolve();
    flushSync();
    expect(observed).toEqual(["Alice", "Alice (v2)"]);

    cleanup();
  });

  it("$effect does not re-fire when reconciled values are identical", async () => {
    let items: Array<{ id: string; name: string }> = $state([{ id: "1", name: "Alice" }]);

    let effectCount = 0;
    const cleanup = $effect.root(() => {
      $effect(() => {
        void items[0]?.name;
        effectCount++;
      });
    });

    await Promise.resolve();
    flushSync();
    expect(effectCount).toBe(1);

    reconcileArray(items, [{ id: "1", name: "Alice" }]);
    await Promise.resolve();
    flushSync();
    expect(effectCount).toBe(1);

    cleanup();
  });
});

// ── Rune behaviour smoke tests ─────────────────────────────────────
// These verify that the callback patterns used by QuerySubscription
// behave correctly under real $state proxying and flushSync — not
// the useAll hook itself, but the reactive primitives it relies on.

describe("$state callback patterns", () => {
  it("onfulfilled delivers data and clears loading", () => {
    let current: any[] | undefined = $state();
    let loading: boolean = $state(true);

    const onfulfilled = (data: any[]) => {
      current = data;
      loading = false;
    };

    onfulfilled([{ id: "1", title: "Alice's todo" }]);
    flushSync();

    expect($state.snapshot(current)).toEqual([{ id: "1", title: "Alice's todo" }]);
    expect(loading).toBe(false);
  });

  it("onDelta reconciles into existing $state array", () => {
    //  ┌──────────┐    onDelta    ┌──────────────────┐
    //  │ current  │ ──────────▶   │ reconcileArray() │
    //  │ $state[] │    delta.all  │ in-place merge   │
    //  └──────────┘               └──────────────────┘
    let current: any[] | undefined = $state([{ id: "1", title: "First" }]);

    const firstRef = current![0];

    const onDelta = (delta: { all: any[] }) => {
      if (current) {
        reconcileArray(current, delta.all);
      } else {
        current = delta.all;
      }
    };

    onDelta({
      all: [
        { id: "1", title: "First (edited)" },
        { id: "2", title: "Second" },
      ],
    });
    flushSync();

    expect(current).toHaveLength(2);
    expect(current![0]).toBe(firstRef);
    expect(current![0].title).toBe("First (edited)");
    expect(current![1].title).toBe("Second");
  });

  it("batch delta: remove + add preserves correct items through $state", () => {
    //  Before: [alice, bob, carol]
    //  Delta:  remove alice, add dave
    //  After:  [bob, carol, dave]
    let current: any[] | undefined = $state([
      { id: "1", name: "Alice" },
      { id: "2", name: "Bob" },
      { id: "3", name: "Carol" },
    ]);

    const bobRef = current![1];
    const carolRef = current![2];

    const onDelta = (delta: { all: any[] }) => {
      if (current) {
        reconcileArray(current, delta.all);
      } else {
        current = delta.all;
      }
    };

    onDelta({
      all: [
        { id: "2", name: "Bob" },
        { id: "3", name: "Carol" },
        { id: "4", name: "Dave" },
      ],
    });
    flushSync();

    expect(current).toHaveLength(3);
    expect(current![0]).toBe(bobRef);
    expect(current![1]).toBe(carolRef);
    expect(current![2].name).toBe("Dave");
  });

  it("batch delta: two removes preserves survivors through $state", () => {
    //  Before: [alice, bob, carol, dave]
    //  Delta:  remove alice, remove carol
    //  After:  [bob, dave]
    let current: any[] | undefined = $state([
      { id: "1", name: "Alice" },
      { id: "2", name: "Bob" },
      { id: "3", name: "Carol" },
      { id: "4", name: "Dave" },
    ]);

    const bobRef = current![1];
    const daveRef = current![3];

    const onDelta = (delta: { all: any[] }) => {
      if (current) {
        reconcileArray(current, delta.all);
      } else {
        current = delta.all;
      }
    };

    onDelta({
      all: [
        { id: "2", name: "Bob" },
        { id: "4", name: "Dave" },
      ],
    });
    flushSync();

    expect(current).toHaveLength(2);
    expect(current![0]).toBe(bobRef);
    expect(current![1]).toBe(daveRef);
  });

  it("updated item changes position, array reorders through $state", () => {
    //  Before: [alice, bob, carol]
    //  Delta:  alice updated and moved to end (e.g. sort order changed)
    //  After:  [bob, carol, alice']
    let current: any[] | undefined = $state([
      { id: "1", name: "Alice", score: 10 },
      { id: "2", name: "Bob", score: 20 },
      { id: "3", name: "Carol", score: 30 },
    ]);

    const aliceRef = current![0];
    const bobRef = current![1];
    const carolRef = current![2];

    const onDelta = (delta: { all: any[] }) => {
      if (current) {
        reconcileArray(current, delta.all);
      } else {
        current = delta.all;
      }
    };

    onDelta({
      all: [
        { id: "2", name: "Bob", score: 20 },
        { id: "3", name: "Carol", score: 30 },
        { id: "1", name: "Alice", score: 5 },
      ],
    });
    flushSync();

    expect(current).toHaveLength(3);
    expect(current![0]).toBe(bobRef);
    expect(current![1]).toBe(carolRef);
    expect(current![2]).toBe(aliceRef); // moved, identity preserved
    expect(current![2].score).toBe(5); // property updated
  });

  it("onError surfaces error and clears current", () => {
    let current: any[] | undefined = $state([{ id: "1" }]);
    let loading: boolean = $state(false);
    let error: Error | null = $state(null);

    const onError = (e: unknown) => {
      error = e instanceof Error ? e : new Error(String(e));
      current = undefined;
      loading = false;
    };

    onError(new Error("subscription failed"));
    flushSync();

    expect(error).toBeInstanceOf(Error);
    expect(error!.message).toBe("subscription failed");
    expect(current).toBeUndefined();
    expect(loading).toBe(false);
  });

  it("onError wraps non-Error values in Error", () => {
    let error: Error | null = $state(null);

    const onError = (e: unknown) => {
      error = e instanceof Error ? e : new Error(String(e));
    };

    onError("string error");
    flushSync();

    expect(error).toBeInstanceOf(Error);
    expect(error!.message).toBe("string error");
  });

  it("synchronous throw during setup is caught and surfaced", () => {
    let error: Error | null = $state(null);
    let loading: boolean = $state(true);

    // Mirrors the try/catch in the $effect body
    try {
      throw new Error("getCacheEntry exploded");
    } catch (e) {
      error = e instanceof Error ? e : new Error(String(e));
      loading = false;
    }

    expect(error).toBeInstanceOf(Error);
    expect(error!.message).toBe("getCacheEntry exploded");
    expect(loading).toBe(false);
  });
});

describe("initial value semantics", () => {
  it("with tier → undefined (awaiting settlement)", () => {
    const options = { tier: "edge" as const };
    const initial = options?.tier ? undefined : [];
    expect(initial).toBeUndefined();
  });

  it("without tier → empty array (locally available)", () => {
    const options = {};
    const initial = (options as any)?.tier ? undefined : [];
    expect(initial).toEqual([]);
  });

  it("without options → empty array", () => {
    const options = undefined as { tier?: string } | undefined;
    const initial = options?.tier ? undefined : [];
    expect(initial).toEqual([]);
  });
});

describe("fulfilled cache entry provides initial data", () => {
  it("applies fulfilled state before subscribing", () => {
    const alice = { id: "1", name: "Alice" };
    const entry = {
      state: { status: "fulfilled" as const, data: [alice] },
      subscribe: vi.fn(() => vi.fn()),
    };

    let current: any[] | undefined = $state();
    let loading: boolean = $state(true);

    if (entry.state.status === "fulfilled") {
      current = entry.state.data;
      loading = false;
    }

    expect($state.snapshot(current)).toEqual([alice]);
    expect(loading).toBe(false);
  });
});
