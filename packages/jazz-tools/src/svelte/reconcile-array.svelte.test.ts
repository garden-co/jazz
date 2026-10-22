import { describe, expect, it } from "vitest";
import { flushSync } from "svelte";
import { applyDelta, reconcileArray } from "../reconcile-array.js";
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

// ── applyDelta skips unchanged rows ─────────────────────────────────

describe("applyDelta only touches changed rows", () => {
  it("$effect on unchanged row does not re-fire when only another row updates", async () => {
    //  alice and bob in the array; only bob changes.
    //  An $effect watching alice.name should NOT re-fire.
    let items: Array<{ id: string; name: string }> = $state([
      { id: "1", name: "Alice" },
      { id: "2", name: "Bob" },
    ]);

    let aliceEffectCount = 0;
    let bobEffectCount = 0;
    const cleanup = $effect.root(() => {
      $effect(() => {
        void items[0]?.name;
        aliceEffectCount++;
      });
      $effect(() => {
        void items[1]?.name;
        bobEffectCount++;
      });
    });

    await Promise.resolve();
    flushSync();
    expect(aliceEffectCount).toBe(1);
    expect(bobEffectCount).toBe(1);

    // Delta: only bob updated
    applyDelta(items, {
      all: [
        { id: "1", name: "Alice" },
        { id: "2", name: "Bob (v2)" },
      ],
      delta: [{ kind: 2, id: "2", index: 1 }],
    });
    await Promise.resolve();
    flushSync();

    expect(aliceEffectCount).toBe(1); // alice untouched — no re-fire
    expect(bobEffectCount).toBe(2); // bob changed — re-fired
    expect(items[1].name).toBe("Bob (v2)");

    cleanup();
  });
});

describe("applyDelta through $state proxy", () => {
  it("onDelta reconciles into existing $state array", () => {
    let current: any[] | undefined = $state([{ id: "1", title: "First" }]);

    const firstRef = current![0];

    applyDelta(current!, {
      all: [
        { id: "1", title: "First (edited)" },
        { id: "2", title: "Second" },
      ],
      delta: [
        { kind: 2, id: "1", index: 0 },
        { kind: 0, id: "2", index: 1, item: { id: "2", title: "Second" } },
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

    applyDelta(current!, {
      all: [
        { id: "2", name: "Bob" },
        { id: "3", name: "Carol" },
        { id: "4", name: "Dave" },
      ],
      delta: [
        { kind: 1, id: "1", index: 0 },
        { kind: 0, id: "4", index: 2, item: { id: "4", name: "Dave" } },
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

    applyDelta(current!, {
      all: [
        { id: "2", name: "Bob" },
        { id: "4", name: "Dave" },
      ],
      delta: [
        { kind: 1, id: "1", index: 0 },
        { kind: 1, id: "3", index: 2 },
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

    applyDelta(current!, {
      all: [
        { id: "2", name: "Bob", score: 20 },
        { id: "3", name: "Carol", score: 30 },
        { id: "1", name: "Alice", score: 5 },
      ],
      delta: [{ kind: 2, id: "1", index: 2 }],
    });
    flushSync();

    expect(current).toHaveLength(3);
    expect(current![0]).toBe(bobRef);
    expect(current![1]).toBe(carolRef);
    expect(current![2]).toBe(aliceRef); // moved, identity preserved
    expect(current![2].score).toBe(5); // property updated
  });
});
