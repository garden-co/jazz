import { describe, expect, it } from "vitest";
import { applyDelta, reconcileArray } from "./reconcile-array.js";
import {
  applySubscriptionDelta,
  RowChangeKind,
  type RowDelta,
  type SubscriptionDelta,
} from "./runtime/subscription-manager.js";

describe("reconcileArray", () => {
  it("preserves identity for matched items", () => {
    const alice = { id: "1", name: "Alice" };
    const target = [alice];

    reconcileArray(target, [{ id: "1", name: "Alice (v2)" }]);

    expect(target[0]!).toBe(alice);
    expect(target[0]!.name).toBe("Alice (v2)");
  });

  it("appends new items", () => {
    const target = [{ id: "1", name: "Alice" }];

    reconcileArray(target, [
      { id: "1", name: "Alice" },
      { id: "2", name: "Bob" },
    ]);

    expect(target).toHaveLength(2);
    expect(target[1]!.name).toBe("Bob");
  });

  it("removes items not in source", () => {
    const target = [
      { id: "1", name: "Alice" },
      { id: "2", name: "Bob" },
    ];

    reconcileArray(target, [{ id: "2", name: "Bob" }]);

    expect(target).toHaveLength(1);
    expect(target[0]!.name).toBe("Bob");
  });

  it("reorders to match source order", () => {
    const alice = { id: "1", name: "Alice" };
    const bob = { id: "2", name: "Bob" };
    const target = [alice, bob];

    reconcileArray(target, [
      { id: "2", name: "Bob" },
      { id: "1", name: "Alice" },
    ]);

    expect(target[0]!).toBe(bob);
    expect(target[1]!).toBe(alice);
  });

  it("handles empty source (clears target)", () => {
    const target = [{ id: "1", name: "Alice" }];

    reconcileArray(target, []);

    expect(target).toHaveLength(0);
  });

  it("handles empty target (populates from source)", () => {
    const target: Array<{ id: string; name: string }> = [];

    reconcileArray(target, [{ id: "1", name: "Alice" }]);

    expect(target).toHaveLength(1);
    expect(target[0]!.name).toBe("Alice");
  });

  it("clears removed indexes on reactive array proxies", () => {
    type Item = { id: string; name: string };
    const target = [
      { id: "1", name: "Alice" },
      { id: "2", name: "Bob" },
    ];
    const staleSignals = new Map<string, Item>();
    const reactiveTarget = new Proxy(target, {
      get(array, property, receiver) {
        if (typeof property === "string" && /^\d+$/.test(property)) {
          if (Number(property) < array.length) {
            staleSignals.set(property, Reflect.get(array, property, receiver));
          } else if (staleSignals.has(property)) {
            return staleSignals.get(property);
          }
        }
        return Reflect.get(array, property, receiver);
      },
      deleteProperty(array, property) {
        if (typeof property === "string") staleSignals.delete(property);
        return Reflect.deleteProperty(array, property);
      },
    });

    reconcileArray(reactiveTarget, [{ id: "1", name: "Alice" }]);

    expect(reactiveTarget[1]).toBeUndefined();
  });

  it("skips property writes when values are identical", () => {
    const alice = { id: "1", name: "Alice" };
    const target = [alice];

    reconcileArray(target, [{ id: "1", name: "Alice" }]);

    expect(target[0]!).toBe(alice);
    expect(target[0]!.name).toBe("Alice");
  });

  it("deletes dropped index properties so proxy-cached reads cannot resurface old rows", () => {
    const deletedIndices: string[] = [];
    const indexCache = new Map<string, unknown>();
    const invalidate = (key: string | symbol) => {
      if (typeof key === "string") indexCache.delete(key);
    };
    const rows = [
      { id: "1", name: "Alice" },
      { id: "2", name: "Bob" },
      { id: "3", name: "Cara" },
    ];
    // Stand-in for a reactive array proxy that materializes per-index state:
    // only set/deleteProperty traps for an index invalidate its cached read.
    const proxy = new Proxy(rows, {
      set(target, key, value, receiver) {
        invalidate(key);
        return Reflect.set(target, key, value, receiver);
      },
      deleteProperty(target, key) {
        if (typeof key === "string") deletedIndices.push(key);
        invalidate(key);
        return Reflect.deleteProperty(target, key);
      },
    });
    const readCached = (index: number): unknown => {
      const key = String(index);
      if (!indexCache.has(key)) indexCache.set(key, proxy[index]);
      return indexCache.get(key);
    };
    expect(readCached(1)).toEqual({ id: "2", name: "Bob" });
    expect(readCached(2)).toEqual({ id: "3", name: "Cara" });

    reconcileArray(proxy, [{ id: "1", name: "Alice" }]);

    expect([...deletedIndices].sort()).toEqual(["1", "2"]);
    expect(readCached(1)).toBeUndefined();
    expect(readCached(2)).toBeUndefined();
  });

  it("does not resurface a truncated row when a later reconciliation regrows the array", () => {
    const indexCache = new Map<string, unknown>();
    const rows = [
      { id: "1", name: "Alice" },
      { id: "2", name: "Bob" },
    ];
    const proxy = new Proxy(rows, {
      set(target, key, value, receiver) {
        if (typeof key === "string") indexCache.delete(key);
        return Reflect.set(target, key, value, receiver);
      },
      deleteProperty(target, key) {
        if (typeof key === "string") indexCache.delete(key);
        return Reflect.deleteProperty(target, key);
      },
    });
    const readCached = (index: number): unknown => {
      const key = String(index);
      if (!indexCache.has(key)) indexCache.set(key, proxy[index]);
      return indexCache.get(key);
    };

    expect(readCached(1)).toEqual({ id: "2", name: "Bob" });
    reconcileArray(proxy, [{ id: "1", name: "Alice" }]);
    expect(readCached(1)).toBeUndefined();

    reconcileArray(proxy, [
      { id: "1", name: "Alice" },
      { id: "3", name: "Cara" },
    ]);
    expect(readCached(1)).toEqual({ id: "3", name: "Cara" });
  });
});

describe("deepMerge (via reconcileArray)", () => {
  it("deep-merges nested plain objects", () => {
    const target = [{ id: "1", profile: { bio: "old", age: 30 } }];
    const original = target[0]!;

    reconcileArray(target, [{ id: "1", profile: { bio: "new", age: 30 } }]);

    expect(target[0]!).toBe(original);
    expect(target[0]!.profile.bio).toBe("new");
    expect(target[0]!.profile.age).toBe(30);
  });

  it("recursively reconciles nested keyed arrays", () => {
    const target = [
      {
        id: "1",
        tags: [{ id: "t1", label: "jazz" }],
      },
    ];
    const original = target[0]!;
    const originalTag = target[0]!.tags[0]!;

    reconcileArray(target, [
      {
        id: "1",
        tags: [
          { id: "t1", label: "jazz (updated)" },
          { id: "t2", label: "svelte" },
        ],
      },
    ]);

    expect(target[0]!).toBe(original);
    expect(target[0]!.tags[0]!).toBe(originalTag);
    expect(target[0]!.tags[0]!.label).toBe("jazz (updated)");
    expect(target[0]!.tags).toHaveLength(2);
  });

  it("handles Date values correctly", () => {
    const now = new Date("2026-01-01");
    const target = [{ id: "1", createdAt: now }];

    // Same date value — should not replace
    reconcileArray(target, [{ id: "1", createdAt: new Date("2026-01-01") }]);
    expect(target[0]!.createdAt).toBe(now);

    // Different date — should replace
    reconcileArray(target, [{ id: "1", createdAt: new Date("2026-06-01") }]);
    expect(target[0]!.createdAt).toEqual(new Date("2026-06-01"));
  });

  it("handles Uint8Array values correctly", () => {
    const bytes = new Uint8Array([1, 2, 3]);
    const target = [{ id: "1", data: bytes }];

    // Same bytes — should not replace
    reconcileArray(target, [{ id: "1", data: new Uint8Array([1, 2, 3]) }]);
    expect(target[0]!.data).toBe(bytes);

    // Different bytes — should replace
    reconcileArray(target, [{ id: "1", data: new Uint8Array([4, 5, 6]) }]);
    expect(target[0]!.data).toEqual(new Uint8Array([4, 5, 6]));
  });

  it("deletes keys removed from source", () => {
    const target = [{ id: "1", name: "Alice", legacy: "old" } as Record<string, unknown>];

    reconcileArray(target as Array<{ id: string }>, [{ id: "1", name: "Alice" } as { id: string }]);

    expect(target[0]!.name).toBe("Alice");
    expect("legacy" in target[0]!).toBe(false);
  });
});

describe("applyDelta", () => {
  type Row = { id: string; name: string; tags: string[] };
  const row = (id: string, name: string): Row => ({ id, name, tags: [name] });

  /** Rows that record whether anything enumerated or wrote to them. */
  function watched(rows: Row[]): { rows: Row[]; touched: Set<string> } {
    const touched = new Set<string>();
    return {
      touched,
      rows: rows.map(
        (item) =>
          new Proxy(item, {
            ownKeys(target) {
              touched.add(target.id);
              return Reflect.ownKeys(target);
            },
            set(target, key, value) {
              touched.add(target.id);
              return Reflect.set(target, key, value);
            },
          }),
      ),
    };
  }

  it("merges only the changed row of a one-row update", () => {
    const { rows, touched } = watched(Array.from({ length: 100 }, (_, i) => row(`${i}`, `r${i}`)));
    const target = [...rows];
    const all = [...rows];
    all[42] = row("42", "changed");

    applyDelta(target, {
      delta: [{ kind: RowChangeKind.Updated, id: "42", index: 42, item: all[42] }],
      all,
    });

    expect(target[42]).toBe(rows[42]);
    expect(target[42]!.name).toBe("changed");
    expect([...touched]).toEqual(["42"]);
  });

  it("matches reconciling against the full result for random deltas", () => {
    let seed = 7;
    const rnd = (n: number) => {
      seed = (seed * 1103515245 + 12345) & 0x7fffffff;
      return (seed >>> 8) % n;
    };
    let nextId = 0;
    const expected: Row[] = [];
    const incremental: Row[] = [];
    const reconciled: Row[] = [];
    for (let step = 0; step < 400; step++) {
      const changes: RowDelta<Row>[] = [];
      const used = new Set<string>();
      for (let n = rnd(5); n > 0; n--) {
        const kind = expected.length === 0 ? 0 : rnd(3);
        if (kind === 0) {
          const id = `${nextId++}`;
          changes.push({
            kind: RowChangeKind.Added,
            id,
            index: rnd(expected.length + 2),
            item: row(id, `a${step}`),
          });
          used.add(id);
          continue;
        }
        const id = expected[rnd(expected.length)]!.id;
        if (used.has(id)) continue;
        used.add(id);
        changes.push(
          kind === 1
            ? { kind: RowChangeKind.Removed, id, index: rnd(expected.length) }
            : {
                kind: RowChangeKind.Updated,
                id,
                index: rnd(expected.length),
                ...(rnd(2) ? { item: row(id, `u${step}`) } : {}),
              },
        );
      }
      changes.sort((a, b) => a.index - b.index);
      applySubscriptionDelta(expected, { delta: changes });
      const delta: SubscriptionDelta<Row> = { delta: changes, all: [...expected] };
      const before = new Map(incremental.map((item) => [item.id, item]));

      applyDelta(incremental, delta);
      reconcileArray(reconciled, delta.all!);

      expect(incremental).toEqual(expected);
      expect(incremental).toEqual(reconciled);
      for (const item of incremental) {
        const previous = before.get(item.id);
        if (previous) expect(item).toBe(previous);
      }
    }
  });

  it("falls back to a full reconcile when the result disagrees with the target", () => {
    const target = [row("1", "one"), row("2", "two")];
    const all = [row("0", "zero"), row("1", "one"), row("2", "two"), row("3", "three")];

    applyDelta(target, {
      delta: [{ kind: RowChangeKind.Added, id: "3", index: 3, item: all[3]! }],
      all,
    });

    expect(target).toEqual(all);
  });

  it("applies a delta without a full result by id and index", () => {
    const target = [row("1", "one"), row("2", "two"), row("3", "three")];
    const first = target[0]!;

    applyDelta(target, {
      delta: [
        { kind: RowChangeKind.Removed, id: "2", index: 1 },
        { kind: RowChangeKind.Updated, id: "1", index: 1, item: row("1", "uno") },
      ],
    });

    expect(target.map((item) => item.name)).toEqual(["three", "uno"]);
    expect(target[1]).toBe(first);
  });
});
