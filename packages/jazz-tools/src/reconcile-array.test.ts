import { describe, expect, it } from "vitest";
import { reconcileArray } from "./reconcile-array.js";

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

  it("skips property writes when values are identical", () => {
    const alice = { id: "1", name: "Alice" };
    const target = [alice];

    reconcileArray(target, [{ id: "1", name: "Alice" }]);

    expect(target[0]!).toBe(alice);
    expect(target[0]!.name).toBe("Alice");
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
