/**
 * Tests for subscription-manager module.
 */

import { describe, it, expect } from "vitest";
import { SubscriptionManager } from "./subscription-manager.js";
import type { WasmRow, RowDelta } from "../drivers/types.js";

interface TestItem {
  id: string;
  name: string;
  count: number;
}

// Helper to create a WasmRow
function makeRow(id: string, name: string, count: number): WasmRow {
  return {
    id,
    values: [
      { type: "Text", value: name },
      { type: "Integer", value: count },
    ],
  };
}

// Simple transform function for tests
function transform(row: WasmRow): TestItem {
  return {
    id: row.id,
    name: (row.values[0] as { type: "Text"; value: string }).value,
    count: (row.values[1] as { type: "Integer"; value: number }).value,
  };
}

describe("SubscriptionManager", () => {
  const add = (id: string, name: string, count: number, index: number) => ({
    row: makeRow(id, name, count),
    index,
  });
  const remove = (id: string, name: string, count: number, index: number) => ({
    row: makeRow(id, name, count),
    index,
  });
  const update = (
    oldId: string,
    oldName: string,
    oldCount: number,
    oldIndex: number,
    newId: string,
    newName: string,
    newCount: number,
    newIndex: number,
  ) => ({
    old_row: makeRow(oldId, oldName, oldCount),
    new_row: makeRow(newId, newName, newCount),
    old_index: oldIndex,
    new_index: newIndex,
  });
  const delta = (input: Partial<RowDelta>): RowDelta => ({
    added: input.added ?? [],
    removed: input.removed ?? [],
    updated: input.updated ?? [],
    pending: input.pending ?? false,
  });

  it("append add reports tail index", () => {
    const manager = new SubscriptionManager<TestItem>();

    manager.handleDelta(delta({ added: [add("1", "a", 1, 0), add("2", "b", 2, 1)] }), transform);
    const result = manager.handleDelta(delta({ added: [add("3", "c", 3, 2)] }), transform);

    expect(result.added).toEqual([{ item: { id: "3", name: "c", count: 3 }, index: 2 }]);
    expect(result.all.map((item) => item.id)).toEqual(["1", "2", "3"]);
  });

  it("middle insert preserves order via index", () => {
    const manager = new SubscriptionManager<TestItem>();

    manager.handleDelta(delta({ added: [add("1", "a", 10, 0), add("3", "c", 30, 1)] }), transform);
    const result = manager.handleDelta(delta({ added: [add("2", "b", 20, 1)] }), transform);

    expect(result.added[0].index).toBe(1);
    expect(result.all.map((item) => item.id)).toEqual(["1", "2", "3"]);
  });

  it("update without move keeps oldIndex/newIndex equal", () => {
    const manager = new SubscriptionManager<TestItem>();

    manager.handleDelta(delta({ added: [add("1", "a", 10, 0), add("2", "b", 20, 1)] }), transform);
    const result = manager.handleDelta(
      delta({ updated: [update("2", "b", 20, 1, "2", "b", 25, 1)] }),
      transform,
    );

    expect(result.updated).toEqual([
      {
        oldItem: { id: "2", name: "b", count: 20 },
        newItem: { id: "2", name: "b", count: 25 },
        oldIndex: 1,
        newIndex: 1,
      },
    ]);
    expect(result.all.map((item) => item.id)).toEqual(["1", "2"]);
    expect(result.all[1]?.count).toBe(25);
  });

  it("update with move applies old/new indices", () => {
    const manager = new SubscriptionManager<TestItem>();

    manager.handleDelta(
      delta({ added: [add("1", "a", 10, 0), add("2", "b", 20, 1), add("3", "c", 30, 2)] }),
      transform,
    );
    const result = manager.handleDelta(
      delta({ updated: [update("3", "c", 30, 2, "3", "c", 5, 0)] }),
      transform,
    );

    expect(result.updated[0]?.oldIndex).toBe(2);
    expect(result.updated[0]?.newIndex).toBe(0);
    expect(result.all.map((item) => item.id)).toEqual(["3", "1", "2"]);
  });

  it("remove reports prior index", () => {
    const manager = new SubscriptionManager<TestItem>();

    manager.handleDelta(
      delta({ added: [add("1", "a", 10, 0), add("2", "b", 20, 1), add("3", "c", 30, 2)] }),
      transform,
    );
    const result = manager.handleDelta(delta({ removed: [remove("2", "b", 20, 1)] }), transform);

    expect(result.removed).toEqual([{ item: { id: "2", name: "b", count: 20 }, index: 1 }]);
    expect(result.all.map((item) => item.id)).toEqual(["1", "3"]);
  });

  it("mixed batch yields consistent indices and final all", () => {
    const manager = new SubscriptionManager<TestItem>();

    manager.handleDelta(
      delta({ added: [add("1", "a", 10, 0), add("2", "b", 20, 1), add("3", "c", 30, 2)] }),
      transform,
    );
    const result = manager.handleDelta(
      delta({
        removed: [remove("2", "b", 20, 1)],
        updated: [update("3", "c", 30, 2, "3", "c", 31, 0)],
        added: [add("4", "d", 40, 2)],
      }),
      transform,
    );

    expect(result.removed[0]?.index).toBe(1);
    expect(result.updated[0]?.oldIndex).toBe(2);
    expect(result.updated[0]?.newIndex).toBe(0);
    expect(result.added[0]?.index).toBe(2);
    expect(result.all.map((item) => item.id)).toEqual(["3", "1", "4"]);
  });

  it("identity-changing update is treated as remove+add", () => {
    const manager = new SubscriptionManager<TestItem>();

    manager.handleDelta(delta({ added: [add("1", "a", 10, 0)] }), transform);
    const result = manager.handleDelta(
      delta({ updated: [update("1", "a", 10, 0, "9", "z", 99, 0)] }),
      transform,
    );

    expect(result.updated).toEqual([]);
    expect(result.removed).toEqual([{ item: { id: "1", name: "a", count: 10 }, index: 0 }]);
    expect(result.added).toEqual([{ item: { id: "9", name: "z", count: 99 }, index: 0 }]);
    expect(result.all.map((item) => item.id)).toEqual(["9"]);
  });

  describe("clear", () => {
    it("clears all tracked state", () => {
      const manager = new SubscriptionManager<TestItem>();

      // Add items
      manager.handleDelta(
        {
          added: [add("1", "item1", 10, 0), add("2", "item2", 20, 1)],
          removed: [],
          updated: [],
          pending: false,
        },
        transform,
      );

      expect(manager.size).toBe(2);

      manager.clear();

      expect(manager.size).toBe(0);

      // Next delta should start fresh
      const result = manager.handleDelta(
        {
          added: [add("3", "item3", 30, 0)],
          removed: [],
          updated: [],
          pending: false,
        },
        transform,
      );

      expect(result.all).toHaveLength(1);
      expect(result.all[0].id).toBe("3");
    });
  });

  describe("all array", () => {
    it("returns current state after delta", () => {
      const manager = new SubscriptionManager<TestItem>();

      const result1 = manager.handleDelta(
        {
          added: [add("1", "item1", 10, 0)],
          removed: [],
          updated: [],
          pending: false,
        },
        transform,
      );

      expect(result1.all).toEqual([{ id: "1", name: "item1", count: 10 }]);

      const result2 = manager.handleDelta(
        {
          added: [add("2", "item2", 20, 1)],
          removed: [],
          updated: [],
          pending: false,
        },
        transform,
      );

      expect(result2.all).toHaveLength(2);
    });

    it("returns empty array initially", () => {
      const manager = new SubscriptionManager<TestItem>();

      const result = manager.handleDelta(
        {
          added: [],
          removed: [],
          updated: [],
          pending: false,
        },
        transform,
      );

      expect(result.all).toEqual([]);
    });
  });
});
