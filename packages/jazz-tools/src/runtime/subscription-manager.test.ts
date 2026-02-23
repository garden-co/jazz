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
  describe("handleDelta with additions", () => {
    it("tracks added items", () => {
      const manager = new SubscriptionManager<TestItem>();

      const delta: RowDelta = {
        added: [makeRow("1", "item1", 10), makeRow("2", "item2", 20)],
        removed: [],
        updated: [],
        pending: false,
      };

      const result = manager.handleDelta(delta, transform);

      expect(result.added).toHaveLength(2);
      expect(result.added[0]).toEqual({ id: "1", name: "item1", count: 10 });
      expect(result.added[1]).toEqual({ id: "2", name: "item2", count: 20 });

      expect(result.updated).toHaveLength(0);
      expect(result.removed).toHaveLength(0);

      expect(result.all).toHaveLength(2);
      expect(manager.size).toBe(2);
    });

    it("accumulates items across multiple deltas", () => {
      const manager = new SubscriptionManager<TestItem>();

      // First delta
      manager.handleDelta(
        {
          added: [makeRow("1", "item1", 10)],
          removed: [],
          updated: [],
          pending: false,
        },
        transform,
      );

      // Second delta
      const result = manager.handleDelta(
        {
          added: [makeRow("2", "item2", 20)],
          removed: [],
          updated: [],
          pending: false,
        },
        transform,
      );

      expect(result.added).toHaveLength(1); // Only new item
      expect(result.all).toHaveLength(2); // Full state
      expect(manager.size).toBe(2);
    });
  });

  describe("handleDelta with updates", () => {
    it("tracks updated items", () => {
      const manager = new SubscriptionManager<TestItem>();

      // First add an item
      manager.handleDelta(
        {
          added: [makeRow("1", "item1", 10)],
          removed: [],
          updated: [],
          pending: false,
        },
        transform,
      );

      // Then update it
      const oldRow = makeRow("1", "item1", 10);
      const newRow = makeRow("1", "item1", 15);
      const result = manager.handleDelta(
        {
          added: [],
          removed: [],
          updated: [[oldRow, newRow]],
          pending: false,
        },
        transform,
      );

      expect(result.updated).toHaveLength(1);
      expect(result.updated[0]).toEqual({ id: "1", name: "item1", count: 15 });

      expect(result.added).toHaveLength(0);
      expect(result.removed).toHaveLength(0);

      expect(result.all).toHaveLength(1);
      expect(result.all[0].count).toBe(15);
    });

    it("updates items with new values", () => {
      const manager = new SubscriptionManager<TestItem>();

      // Add initial item
      manager.handleDelta(
        {
          added: [makeRow("1", "original", 10)],
          removed: [],
          updated: [],
          pending: false,
        },
        transform,
      );

      // Update with new name
      const result = manager.handleDelta(
        {
          added: [],
          removed: [],
          updated: [[makeRow("1", "original", 10), makeRow("1", "updated", 100)]],
          pending: false,
        },
        transform,
      );

      expect(result.all[0]).toEqual({ id: "1", name: "updated", count: 100 });
    });
  });

  describe("handleDelta with removals", () => {
    it("tracks removed items", () => {
      const manager = new SubscriptionManager<TestItem>();

      // Add items first
      manager.handleDelta(
        {
          added: [makeRow("1", "item1", 10), makeRow("2", "item2", 20)],
          removed: [],
          updated: [],
          pending: false,
        },
        transform,
      );

      // Remove one item
      const result = manager.handleDelta(
        {
          added: [],
          removed: [makeRow("1", "item1", 10)],
          updated: [],
          pending: false,
        },
        transform,
      );

      expect(result.removed).toHaveLength(1);
      expect(result.removed[0]).toEqual({ id: "1", name: "item1", count: 10 });

      expect(result.added).toHaveLength(0);
      expect(result.updated).toHaveLength(0);

      expect(result.all).toHaveLength(1);
      expect(result.all[0].id).toBe("2");
      expect(manager.size).toBe(1);
    });

    it("handles removing non-existent items gracefully", () => {
      const manager = new SubscriptionManager<TestItem>();

      // Try to remove an item that was never added
      const result = manager.handleDelta(
        {
          added: [],
          removed: [makeRow("nonexistent", "ghost", 0)],
          updated: [],
          pending: false,
        },
        transform,
      );

      expect(result.removed).toHaveLength(0);
      expect(result.all).toHaveLength(0);
    });
  });

  describe("handleDelta with mixed operations", () => {
    it("handles add, update, and remove in same delta", () => {
      const manager = new SubscriptionManager<TestItem>();

      // Add initial items
      manager.handleDelta(
        {
          added: [makeRow("1", "item1", 10), makeRow("2", "item2", 20)],
          removed: [],
          updated: [],
          pending: false,
        },
        transform,
      );

      // Mixed delta: add "3", update "2", remove "1"
      const result = manager.handleDelta(
        {
          added: [makeRow("3", "item3", 30)],
          removed: [makeRow("1", "item1", 10)],
          updated: [[makeRow("2", "item2", 20), makeRow("2", "item2", 25)]],
          pending: false,
        },
        transform,
      );

      expect(result.added).toHaveLength(1);
      expect(result.added[0].id).toBe("3");

      expect(result.updated).toHaveLength(1);
      expect(result.updated[0].id).toBe("2");
      expect(result.updated[0].count).toBe(25);

      expect(result.removed).toHaveLength(1);
      expect(result.removed[0].id).toBe("1");

      expect(result.all).toHaveLength(2);
      const ids = result.all.map((item) => item.id).sort();
      expect(ids).toEqual(["2", "3"]);
    });
  });

  describe("clear", () => {
    it("clears all tracked state", () => {
      const manager = new SubscriptionManager<TestItem>();

      // Add items
      manager.handleDelta(
        {
          added: [makeRow("1", "item1", 10), makeRow("2", "item2", 20)],
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
          added: [makeRow("3", "item3", 30)],
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
          added: [makeRow("1", "item1", 10)],
          removed: [],
          updated: [],
          pending: false,
        },
        transform,
      );

      expect(result1.all).toEqual([{ id: "1", name: "item1", count: 10 }]);

      const result2 = manager.handleDelta(
        {
          added: [makeRow("2", "item2", 20)],
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
