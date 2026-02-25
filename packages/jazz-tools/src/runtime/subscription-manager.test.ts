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

function makeRow(id: string, name: string, count: number): WasmRow {
  return {
    id,
    values: [
      { type: "Text", value: name },
      { type: "Integer", value: count },
    ],
  };
}

function transform(row: WasmRow): TestItem {
  return {
    id: row.id,
    name: (row.values[0] as { type: "Text"; value: string }).value,
    count: (row.values[1] as { type: "Integer"; value: number }).value,
  };
}

function makeDelta(partial: Partial<RowDelta>): RowDelta {
  return {
    added: [],
    removed: [],
    updated: [],
    pending: false,
    ...partial,
  };
}

describe("SubscriptionManager", () => {
  it("tracks additions", () => {
    const manager = new SubscriptionManager<TestItem>();

    const result = manager.handleDelta(
      makeDelta({
        added: [
          { id: "1", index: 0, row: makeRow("1", "item1", 10) },
          { id: "2", index: 1, row: makeRow("2", "item2", 20) },
        ],
      }),
      transform,
    );

    expect(result.added).toHaveLength(2);
    expect(result.updated).toHaveLength(0);
    expect(result.removed).toHaveLength(0);
    expect(result.all.map((item) => item.id)).toEqual(["1", "2"]);
    expect(manager.size).toBe(2);
  });

  it("tracks content updates", () => {
    const manager = new SubscriptionManager<TestItem>();

    manager.handleDelta(
      makeDelta({
        added: [{ id: "1", index: 0, row: makeRow("1", "item1", 10) }],
      }),
      transform,
    );

    const result = manager.handleDelta(
      makeDelta({
        updated: [{ id: "1", oldIndex: 0, newIndex: 0, row: makeRow("1", "item1", 15) }],
      }),
      transform,
    );

    expect(result.updated).toHaveLength(1);
    expect(result.updated[0]).toEqual({ id: "1", name: "item1", count: 15 });
    expect(result.all[0].count).toBe(15);
  });

  it("handles move-only updates without row payload", () => {
    const manager = new SubscriptionManager<TestItem>();

    manager.handleDelta(
      makeDelta({
        added: [
          { id: "a", index: 0, row: makeRow("a", "A", 1) },
          { id: "b", index: 1, row: makeRow("b", "B", 2) },
          { id: "c", index: 2, row: makeRow("c", "C", 3) },
        ],
      }),
      transform,
    );

    const result = manager.handleDelta(
      makeDelta({
        updated: [{ id: "c", oldIndex: 2, newIndex: 0 }],
      }),
      transform,
    );

    expect(result.updated).toHaveLength(1);
    expect(result.updated[0].id).toBe("c");
    expect(result.all.map((item) => item.id)).toEqual(["c", "a", "b"]);
  });

  it("tracks removals and shifts", () => {
    const manager = new SubscriptionManager<TestItem>();

    manager.handleDelta(
      makeDelta({
        added: [
          { id: "1", index: 0, row: makeRow("1", "item1", 10) },
          { id: "2", index: 1, row: makeRow("2", "item2", 20) },
          { id: "3", index: 2, row: makeRow("3", "item3", 30) },
        ],
      }),
      transform,
    );

    const result = manager.handleDelta(
      makeDelta({
        removed: [{ id: "2", index: 1 }],
      }),
      transform,
    );

    expect(result.removed).toHaveLength(1);
    expect(result.removed[0].id).toBe("2");
    expect(result.all.map((item) => item.id)).toEqual(["1", "3"]);
  });

  it("handles mixed remove + update + add in one delta", () => {
    const manager = new SubscriptionManager<TestItem>();

    manager.handleDelta(
      makeDelta({
        added: [
          { id: "A", index: 0, row: makeRow("A", "A", 1) },
          { id: "B", index: 1, row: makeRow("B", "B", 2) },
          { id: "C", index: 2, row: makeRow("C", "C", 3) },
          { id: "D", index: 3, row: makeRow("D", "D", 4) },
        ],
      }),
      transform,
    );

    const result = manager.handleDelta(
      makeDelta({
        removed: [{ id: "B", index: 1 }],
        updated: [{ id: "D", oldIndex: 3, newIndex: 1, row: makeRow("D", "D", 44) }],
        added: [{ id: "E", index: 3, row: makeRow("E", "E", 5) }],
      }),
      transform,
    );

    expect(result.removed.map((item) => item.id)).toEqual(["B"]);
    expect(result.updated.map((item) => item.id)).toEqual(["D"]);
    expect(result.added.map((item) => item.id)).toEqual(["E"]);
    expect(result.all.map((item) => item.id)).toEqual(["A", "D", "C", "E"]);
    expect(result.all.find((item) => item.id === "D")?.count).toBe(44);
  });

  it("clears state", () => {
    const manager = new SubscriptionManager<TestItem>();

    manager.handleDelta(
      makeDelta({
        added: [{ id: "1", index: 0, row: makeRow("1", "item1", 10) }],
      }),
      transform,
    );

    expect(manager.size).toBe(1);
    manager.clear();
    expect(manager.size).toBe(0);

    const result = manager.handleDelta(
      makeDelta({
        added: [{ id: "2", index: 0, row: makeRow("2", "item2", 20) }],
      }),
      transform,
    );

    expect(result.all.map((item) => item.id)).toEqual(["2"]);
  });
});
