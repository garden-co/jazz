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

function makeDelta(changes: RowDelta = []): RowDelta {
  return changes;
}

describe("SubscriptionManager", () => {
  it("passes delta by reference (zero-copy)", () => {
    const manager = new SubscriptionManager<TestItem>();
    const input = makeDelta([{ kind: 0, id: "1", index: 0, row: makeRow("1", "item1", 10) }]);

    const result = manager.handleDelta(input, transform);

    expect(result.delta).toBe(input);
    expect(result.all.map((item) => item.id)).toEqual(["1"]);
  });

  it("tracks additions", () => {
    const manager = new SubscriptionManager<TestItem>();

    const result = manager.handleDelta(
      makeDelta([
        { kind: 0, id: "1", index: 0, row: makeRow("1", "item1", 10) },
        { kind: 0, id: "2", index: 1, row: makeRow("2", "item2", 20) },
      ]),
      transform,
    );

    expect(result.delta).toHaveLength(2);
    expect(result.all.map((item) => item.id)).toEqual(["1", "2"]);
    expect(manager.size).toBe(2);
  });

  it("tracks content updates", () => {
    const manager = new SubscriptionManager<TestItem>();

    manager.handleDelta(
      makeDelta([{ kind: 0, id: "1", index: 0, row: makeRow("1", "item1", 10) }]),
      transform,
    );

    const result = manager.handleDelta(
      makeDelta([{ kind: 2, id: "1", index: 0, row: makeRow("1", "item1", 15) }]),
      transform,
    );

    expect(result.delta[0]).toEqual({ kind: 2, id: "1", index: 0, row: makeRow("1", "item1", 15) });
    expect(result.all[0]!.count).toBe(15);
  });

  it("handles move-only updates without row payload", () => {
    const manager = new SubscriptionManager<TestItem>();

    manager.handleDelta(
      makeDelta([
        { kind: 0, id: "a", index: 0, row: makeRow("a", "A", 1) },
        { kind: 0, id: "b", index: 1, row: makeRow("b", "B", 2) },
        { kind: 0, id: "c", index: 2, row: makeRow("c", "C", 3) },
      ]),
      transform,
    );

    const result = manager.handleDelta(makeDelta([{ kind: 2, id: "c", index: 0 }]), transform);

    expect(result.delta).toEqual([{ kind: 2, id: "c", index: 0 }]);
    expect(result.all.map((item) => item.id)).toEqual(["c", "a", "b"]);
  });

  it("tracks removals and shifts", () => {
    const manager = new SubscriptionManager<TestItem>();

    manager.handleDelta(
      makeDelta([
        { kind: 0, id: "1", index: 0, row: makeRow("1", "item1", 10) },
        { kind: 0, id: "2", index: 1, row: makeRow("2", "item2", 20) },
        { kind: 0, id: "3", index: 2, row: makeRow("3", "item3", 30) },
      ]),
      transform,
    );

    const result = manager.handleDelta(makeDelta([{ kind: 1, id: "2", index: 1 }]), transform);

    expect(result.delta).toEqual([{ kind: 1, id: "2", index: 1 }]);
    expect(result.all.map((item) => item.id)).toEqual(["1", "3"]);
  });

  it("handles mixed remove + update + add in one delta", () => {
    const manager = new SubscriptionManager<TestItem>();

    manager.handleDelta(
      makeDelta([
        { kind: 0, id: "A", index: 0, row: makeRow("A", "A", 1) },
        { kind: 0, id: "B", index: 1, row: makeRow("B", "B", 2) },
        { kind: 0, id: "C", index: 2, row: makeRow("C", "C", 3) },
        { kind: 0, id: "D", index: 3, row: makeRow("D", "D", 4) },
      ]),
      transform,
    );

    const result = manager.handleDelta(
      makeDelta([
        { kind: 1, id: "B", index: 1 },
        { kind: 2, id: "D", index: 1, row: makeRow("D", "D", 44) },
        { kind: 0, id: "E", index: 3, row: makeRow("E", "E", 5) },
      ]),
      transform,
    );

    expect(result.delta.map((change) => change.kind)).toEqual([1, 2, 0]);
    expect(result.all.map((item) => item.id)).toEqual(["A", "D", "C", "E"]);
  });

  it("applies index positions correctly for mixed bulk updates", () => {
    const manager = new SubscriptionManager<TestItem>();

    manager.handleDelta(
      makeDelta([
        { kind: 0, id: "A", index: 0, row: makeRow("A", "A", 1) },
        { kind: 0, id: "B", index: 1, row: makeRow("B", "B", 2) },
        { kind: 0, id: "C", index: 2, row: makeRow("C", "C", 3) },
        { kind: 0, id: "D", index: 3, row: makeRow("D", "D", 4) },
      ]),
      transform,
    );

    const result = manager.handleDelta(
      makeDelta([
        // Bulk mixed change set:
        // - remove B
        // - move D to index 1 with payload update
        // - move C to index 0 (no payload)
        // - add E at tail
        { kind: 1, id: "B", index: 1 },
        { kind: 2, id: "D", index: 1, row: makeRow("D", "D*", 40) },
        { kind: 2, id: "C", index: 0 },
        { kind: 0, id: "E", index: 3, row: makeRow("E", "E", 5) },
      ]),
      transform,
    );

    expect(result.all.map((item) => item.id)).toEqual(["C", "D", "A", "E"]);
    expect(result.all.find((item) => item.id === "D")?.name).toBe("D*");
  });

  it("clears state", () => {
    const manager = new SubscriptionManager<TestItem>();

    manager.handleDelta(
      makeDelta([{ kind: 0, id: "1", index: 0, row: makeRow("1", "item1", 10) }]),
      transform,
    );

    expect(manager.size).toBe(1);
    manager.clear();
    expect(manager.size).toBe(0);

    const result = manager.handleDelta(
      makeDelta([{ kind: 0, id: "2", index: 0, row: makeRow("2", "item2", 20) }]),
      transform,
    );

    expect(result.all.map((item) => item.id)).toEqual(["2"]);
  });
});
