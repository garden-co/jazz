/**
 * Tests for subscription-manager module.
 */

import { describe, it, expect } from "vitest";
import { SubscriptionManager } from "./subscription-manager.js";
import type { ColumnDescriptor, NativeRowDelta, WasmRow, RowDelta } from "../drivers/types.js";

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

const nativeColumns: ColumnDescriptor[] = [
  { name: "name", column_type: { type: "Text" }, nullable: false },
  { name: "count", column_type: { type: "Integer" }, nullable: false },
];

function uuidBytes(id: string): Uint8Array {
  return Uint8Array.from(
    id
      .replaceAll("-", "")
      .match(/../g)!
      .map((hex) => Number.parseInt(hex, 16)),
  );
}

function pushU32(target: number[], value: number): void {
  target.push(value & 0xff, (value >>> 8) & 0xff, (value >>> 16) & 0xff, (value >>> 24) & 0xff);
}

function nativeRowData(name: string, count: number): Uint8Array {
  const text = new TextEncoder().encode(name);
  const data = new Uint8Array(4 + text.byteLength);
  new DataView(data.buffer).setInt32(0, count, true);
  data.set(text, 4);
  return data;
}

function nativeAddedRecord(id: string, index: number, name: string, count: number): Uint8Array {
  const data = nativeRowData(name, count);
  const bytes: number[] = [...uuidBytes(id)];
  pushU32(bytes, index);
  pushU32(bytes, data.byteLength);
  bytes.push(...data);
  return Uint8Array.from(bytes);
}

describe("SubscriptionManager", () => {
  it("transforms wire deltas into typed deltas", () => {
    const manager = new SubscriptionManager<TestItem>();
    const input = makeDelta([{ kind: 0, id: "1", index: 0, row: makeRow("1", "item1", 10) }]);

    const result = manager.handleDelta(input, transform);

    expect(result.delta).toEqual([
      { kind: 0, id: "1", index: 0, item: { id: "1", name: "item1", count: 10 } },
    ]);
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

  it("decodes native subscription additions", () => {
    const manager = new SubscriptionManager<TestItem>();
    const id = "00000000-0000-4000-8000-000000000001";
    const delta: NativeRowDelta = {
      __jazzNativeRowDelta: true,
      added: nativeAddedRecord(id, 0, "native", 42),
      removed: new Uint8Array(),
      updated: new Uint8Array(),
      addedCount: 1,
      removedCount: 0,
      updatedCount: 0,
    };

    const result = manager.handleDelta(delta, transform, nativeColumns);

    expect(result.all).toEqual([{ id, name: "native", count: 42 }]);
    expect(result.delta).toEqual([
      {
        kind: 0,
        id,
        index: 0,
        item: { id, name: "native", count: 42 },
      },
    ]);
  });

  it("decodes native subscription additions directly to typed rows", () => {
    const manager = new SubscriptionManager<TestItem>();
    const id = "00000000-0000-4000-8000-000000000002";
    const delta: NativeRowDelta = {
      __jazzNativeRowDelta: true,
      added: nativeAddedRecord(id, 0, "direct", 7),
      removed: new Uint8Array(),
      updated: new Uint8Array(),
      addedCount: 1,
      removedCount: 0,
      updatedCount: 0,
    };

    const result = manager.handleDelta(delta, transform, nativeColumns, (row) => ({
      id: row.id as string,
      name: row.name as string,
      count: row.count as number,
    }));

    expect(result.all).toEqual([{ id, name: "direct", count: 7 }]);
    expect(result.delta).toEqual([
      {
        kind: 0,
        id,
        index: 0,
        item: { id, name: "direct", count: 7 },
      },
    ]);
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

    expect(result.delta[0]).toEqual({
      kind: 2,
      id: "1",
      index: 0,
      item: { id: "1", name: "item1", count: 15 },
    });
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
