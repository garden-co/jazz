/**
 * Tests for subscription-manager module.
 */

import { describe, expect, it } from "vitest";
import type {
  RuntimeSubscriptionAddedRow,
  RuntimeSubscriptionDelta,
  RuntimeSubscriptionRemovedRow,
  Value,
  WasmRow,
} from "../drivers/types.js";
import { applySubscriptionDelta, SubscriptionManager } from "./subscription-manager.js";
import type { SubscriptionDelta } from "./subscription-manager.js";

interface TestItem {
  id: string;
  name: string;
  count: number;
}

type DecodedRowDelta = Array<
  | { kind: 0; id: string; index: number; row: WasmRow }
  | { kind: 1; id: string; index: number }
  | { kind: 2; id: string; index: number; row?: WasmRow | null }
>;

function handleDecodedDelta<T extends { id: string }>(
  manager: SubscriptionManager<T>,
  delta: DecodedRowDelta,
  transformRow: (row: WasmRow) => T,
): SubscriptionDelta<T> {
  return (
    manager as unknown as {
      handleDecodedDelta(
        delta: DecodedRowDelta,
        transform: (row: WasmRow) => T,
      ): SubscriptionDelta<T>;
    }
  ).handleDecodedDelta(delta, transformRow);
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

function reduceDeltas(...deltas: SubscriptionDelta<TestItem>[]): TestItem[] {
  const current: TestItem[] = [];
  for (const delta of deltas) applySubscriptionDelta(current, delta);
  return current;
}

function uuidBytes(id: string): Uint8Array {
  return Uint8Array.from(
    id
      .replaceAll("-", "")
      .match(/../g)!
      .map((hex) => Number.parseInt(hex, 16)),
  );
}

function pushU32Be(target: number[], value: number): void {
  target.push((value >>> 24) & 0xff, (value >>> 16) & 0xff, (value >>> 8) & 0xff, value & 0xff);
}

function typedResultKey(
  root: Uint8Array,
  joined: readonly Uint8Array[],
  discriminators: ReadonlyArray<readonly [number, string]>,
): Uint8Array {
  const bytes = [2, ...root];
  pushU32Be(bytes, joined.length);
  for (const value of joined) bytes.push(...value);
  pushU32Be(bytes, discriminators.length);
  for (const [position, label] of discriminators) {
    const encoded = new TextEncoder().encode(label);
    pushU32Be(bytes, position);
    pushU32Be(bytes, encoded.byteLength);
    bytes.push(...encoded);
  }
  return Uint8Array.from(bytes);
}

function occurrenceKey(id: string): Uint8Array {
  return Uint8Array.from([1, ...uuidBytes(id)]);
}

function runtimeAddedRecord(
  id: string,
  index: number,
  name: string,
  count: number,
  key = occurrenceKey(id),
): RuntimeSubscriptionAddedRow {
  return { sourceId: id, occurrenceKey: key, index, row: makeRow(id, name, count) };
}

function runtimeRemovedRecord(id: string, index: number): RuntimeSubscriptionRemovedRow {
  return { sourceId: id, occurrenceKey: occurrenceKey(id), index };
}

function includedRootRow(id: string, title: string): WasmRow {
  const values: Value[] = [
    { type: "Text", value: title },
    { type: "Array", value: [] },
  ];
  const row = { id, values };
  Object.defineProperty(row, "valuesByColumn", {
    value: new Map([
      ["title", values[0]!],
      ["children", values[1]!],
    ]),
  });
  return row;
}

function runtimeAddedRoot(id: string, index: number, title: string): RuntimeSubscriptionAddedRow {
  return { sourceId: id, occurrenceKey: occurrenceKey(id), index, row: includedRootRow(id, title) };
}

function terminalTextChild(id: string, name: string): WasmRow {
  const values: Value[] = [{ type: "Text", value: name }];
  const row: WasmRow = { id, values };
  Object.defineProperty(row, "valuesByColumn", {
    value: new Map([["name", values[0]!]]),
  });
  return row;
}

function emptyRuntimeDelta(
  overrides: Partial<RuntimeSubscriptionDelta> = {},
): RuntimeSubscriptionDelta {
  return {
    added: [],
    removed: [],
    updated: [],
    ...overrides,
  };
}

type IncludedRoot = {
  id: string;
  title: string;
  children: Array<{ id: string; name: string }>;
};

function transformIncluded(row: WasmRow): IncludedRoot {
  const byName = (row as WasmRow & { valuesByColumn: Map<string, Value> }).valuesByColumn;
  const children = byName.get("children");
  return {
    id: row.id,
    title: (byName.get("title") as { type: "Text"; value: string }).value,
    children:
      children?.type === "Array"
        ? children.value.map((value) => {
            if (value.type !== "Row") throw new Error("expected child row");
            return {
              id: value.value.id!,
              name: (value.value.values[0] as { type: "Text"; value: string }).value,
            };
          })
        : [],
  };
}

describe("SubscriptionManager", () => {
  it("transforms decoded root deltas into typed deltas", () => {
    const manager = new SubscriptionManager<TestItem>();
    const input: DecodedRowDelta = [{ kind: 0, id: "1", index: 0, row: makeRow("1", "item1", 10) }];

    const result = handleDecodedDelta(manager, input, transform);

    expect(result.delta).toEqual([
      { kind: 0, id: "1", index: 0, item: { id: "1", name: "item1", count: 10 } },
    ]);
    expect(reduceDeltas(result).map((item) => item.id)).toEqual(["1"]);
  });

  it("tracks additions", () => {
    const manager = new SubscriptionManager<TestItem>();
    const result = handleDecodedDelta(
      manager,
      [
        { kind: 0, id: "1", index: 0, row: makeRow("1", "item1", 10) },
        { kind: 0, id: "2", index: 1, row: makeRow("2", "item2", 20) },
      ],
      transform,
    );

    expect(result.delta).toHaveLength(2);
    expect(reduceDeltas(result).map((item) => item.id)).toEqual(["1", "2"]);
    expect(manager.size).toBe(2);
  });

  it("applies an authoritative move even when the public item is identical", () => {
    type EdgeItem = { id: string; count: bigint; bytes: Uint8Array; nan: number };
    const manager = new SubscriptionManager<EdgeItem>();
    const transformEdge = (row: WasmRow): EdgeItem => ({
      id: row.id,
      count: 1n,
      bytes: Uint8Array.of(7, 8),
      nan: Number.NaN,
    });
    handleDecodedDelta(
      manager,
      [
        { kind: 0, id: "A", index: 0, row: makeRow("A", "A", 1) },
        { kind: 0, id: "B", index: 1, row: makeRow("B", "B", 2) },
      ],
      transformEdge,
    );

    const result = handleDecodedDelta(
      manager,
      [{ kind: 2, id: "B", index: 0, row: makeRow("B", "B", 2) }],
      transformEdge,
    );

    expect(result.delta).toMatchObject([{ kind: 2, id: "B", index: 0 }]);
    expect(result.all?.map((item) => item.id)).toEqual(["B", "A"]);
  });

  it("reports an identical update at its final index after same-frame inserts", () => {
    const manager = new SubscriptionManager<TestItem>();
    handleDecodedDelta(
      manager,
      [
        { kind: 0, id: "A", index: 0, row: makeRow("A", "A", 1) },
        { kind: 0, id: "B", index: 1, row: makeRow("B", "B", 2) },
      ],
      transform,
    );

    const result = handleDecodedDelta(
      manager,
      [
        { kind: 0, id: "C", index: 0, row: makeRow("C", "C", 3) },
        { kind: 2, id: "B", index: 2, row: makeRow("B", "B", 2) },
      ],
      transform,
    );

    expect(result.delta).toMatchObject([
      { kind: 0, id: "C", index: 0 },
      { kind: 2, id: "B", index: 2 },
    ]);
    expect(result.all?.map((item) => item.id)).toEqual(["C", "A", "B"]);
  });

  it("transforms runtime subscription additions", () => {
    const manager = new SubscriptionManager<TestItem>();
    const id = "00000000-0000-4000-8000-000000000001";
    const result = manager.handleDelta(
      emptyRuntimeDelta({ added: [runtimeAddedRecord(id, 0, "native", -42)] }),
      transform,
    );

    expect(reduceDeltas(result)).toEqual([{ id, name: "native", count: -42 }]);
    expect(result.delta).toEqual([
      { kind: 0, id, index: 0, item: { id, name: "native", count: -42 } },
    ]);
  });

  it("rejects root terminal operations without mutating subscription state", () => {
    const manager = new SubscriptionManager<TestItem>();
    const id = "00000000-0000-4000-8000-000000000001";
    const key = [10, ...uuidBytes(id)];
    manager.handleDelta(
      emptyRuntimeDelta({ added: [runtimeAddedRecord(id, 0, "before", 1)] }),
      transform,
    );

    expect(() =>
      manager.handleDelta(
        emptyRuntimeDelta({
          terminalOperations: [
            { root_key: key, path: [], edit: { Update: { key, row: makeRow(id, "after", 2) } } },
          ],
        }),
        transform,
      ),
    ).toThrow(/native producer emitted a root terminal operation/);
    expect(manager.all()).toEqual([{ id, name: "before", count: 1 }]);
  });

  it("rejects noncanonical typed occurrence sidecars and ordered-key collisions", () => {
    const id = "00000000-0000-4000-8000-000000000001";
    const joinedId = "00000000-0000-4000-8000-000000000002";
    const secondJoinedId = "00000000-0000-4000-8000-000000000003";
    const root = uuidBytes(id);
    const joined = uuidBytes(joinedId);
    const secondJoined = uuidBytes(secondJoinedId);
    const rejectSidecar = (sidecar: Uint8Array) => {
      const manager = new SubscriptionManager<TestItem>();
      expect(() =>
        manager.handleDelta(
          emptyRuntimeDelta({ added: [runtimeAddedRecord(id, 0, "typed", 1, sidecar)] }),
          transform,
        ),
      ).toThrow(/malformed or noncanonical typed terminal occurrence key/);
      expect(manager.all()).toEqual([]);
    };

    rejectSidecar(typedResultKey(root, [joined], []));
    rejectSidecar(
      typedResultKey(
        root,
        [joined, secondJoined],
        [
          [1, "second"],
          [0, "first"],
        ],
      ),
    );
    rejectSidecar(
      typedResultKey(
        root,
        [joined, secondJoined],
        [
          [0, "first"],
          [0, "duplicate"],
        ],
      ),
    );

    const manager = new SubscriptionManager<TestItem>();
    const registry = manager as unknown as {
      registerTerminalOccurrenceAddress(ordered: Uint8Array, occurrence: string): void;
    };
    const ordered = Uint8Array.from([10, ...root, 6, 0x61, 0, 0, 10, ...joined]);
    registry.registerTerminalOccurrenceAddress(ordered, "result:02first");
    expect(() => registry.registerTerminalOccurrenceAddress(ordered, "result:02second")).toThrow(
      /conflicting typed terminal occurrence keys share an ordered root key/,
    );
  });

  it("applies descendant terminal edits to a retained root", () => {
    const manager = new SubscriptionManager<IncludedRoot>();
    const rootId = "00000000-0000-4000-8000-000000000001";
    const firstId = "00000000-0000-4000-8000-000000000002";
    const secondId = "00000000-0000-4000-8000-000000000003";
    const rootKey = [10, ...uuidBytes(rootId)];
    const firstKey = [10, ...uuidBytes(firstId)];
    const secondKey = [10, ...uuidBytes(secondId)];
    manager.handleDelta(
      emptyRuntimeDelta({ added: [runtimeAddedRoot(rootId, 0, "root")] }),
      transformIncluded,
    );

    const inserted = manager.handleDelta(
      emptyRuntimeDelta({
        terminalOperations: [
          {
            root_key: rootKey,
            path: [{ Collection: 1 }],
            edit: {
              Insert: { index: 0, key: firstKey, row: terminalTextChild(firstId, "one") },
            },
          },
          {
            root_key: rootKey,
            path: [{ Collection: 1 }],
            edit: {
              Insert: { index: 1, key: secondKey, row: terminalTextChild(secondId, "two") },
            },
          },
        ],
      }),
      transformIncluded,
    );
    expect(inserted.all?.[0]?.children.map((child) => child.name)).toEqual(["one", "two"]);

    const edited = manager.handleDelta(
      emptyRuntimeDelta({
        terminalOperations: [
          {
            root_key: rootKey,
            path: [{ Collection: 1 }],
            edit: { Move: { index: 0, key: secondKey } },
          },
          {
            root_key: rootKey,
            path: [{ Collection: 1 }],
            edit: {
              Update: { key: secondKey, row: terminalTextChild(secondId, "updated") },
            },
          },
          {
            root_key: rootKey,
            path: [{ Collection: 1 }],
            edit: { Remove: { key: firstKey } },
          },
        ],
      }),
      transformIncluded,
    );
    expect(edited.all).toEqual([
      { id: rootId, title: "root", children: [{ id: secondId, name: "updated" }] },
    ]);
  });

  it("replays a descendant edit that arrives before its root", () => {
    const manager = new SubscriptionManager<IncludedRoot>();
    const rootId = "00000000-0000-4000-8000-000000000001";
    const childId = "00000000-0000-4000-8000-000000000002";
    const rootKey = [10, ...uuidBytes(rootId)];
    const childKey = [10, ...uuidBytes(childId)];

    const deferred = manager.handleDelta(
      emptyRuntimeDelta({
        terminalOperations: [
          {
            root_key: rootKey,
            path: [{ Collection: 1 }],
            edit: {
              Insert: { index: 0, key: childKey, row: terminalTextChild(childId, "child") },
            },
          },
        ],
      }),
      transformIncluded,
    );
    expect(deferred.all).toEqual([]);

    const result = manager.handleDelta(
      emptyRuntimeDelta({ added: [runtimeAddedRoot(rootId, 0, "root")] }),
      transformIncluded,
    );
    expect(result.all).toEqual([
      { id: rootId, title: "root", children: [{ id: childId, name: "child" }] },
    ]);
  });

  it("discards descendant teardown after Rust removes its exact root", () => {
    const manager = new SubscriptionManager<IncludedRoot>();
    const rootId = "00000000-0000-4000-8000-000000000001";
    const childId = "00000000-0000-4000-8000-000000000002";
    const rootKey = [10, ...uuidBytes(rootId)];
    const childKey = [10, ...uuidBytes(childId)];
    manager.handleDelta(
      emptyRuntimeDelta({ added: [runtimeAddedRoot(rootId, 0, "original")] }),
      transformIncluded,
    );

    const removed = manager.handleDelta(
      emptyRuntimeDelta({
        removed: [runtimeRemovedRecord(rootId, 0)],
        terminalOperations: [
          {
            root_key: rootKey,
            path: [{ Collection: 1 }],
            edit: { Remove: { key: childKey } },
          },
        ],
      }),
      transformIncluded,
    );
    expect(removed.all).toEqual([]);

    const reopened = manager.handleDelta(
      emptyRuntimeDelta({ added: [runtimeAddedRoot(rootId, 0, "reopened")] }),
      transformIncluded,
    );
    expect(reopened.all).toEqual([{ id: rootId, title: "reopened", children: [] }]);

    expect(() =>
      manager.handleDelta(
        emptyRuntimeDelta({
          removed: [runtimeRemovedRecord(rootId, 0)],
          terminalOperations: [
            {
              root_key: rootKey,
              path: [{ Collection: 1 }],
              edit: {
                Insert: {
                  index: 0,
                  key: childKey,
                  row: terminalTextChild(childId, "rejected"),
                },
              },
            },
          ],
        }),
        transformIncluded,
      ),
    ).toThrow(/terminal child edit addressed a root removed in the same frame/);
    expect(manager.all()).toEqual([{ id: rootId, title: "reopened", children: [] }]);
  });

  it("clears tracked state before applying reset frames", () => {
    const manager = new SubscriptionManager<TestItem>();
    const first = "00000000-0000-4000-8000-000000000001";
    const second = "00000000-0000-4000-8000-000000000002";
    manager.handleDelta(
      emptyRuntimeDelta({ added: [runtimeAddedRecord(first, 0, "first", 1)] }),
      transform,
    );

    const result = manager.handleDelta(
      emptyRuntimeDelta({
        reset: true,
        added: [runtimeAddedRecord(second, 0, "second", 2)],
      }),
      transform,
    );

    expect(result.reset).toBe(true);
    expect(result.all).toEqual([{ id: second, name: "second", count: 2 }]);
    expect(manager.size).toBe(1);
  });

  it("tracks content updates", () => {
    const manager = new SubscriptionManager<TestItem>();
    const initial = handleDecodedDelta(
      manager,
      [{ kind: 0, id: "1", index: 0, row: makeRow("1", "item1", 10) }],
      transform,
    );
    const result = handleDecodedDelta(
      manager,
      [{ kind: 2, id: "1", index: 0, row: makeRow("1", "item1", 15) }],
      transform,
    );

    expect(result.delta[0]).toEqual({
      kind: 2,
      id: "1",
      index: 0,
      item: { id: "1", name: "item1", count: 15 },
    });
    expect(reduceDeltas(initial, result)[0]!.count).toBe(15);
  });

  it("handles move-only updates without row payload", () => {
    const manager = new SubscriptionManager<TestItem>();
    const initial = handleDecodedDelta(
      manager,
      [
        { kind: 0, id: "a", index: 0, row: makeRow("a", "A", 1) },
        { kind: 0, id: "b", index: 1, row: makeRow("b", "B", 2) },
        { kind: 0, id: "c", index: 2, row: makeRow("c", "C", 3) },
      ],
      transform,
    );
    const result = handleDecodedDelta(manager, [{ kind: 2, id: "c", index: 0 }], transform);

    expect(result.delta).toEqual([{ kind: 2, id: "c", index: 0 }]);
    expect(reduceDeltas(initial, result).map((item) => item.id)).toEqual(["c", "a", "b"]);
  });

  it("tracks removals and shifts", () => {
    const manager = new SubscriptionManager<TestItem>();
    const initial = handleDecodedDelta(
      manager,
      [
        { kind: 0, id: "1", index: 0, row: makeRow("1", "item1", 10) },
        { kind: 0, id: "2", index: 1, row: makeRow("2", "item2", 20) },
        { kind: 0, id: "3", index: 2, row: makeRow("3", "item3", 30) },
      ],
      transform,
    );
    const result = handleDecodedDelta(manager, [{ kind: 1, id: "2", index: 1 }], transform);

    expect(result.delta).toEqual([{ kind: 1, id: "2", index: 1 }]);
    expect(reduceDeltas(initial, result).map((item) => item.id)).toEqual(["1", "3"]);
  });

  it("handles mixed indexed changes in one delta", () => {
    const manager = new SubscriptionManager<TestItem>();
    const initial = handleDecodedDelta(
      manager,
      [
        { kind: 0, id: "A", index: 0, row: makeRow("A", "A", 1) },
        { kind: 0, id: "B", index: 1, row: makeRow("B", "B", 2) },
        { kind: 0, id: "C", index: 2, row: makeRow("C", "C", 3) },
        { kind: 0, id: "D", index: 3, row: makeRow("D", "D", 4) },
      ],
      transform,
    );
    const result = handleDecodedDelta(
      manager,
      [
        { kind: 1, id: "B", index: 1 },
        { kind: 2, id: "D", index: 1, row: makeRow("D", "D*", 40) },
        { kind: 2, id: "C", index: 0 },
        { kind: 0, id: "E", index: 3, row: makeRow("E", "E", 5) },
      ],
      transform,
    );

    const current = reduceDeltas(initial, result);
    expect(current.map((item) => item.id)).toEqual(["C", "D", "A", "E"]);
    expect(current.find((item) => item.id === "D")?.name).toBe("D*");
  });

  it("clears state", () => {
    const manager = new SubscriptionManager<TestItem>();
    handleDecodedDelta(
      manager,
      [{ kind: 0, id: "1", index: 0, row: makeRow("1", "item1", 10) }],
      transform,
    );

    manager.clear();
    expect(manager.size).toBe(0);

    const result = handleDecodedDelta(
      manager,
      [{ kind: 0, id: "2", index: 0, row: makeRow("2", "item2", 20) }],
      transform,
    );
    expect(reduceDeltas(result).map((item) => item.id)).toEqual(["2"]);
  });
});
