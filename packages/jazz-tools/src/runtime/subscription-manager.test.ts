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
  const bytes = [1, ...root];
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
  return Uint8Array.from([1, ...uuidBytes(id), 0, 0, 0, 0, 0, 0, 0, 0]);
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
      ).toThrow(/malformed or noncanonical ResultKey V1 terminal occurrence key/);
      expect(manager.all()).toEqual([]);
    };

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

  it("keeps duplicate child occurrences distinct across hydration, moves, and edits", () => {
    const manager = new SubscriptionManager<IncludedRoot>();
    const rootId = "00000000-0000-4000-8000-000000000001";
    const a = "00000000-0000-4000-8000-000000000002";
    const b = "00000000-0000-4000-8000-000000000003";
    const rootKey = [10, ...uuidBytes(rootId)];
    const aKey = [10, ...uuidBytes(a)];
    const bKey = [10, ...uuidBytes(b)];
    // Pinned terminal child occurrence key v1: second copy, u64 BE ordinal 1.
    const bAgain = [...bKey, 255, 0, 0, 0, 0, 0, 0, 0, 1];
    const added = runtimeAddedRoot(rootId, 0, "root");
    const children = added.row.values[1]!;
    if (children.type !== "Array") throw new Error("expected children");
    children.value.push(
      ...[b, a, b].map((id) => ({
        type: "Row" as const,
        value: terminalTextChild(id, id === a ? "A" : "B"),
      })),
    );
    manager.handleDelta(emptyRuntimeDelta({ added: [added] }), transformIncluded);
    const apply = (
      edit: NonNullable<RuntimeSubscriptionDelta["terminalOperations"]>[number]["edit"],
    ) =>
      manager.handleDelta(
        emptyRuntimeDelta({
          terminalOperations: [{ root_key: rootKey, path: [{ Collection: 1 }], edit }],
        }),
        transformIncluded,
      );
    apply({ Update: { key: bAgain, row: terminalTextChild(b, "second") } });
    expect(manager.all()[0]?.children.map((child) => child.name)).toEqual(["B", "A", "second"]);
    apply({ Move: { key: bAgain, index: 0 } });
    apply({ Update: { key: bKey, row: terminalTextChild(b, "first") } });
    expect(manager.all()[0]?.children.map((child) => child.name)).toEqual(["second", "first", "A"]);
    apply({ Remove: { key: bAgain } });
    expect(manager.all()[0]?.children.map((child) => child.name)).toEqual(["first", "A"]);
    apply({ Insert: { key: bAgain, index: 2, row: terminalTextChild(b, "again") } });
    apply({ Move: { key: aKey, index: 0 } });
    expect(manager.all()[0]?.children.map((child) => child.name)).toEqual(["A", "first", "again"]);
    expect(manager.all()[0]?.children.map((child) => child.id)).toEqual([a, b, b]);
    expect(() => apply({ Remove: { key: [...bKey, 255, 0, 0, 0, 0, 0, 0, 0, 0] } })).toThrow(
      /noncanonical zero/,
    );
  });

  it("does not replay descendant removals already included in a complete root update", () => {
    const manager = new SubscriptionManager<IncludedRoot>();
    const rootId = "00000000-0000-4000-8000-000000000001";
    const childId = "00000000-0000-4000-8000-000000000002";
    const added = runtimeAddedRoot(rootId, 0, "before");
    const children = added.row.values[1]!;
    if (children.type !== "Array") throw new Error("expected children");
    children.value.push({ type: "Row", value: terminalTextChild(childId, "child") });
    manager.handleDelta(emptyRuntimeDelta({ added: [added] }), transformIncluded);
    const result = manager.handleDelta(
      emptyRuntimeDelta({
        updated: [runtimeAddedRoot(rootId, 0, "after")],
        terminalOperations: [
          {
            root_key: [10, ...uuidBytes(rootId)],
            path: [{ Collection: 1 }],
            edit: { Remove: { key: [10, ...uuidBytes(childId)] } },
          },
        ],
      }),
      transformIncluded,
    );
    expect(result.all).toEqual([{ id: rootId, title: "after", children: [] }]);
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

  it("removes many roots in one frame while keeping edits on surviving roots", () => {
    const manager = new SubscriptionManager<IncludedRoot>();
    const count = 2000;
    const ids = Array.from(
      { length: count },
      (_, index) => `00000000-0000-4000-8000-${(index + 1).toString(16).padStart(12, "0")}`,
    );
    const childId = "00000000-0000-4000-9000-000000000001";
    const childKey = [10, ...uuidBytes(childId)];
    manager.handleDelta(
      emptyRuntimeDelta({ added: ids.map((id, index) => runtimeAddedRoot(id, index, id)) }),
      transformIncluded,
    );

    const survivor = ids[count - 1]!;
    const result = manager.handleDelta(
      emptyRuntimeDelta({
        removed: ids.slice(0, -1).map((id) => runtimeRemovedRecord(id, 0)),
        terminalOperations: [
          {
            root_key: [10, ...uuidBytes(ids[0]!)],
            path: [{ Collection: 1 }],
            edit: { Remove: { key: childKey } },
          },
          {
            root_key: [10, ...uuidBytes(survivor)],
            path: [{ Collection: 1 }],
            edit: {
              Insert: { index: 0, key: childKey, row: terminalTextChild(childId, "child") },
            },
          },
        ],
      }),
      transformIncluded,
    );

    expect(result.all).toEqual([
      { id: survivor, title: survivor, children: [{ id: childId, name: "child" }] },
    ]);
    expect(manager.size).toBe(1);

    // A removed root no longer has a retained terminal row, so a later child
    // edit for it waits for re-hydration instead of patching stale state.
    const deferred = manager.handleDelta(
      emptyRuntimeDelta({
        terminalOperations: [
          {
            root_key: [10, ...uuidBytes(ids[1]!)],
            path: [{ Collection: 1 }],
            edit: {
              Insert: { index: 0, key: childKey, row: terminalTextChild(childId, "late") },
            },
          },
        ],
      }),
      transformIncluded,
    );
    expect(deferred.all).toEqual(result.all);
  });

  it("keeps occurrence addresses bounded by the live result under churn", () => {
    const manager = new SubscriptionManager<IncludedRoot>();
    const addresses = () =>
      (manager as unknown as { terminalOccurrenceAddresses: Map<string, string> })
        .terminalOccurrenceAddresses.size;
    const id = (n: number) => `00000000-0000-4000-8000-${n.toString(16).padStart(12, "0")}`;
    const window = 10;
    manager.handleDelta(
      emptyRuntimeDelta({
        added: Array.from({ length: window }, (_, n) => runtimeAddedRoot(id(n), n, `r${n}`)),
      }),
      transformIncluded,
    );
    for (let n = window; n < window + 500; n++) {
      manager.handleDelta(
        emptyRuntimeDelta({
          added: [runtimeAddedRoot(id(n), window - 1, `r${n}`)],
          removed: [runtimeRemovedRecord(id(n - window), 0)],
        }),
        transformIncluded,
      );
    }
    expect(manager.size).toBe(window);
    expect(addresses()).toBe(window);

    // A root that leaves and comes back is addressable by descendant edits again.
    const childId = "00000000-0000-4000-9000-000000000001";
    const childKey = [10, ...uuidBytes(childId)];
    manager.handleDelta(
      emptyRuntimeDelta({ added: [runtimeAddedRoot(id(0), 0, "back")] }),
      transformIncluded,
    );
    const edited = manager.handleDelta(
      emptyRuntimeDelta({
        terminalOperations: [
          {
            root_key: [10, ...uuidBytes(id(0))],
            path: [{ Collection: 1 }],
            edit: {
              Insert: { index: 0, key: childKey, row: terminalTextChild(childId, "child") },
            },
          },
        ],
      }),
      transformIncluded,
    );
    expect(edited.all?.[0]).toEqual({
      id: id(0),
      title: "back",
      children: [{ id: childId, name: "child" }],
    });
    expect(addresses()).toBe(window + 1);
  });

  it("replays a deferred edit on a typed root after its address was pruned", () => {
    const manager = new SubscriptionManager<IncludedRoot>();
    const rootId = "00000000-0000-4000-8000-000000000001";
    const joinedId = "00000000-0000-4000-8000-000000000002";
    const childId = "00000000-0000-4000-9000-000000000001";
    const childKey = [10, ...uuidBytes(childId)];
    const sidecar = typedResultKey(uuidBytes(rootId), [uuidBytes(joinedId)], [[1, "arm"]]);
    const added = (title: string): RuntimeSubscriptionAddedRow => ({
      sourceId: rootId,
      occurrenceKey: sidecar,
      index: 0,
      row: includedRootRow(rootId, title),
    });
    // Groove's ordered root key for this occurrence: root UUID, then the
    // union-arm label ahead of the joined UUID it discriminates.
    const orderedRoot = [
      10,
      ...uuidBytes(rootId),
      6,
      ...new TextEncoder().encode("arm"),
      0,
      0,
      10,
      ...uuidBytes(joinedId),
    ];
    manager.handleDelta(emptyRuntimeDelta({ added: [added("first")] }), transformIncluded);
    manager.handleDelta(
      emptyRuntimeDelta({ removed: [{ sourceId: rootId, occurrenceKey: sidecar, index: 0 }] }),
      transformIncluded,
    );
    expect(manager.all()).toEqual([]);

    const deferred = manager.handleDelta(
      emptyRuntimeDelta({
        terminalOperations: [
          {
            root_key: orderedRoot,
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

    const rehydrated = manager.handleDelta(
      emptyRuntimeDelta({ added: [added("again")] }),
      transformIncluded,
    );
    expect(rehydrated.all).toEqual([
      { id: rootId, title: "again", children: [{ id: childId, name: "child" }] },
    ]);
  });

  it("keeps a root that one frame removes and re-adds addressable by descendant edits", () => {
    const manager = new SubscriptionManager<IncludedRoot>();
    const rootId = "00000000-0000-4000-8000-000000000001";
    const otherId = "00000000-0000-4000-8000-000000000003";
    const childId = "00000000-0000-4000-8000-000000000002";
    const rootKey = [10, ...uuidBytes(rootId)];
    const childKey = [10, ...uuidBytes(childId)];
    manager.handleDelta(
      emptyRuntimeDelta({
        added: [runtimeAddedRoot(rootId, 0, "original"), runtimeAddedRoot(otherId, 1, "other")],
      }),
      transformIncluded,
    );

    // The root stays in the result, so its occurrence address must survive
    // pruning and its retained row must survive the removal.
    const readded = manager.handleDelta(
      emptyRuntimeDelta({
        removed: [runtimeRemovedRecord(rootId, 0)],
        added: [runtimeAddedRoot(rootId, 1, "readded")],
      }),
      transformIncluded,
    );
    expect(readded.all).toEqual([
      { id: otherId, title: "other", children: [] },
      { id: rootId, title: "readded", children: [] },
    ]);
    expect(
      (manager as unknown as { terminalOccurrenceAddresses: Map<string, string> })
        .terminalOccurrenceAddresses.size,
    ).toBe(2);

    const edited = manager.handleDelta(
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
    expect(edited.all).toEqual([
      { id: otherId, title: "other", children: [] },
      { id: rootId, title: "readded", children: [{ id: childId, name: "child" }] },
    ]);
  });

  it("rolls back a failed reset or bulk frame to the exact prior state", () => {
    const manager = new SubscriptionManager<IncludedRoot>();
    const id = (n: number) => `00000000-0000-4000-8000-${n.toString(16).padStart(12, "0")}`;
    const childId = "00000000-0000-4000-9000-000000000001";
    const childKey = [10, ...uuidBytes(childId)];
    const insertChild = (root: number, name: string) => ({
      root_key: [10, ...uuidBytes(id(root))],
      path: [{ Collection: 1 }],
      edit: { Insert: { index: 0, key: childKey, row: terminalTextChild(childId, name) } },
    });
    const removeMissingChild = (root: number) => ({
      root_key: [10, ...uuidBytes(id(root))],
      path: [{ Collection: 1 }],
      edit: { Remove: { key: [10, ...uuidBytes("00000000-0000-4000-9000-0000000000ff")] } },
    });
    manager.handleDelta(
      emptyRuntimeDelta({
        added: Array.from({ length: 40 }, (_, n) => runtimeAddedRoot(id(n), n, `r${n}`)),
      }),
      transformIncluded,
    );
    manager.handleDelta(
      emptyRuntimeDelta({ terminalOperations: [insertChild(3, "kept")] }),
      transformIncluded,
    );
    const before = manager.all();
    const addresses = () =>
      (manager as unknown as { terminalOccurrenceAddresses: Map<string, string> })
        .terminalOccurrenceAddresses.size;

    // A reset replaces all state before it meets a malformed occurrence key.
    const malformed = typedResultKey(
      uuidBytes(id(101)),
      [uuidBytes(id(102)), uuidBytes(id(103))],
      [
        [1, "second"],
        [0, "first"],
      ],
    );
    expect(() =>
      manager.handleDelta(
        emptyRuntimeDelta({
          reset: true,
          added: [
            runtimeAddedRoot(id(100), 0, "fresh"),
            { ...runtimeAddedRoot(id(101), 1, "bad"), occurrenceKey: malformed },
          ],
        }),
        transformIncluded,
      ),
    ).toThrow(/malformed or noncanonical ResultKey V1 terminal occurrence key/);
    expect(manager.all()).toEqual(before);

    // A bulk frame (32+ changes) that removes, moves and adds, then fails.
    expect(() =>
      manager.handleDelta(
        emptyRuntimeDelta({
          removed: Array.from({ length: 20 }, (_, n) => runtimeRemovedRecord(id(n + 10), n + 10)),
          added: Array.from({ length: 20 }, (_, n) => runtimeAddedRoot(id(n + 200), n, `new${n}`)),
          terminalOperations: [removeMissingChild(5)],
        }),
        transformIncluded,
      ),
    ).toThrow(/terminal child removal addressed missing key/);
    expect(manager.all()).toEqual(before);
    expect(manager.size).toBe(40);
    expect(addresses()).toBe(40);

    // Rolled-back state is fully live: removed roots are addressable again and
    // the failed frame's roots are not.
    const edited = manager.handleDelta(
      emptyRuntimeDelta({
        removed: [runtimeRemovedRecord(id(0), 0)],
        terminalOperations: [insertChild(15, "after")],
      }),
      transformIncluded,
    );
    expect(edited.all).toHaveLength(39);
    expect(edited.all?.find((root) => root.id === id(15))?.children).toEqual([
      { id: childId, name: "after" },
    ]);
    expect(edited.all?.find((root) => root.id === id(3))?.children).toEqual([
      { id: childId, name: "kept" },
    ]);
    expect(edited.all?.some((root) => root.id === id(200))).toBe(false);
  });

  it("keeps a joined root that one frame removes and re-adds addressable by descendant edits", () => {
    // A typed occurrence's ordered key is not derivable from its public id, so
    // a pruned address would leave descendant edits for it unresolvable.
    const manager = new SubscriptionManager<IncludedRoot>();
    const rootId = "00000000-0000-4000-8000-000000000001";
    const joinedId = "00000000-0000-4000-8000-000000000002";
    const childId = "00000000-0000-4000-9000-000000000001";
    const sidecar = typedResultKey(uuidBytes(rootId), [uuidBytes(joinedId)], [[1, "arm"]]);
    const orderedRootKey = [
      10,
      ...uuidBytes(rootId),
      6,
      ...new TextEncoder().encode("arm"),
      0,
      0,
      10,
      ...uuidBytes(joinedId),
    ];
    const added = (title: string) => ({
      sourceId: rootId,
      occurrenceKey: sidecar,
      index: 0,
      row: includedRootRow(rootId, title),
    });
    manager.handleDelta(emptyRuntimeDelta({ added: [added("first")] }), transformIncluded);
    manager.handleDelta(
      emptyRuntimeDelta({
        removed: [{ sourceId: rootId, occurrenceKey: sidecar, index: 0 }],
        added: [added("again")],
      }),
      transformIncluded,
    );

    const edited = manager.handleDelta(
      emptyRuntimeDelta({
        terminalOperations: [
          {
            root_key: orderedRootKey,
            path: [{ Collection: 1 }],
            edit: {
              Insert: {
                index: 0,
                key: [10, ...uuidBytes(childId)],
                row: terminalTextChild(childId, "child"),
              },
            },
          },
        ],
      }),
      transformIncluded,
    );
    expect(edited.all?.map((root) => root.children)).toEqual([[{ id: childId, name: "child" }]]);
  });

  it("drops the retained row of a root that leaves the result", () => {
    const manager = new SubscriptionManager<IncludedRoot>();
    const rootId = "00000000-0000-4000-8000-000000000001";
    manager.handleDelta(
      emptyRuntimeDelta({ added: [runtimeAddedRoot(rootId, 0, "original")] }),
      transformIncluded,
    );
    manager.handleDelta(
      emptyRuntimeDelta({ removed: [runtimeRemovedRecord(rootId, 0)] }),
      transformIncluded,
    );
    expect(
      (manager as unknown as { terminalRows: Map<string, unknown> }).terminalRows.has(rootId),
    ).toBe(false);
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

  it("applies runs of removals around indexed inserts in order", () => {
    const manager = new SubscriptionManager<TestItem>();
    const ids = ["A", "B", "C", "D", "E", "F"];
    const initial = handleDecodedDelta(
      manager,
      ids.map((id, index) => ({ kind: 0 as const, id, index, row: makeRow(id, id, index) })),
      transform,
    );
    const result = handleDecodedDelta(
      manager,
      [
        { kind: 1, id: "A", index: 0 },
        { kind: 1, id: "C", index: 0 },
        { kind: 0, id: "X", index: 1, row: makeRow("X", "X", 9) },
        { kind: 1, id: "D", index: 2 },
        { kind: 1, id: "F", index: 2 },
        { kind: 1, id: "missing", index: 2 },
      ],
      transform,
    );

    expect(manager.all().map((item) => item.id)).toEqual(["B", "X", "E"]);
    expect(reduceDeltas(initial, { delta: result.delta }).map((item) => item.id)).toEqual([
      "B",
      "X",
      "E",
    ]);
    expect(manager.size).toBe(3);
  });

  it("keeps positions exact for indexed changes after a removal run", () => {
    const manager = new SubscriptionManager<TestItem>();
    const ids = ["A", "B", "C", "D", "E"];
    const frames = [
      handleDecodedDelta(
        manager,
        ids.map((id, index) => ({ kind: 0 as const, id, index, row: makeRow(id, id, index) })),
        transform,
      ),
    ];
    frames.push(
      handleDecodedDelta(
        manager,
        [
          { kind: 1, id: "B", index: 1 },
          { kind: 1, id: "C", index: 1 },
        ],
        transform,
      ),
    );
    frames.push(handleDecodedDelta(manager, [{ kind: 1, id: "E", index: 2 }], transform));
    expect(manager.all().map((item) => item.id)).toEqual(["A", "D"]);
    frames.push(
      handleDecodedDelta(
        manager,
        [
          { kind: 0, id: "B", index: 1, row: makeRow("B", "B", 5) },
          { kind: 2, id: "D", index: 0, row: makeRow("D", "D", 6) },
        ],
        transform,
      ),
    );

    expect(manager.all().map((item) => item.id)).toEqual(["D", "B", "A"]);
    expect(reduceDeltas(...frames.map((frame) => ({ delta: frame.delta })))).toEqual(manager.all());
  });

  it("applies many same-index replacements, updates and moves in one frame", () => {
    const manager = new SubscriptionManager<TestItem>();
    const ids = Array.from({ length: 40 }, (_, index) => `r${index}`);
    const initial = handleDecodedDelta(
      manager,
      ids.map((id, index) => ({ kind: 0 as const, id, index, row: makeRow(id, id, 0) })),
      transform,
    );
    // Changes arrive in ascending final-index order, as the runtime emits them.
    const expected = [...ids];
    const frame: DecodedRowDelta = [{ kind: 2, id: "r39", index: 0 }];
    expected.splice(expected.indexOf("r39"), 1);
    expected.unshift("r39");
    for (let index = 1; index < 37; index += 4) {
      // Replace the row at `index` with a new one at the same position.
      frame.push({ kind: 1, id: expected[index]!, index });
      frame.push({ kind: 0, id: `n${index}`, index, row: makeRow(`n${index}`, "new", 1) });
      expected[index] = `n${index}`;
      // Update the next row in place.
      const next = expected[index + 1]!;
      frame.push({ kind: 2, id: next, index: index + 1, row: makeRow(next, "upd", 2) });
    }
    frame.push({ kind: 2, id: "r38", index: 39, row: makeRow("r38", "upd", 3) });
    const result = handleDecodedDelta(manager, frame, transform);

    expect(manager.all().map((item) => item.id)).toEqual(expected);
    expect(reduceDeltas(initial, { delta: result.delta })).toEqual(manager.all());

    // The next frame addresses rows by id, so positions must be exact again.
    const next = handleDecodedDelta(
      manager,
      [
        { kind: 1, id: "r38", index: 39 },
        { kind: 2, id: "n21", index: 0, row: makeRow("n21", "moved", 4) },
      ],
      transform,
    );
    expected.splice(expected.indexOf("r38"), 1);
    expected.splice(expected.indexOf("n21"), 1);
    expected.unshift("n21");
    expect(manager.all().map((item) => item.id)).toEqual(expected);
    expect(reduceDeltas(initial, { delta: result.delta }, { delta: next.delta })).toEqual(
      manager.all(),
    );
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
