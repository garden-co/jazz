/**
 * Tests for subscription-manager module.
 */

import { describe, it, expect } from "vitest";
import { SubscriptionManager, applySubscriptionDelta } from "./subscription-manager.js";
import type { SubscriptionDelta } from "./subscription-manager.js";
import {
  encodeNativeRowValues,
  storageColumnValueType,
  writeDescriptor,
} from "./native-runtime/native-row-codec.js";
import { PostcardWriter } from "./native-runtime/native-codec.js";
import type {
  ColumnDescriptor,
  NativeRowDelta,
  WasmRow,
  RowDelta,
  Value,
} from "../drivers/types.js";

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

function reduceDeltas(...deltas: SubscriptionDelta<TestItem>[]): TestItem[] {
  const current: TestItem[] = [];
  for (const delta of deltas) {
    applySubscriptionDelta(current, delta);
  }
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

function terminalRowData(id: string, name: string, count: number): Uint8Array {
  return Uint8Array.from([
    ...uuidBytes(id),
    ...encodeNativeRowValues(currentRowColumns(nativeColumns), [
      { type: "Text", value: name },
      { type: "Integer", value: count },
    ]),
  ]);
}

function terminalRootWithEmptyChildren(id: string, title: string): Uint8Array {
  const text = new TextEncoder().encode(title);
  const bytes: number[] = [...uuidBytes(id)];
  // The root uses CurrentRow's nullable carrier. The child collection stays a
  // terminal record when it is populated by a descendant operation.
  pushU32(bytes, 21 + text.byteLength);
  bytes.push(1, ...text, 1, 0, 0, 0, 0);
  return Uint8Array.from(bytes);
}

function nativeRootWithEmptyChildren(title: string): Uint8Array {
  const text = new TextEncoder().encode(title);
  const bytes: number[] = [];
  pushU32(bytes, 4 + text.byteLength);
  bytes.push(...text);
  pushU32(bytes, 0);
  return Uint8Array.from(bytes);
}

function currentRowColumns(columns: readonly ColumnDescriptor[]): readonly ColumnDescriptor[] {
  return columns.map((column) => ({ ...column, nullable: true, sparse: undefined }));
}

function terminalDescriptor(columns: readonly ColumnDescriptor[]): number[] {
  const writer = new PostcardWriter();
  writeDescriptor(writer, [
    { name: "__jazz_terminal_row_key", valueType: { tag: 10 } },
    ...columns.map((column) => ({
      name: column.name,
      valueType: storageColumnValueType(column),
    })),
  ]);
  return [...writer.finish()];
}

function currentRowTerminalDescriptor(columns: readonly ColumnDescriptor[]): number[] {
  const writer = new PostcardWriter();
  writeDescriptor(writer, [
    { name: "row_uuid", valueType: { tag: 10 } },
    ...currentRowColumns(columns).map((column) => ({
      name: `user_${column.name}`,
      valueType: storageColumnValueType(column),
    })),
  ]);
  return [...writer.finish()];
}

function terminalTextChild(id: string, name: string): Uint8Array {
  return Uint8Array.from([...uuidBytes(id), ...new TextEncoder().encode(name)]);
}

function nativeAddedRecord(id: string, index: number, name: string, count: number): Uint8Array {
  const data = nativeRowData(name, count);
  return nativeAddedRawRecord(id, index, data);
}

function nativeAddedRawRecord(id: string, index: number, data: Uint8Array): Uint8Array {
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
    expect(reduceDeltas(result).map((item) => item.id)).toEqual(["1"]);
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
    expect(reduceDeltas(result).map((item) => item.id)).toEqual(["1", "2"]);
    expect(manager.size).toBe(2);
  });

  it("decodes native subscription additions", () => {
    const manager = new SubscriptionManager<TestItem>();
    const id = "00000000-0000-4000-8000-000000000001";
    const delta: NativeRowDelta = {
      __jazzNativeRowDelta: true,
      added: nativeAddedRecord(id, 0, "native", -42),
      removed: new Uint8Array(),
      updated: new Uint8Array(),
      addedCount: 1,
      removedCount: 0,
      updatedCount: 0,
    };

    const result = manager.handleDelta(delta, transform, nativeColumns);

    expect(reduceDeltas(result)).toEqual([{ id, name: "native", count: -42 }]);
    expect(result.delta).toEqual([
      {
        kind: 0,
        id,
        index: 0,
        item: { id, name: "native", count: -42 },
      },
    ]);
  });

  it("applies typed terminal patches without a replacement row delta", () => {
    const manager = new SubscriptionManager<TestItem>();
    const id = "00000000-0000-4000-8000-000000000001";
    manager.handleDelta(
      {
        __jazzNativeRowDelta: true,
        added: nativeAddedRecord(id, 0, "before", 1),
        removed: new Uint8Array(),
        updated: new Uint8Array(),
        addedCount: 1,
        removedCount: 0,
        updatedCount: 0,
      },
      transform,
      nativeColumns,
    );

    const key = [10, ...uuidBytes(id)];
    const result = manager.handleDelta(
      {
        __jazzNativeRowDelta: true,
        added: new Uint8Array(),
        removed: new Uint8Array(),
        updated: new Uint8Array(),
        addedCount: 0,
        removedCount: 0,
        updatedCount: 0,
        terminalOperations: [
          {
            root_key: key,
            rootDescriptor: currentRowTerminalDescriptor(nativeColumns),
            path: [],
            edit: { Update: { key, value: [...terminalRowData(id, "after", 2)] } },
          },
        ],
      },
      transform,
      nativeColumns,
    );

    expect(result.delta).toEqual([
      { kind: 2, id, index: 0, item: { id, name: "after", count: 2 } },
    ]);
    expect(result.all).toEqual([{ id, name: "after", count: 2 }]);
  });

  it("matches a compound terminal key to the seeded full occurrence identity", () => {
    const manager = new SubscriptionManager<TestItem>();
    const id = "00000000-0000-4000-8000-000000000001";
    const joinedId = "00000000-0000-4000-8000-000000000002";
    const key = [10, ...uuidBytes(id), 10, ...uuidBytes(joinedId)];

    manager.handleDelta(
      {
        __jazzNativeRowDelta: true,
        added: nativeAddedRecord(id, 0, "before", 6),
        removed: new Uint8Array(),
        updated: new Uint8Array(),
        addedCount: 1,
        removedCount: 0,
        updatedCount: 0,
        addedOccurrenceKeys: [Uint8Array.from([1, ...uuidBytes(id), ...uuidBytes(joinedId)])],
      },
      transform,
      nativeColumns,
    );

    const result = manager.handleDelta(
      {
        __jazzNativeRowDelta: true,
        added: new Uint8Array(),
        removed: new Uint8Array(),
        updated: new Uint8Array(),
        addedCount: 0,
        removedCount: 0,
        updatedCount: 0,
        terminalOperations: [
          {
            root_key: key,
            rootDescriptor: currentRowTerminalDescriptor(nativeColumns),
            path: [],
            edit: { Update: { key, value: [...terminalRowData(id, "joined", 7)] } },
          },
        ],
      },
      transform,
      nativeColumns,
    );

    expect(result.all).toEqual([{ id, name: "joined", count: 7 }]);
    expect(result.delta[0]?.id).toBe(
      `result:01${Array.from(uuidBytes(id), (byte) => byte.toString(16).padStart(2, "0")).join("")}${Array.from(uuidBytes(joinedId), (byte) => byte.toString(16).padStart(2, "0")).join("")}`,
    );
  });

  it("does not collapse malformed or typed terminal root keys to their leading UUID", () => {
    const id = "00000000-0000-4000-8000-000000000001";
    const joinedId = "00000000-0000-4000-8000-000000000002";
    const fullOccurrence = Uint8Array.from([1, ...uuidBytes(id), ...uuidBytes(joinedId)]);
    const malformed = [10, ...uuidBytes(id), 10];
    const typedComponent = [10, ...uuidBytes(id), 8, 0x61];

    for (const key of [malformed, typedComponent]) {
      const manager = new SubscriptionManager<TestItem>();
      manager.handleDelta(
        {
          __jazzNativeRowDelta: true,
          added: nativeAddedRecord(id, 0, "before", 6),
          removed: new Uint8Array(),
          updated: new Uint8Array(),
          addedCount: 1,
          removedCount: 0,
          updatedCount: 0,
          addedOccurrenceKeys: [fullOccurrence],
        },
        transform,
        nativeColumns,
      );

      expect(() =>
        manager.handleDelta(
          {
            __jazzNativeRowDelta: true,
            added: new Uint8Array(),
            removed: new Uint8Array(),
            updated: new Uint8Array(),
            addedCount: 0,
            removedCount: 0,
            updatedCount: 0,
            terminalOperations: [
              {
                root_key: key,
                rootDescriptor: currentRowTerminalDescriptor(nativeColumns),
                path: [],
                edit: { Update: { key, value: [...terminalRowData(id, "after", 7)] } },
              },
            ],
          },
          transform,
          nativeColumns,
        ),
      ).toThrow(/addressed missing root/);
      expect(manager.all()).toEqual([{ id, name: "before", count: 6 }]);
    }

    const typedV2Occurrence = Uint8Array.from([
      2,
      ...uuidBytes(id),
      0,
      0,
      0,
      1,
      ...uuidBytes(joinedId),
      0,
      0,
      0,
      1,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      3,
      0x61,
      0x72,
      0x6d,
    ]);
    const manager = new SubscriptionManager<TestItem>();
    manager.handleDelta(
      {
        __jazzNativeRowDelta: true,
        added: nativeAddedRecord(id, 0, "typed", 8),
        removed: new Uint8Array(),
        updated: new Uint8Array(),
        addedCount: 1,
        removedCount: 0,
        updatedCount: 0,
        addedOccurrenceKeys: [typedV2Occurrence],
      },
      transform,
      nativeColumns,
    );
    const legacyComposite = [10, ...uuidBytes(id), 10, ...uuidBytes(joinedId)];
    expect(
      manager.handleDelta(
        {
          __jazzNativeRowDelta: true,
          added: new Uint8Array(),
          removed: new Uint8Array(),
          updated: new Uint8Array(),
          addedCount: 0,
          removedCount: 0,
          updatedCount: 0,
          terminalOperations: [
            {
              root_key: legacyComposite,
              rootDescriptor: currentRowTerminalDescriptor(nativeColumns),
              path: [],
              edit: { Update: { key: legacyComposite, value: [...terminalRowData(id, "bad", 9)] } },
            },
          ],
        },
        transform,
        nativeColumns,
      ),
    ).toEqual({ delta: [], all: [{ id, name: "typed", count: 8 }] });
    expect(manager.all()).toEqual([{ id, name: "typed", count: 8 }]);
  });

  it("rejects noncanonical typed terminal occurrence sidecars and ordered-key collisions", () => {
    const id = "00000000-0000-4000-8000-000000000001";
    const joinedId = "00000000-0000-4000-8000-000000000002";
    const joinedSecondId = "00000000-0000-4000-8000-000000000003";
    const root = uuidBytes(id);
    const joined = uuidBytes(joinedId);
    const secondJoined = uuidBytes(joinedSecondId);
    const emptyNative = {
      __jazzNativeRowDelta: true as const,
      added: new Uint8Array(),
      removed: new Uint8Array(),
      updated: new Uint8Array(),
      addedCount: 0,
      removedCount: 0,
      updatedCount: 0,
    };
    const rejectSidecar = (sidecar: Uint8Array) => {
      const manager = new SubscriptionManager<TestItem>();
      expect(() =>
        manager.handleDelta(
          {
            ...emptyNative,
            added: nativeAddedRecord(id, 0, "typed", 1),
            addedCount: 1,
            addedOccurrenceKeys: [sidecar],
          },
          transform,
          nativeColumns,
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
    const invalidUtf8 = typedResultKey(root, [joined], [[0, "valid"]]);
    invalidUtf8[invalidUtf8.length - 1] = 0xff;
    rejectSidecar(invalidUtf8);

    const manager = new SubscriptionManager<TestItem>();
    const registry = manager as unknown as {
      registerTerminalOccurrenceAddress(ordered: Uint8Array, occurrence: string): void;
    };
    const ordered = Uint8Array.from([10, ...root, 6, 0x61, 0x00, 0x00, 10, ...joined]);
    registry.registerTerminalOccurrenceAddress(ordered, "result:02first");
    expect(() => registry.registerTerminalOccurrenceAddress(ordered, "result:02second")).toThrow(
      /conflicting typed terminal occurrence keys share an ordered root key/,
    );
  });

  it("applies typed-v2 terminal update, removal, and reopen through its exact ordered key", () => {
    const manager = new SubscriptionManager<TestItem>();
    const id = "00000000-0000-4000-8000-000000000001";
    const joinedId = "00000000-0000-4000-8000-000000000002";
    const sidecar = Uint8Array.from([
      2,
      ...uuidBytes(id),
      0,
      0,
      0,
      1,
      ...uuidBytes(joinedId),
      0,
      0,
      0,
      1,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      3,
      0x61,
      0x72,
      0x6d,
    ]);
    // Groove's ordered Record key: root UUID, union-arm String, joined UUID.
    const key = [10, ...uuidBytes(id), 6, 0x61, 0x72, 0x6d, 0, 0, 10, ...uuidBytes(joinedId)];
    const emptyNative = {
      __jazzNativeRowDelta: true as const,
      added: new Uint8Array(),
      removed: new Uint8Array(),
      updated: new Uint8Array(),
      addedCount: 0,
      removedCount: 0,
      updatedCount: 0,
    };

    manager.handleDelta(
      {
        ...emptyNative,
        added: nativeAddedRecord(id, 0, "opened", 1),
        addedCount: 1,
        addedOccurrenceKeys: [sidecar],
      },
      transform,
      nativeColumns,
    );
    const updated = manager.handleDelta(
      {
        ...emptyNative,
        terminalOperations: [
          {
            root_key: key,
            rootDescriptor: currentRowTerminalDescriptor(nativeColumns),
            path: [],
            edit: { Update: { key, value: [...terminalRowData(id, "updated", 2)] } },
          },
        ],
      },
      transform,
      nativeColumns,
    );
    expect(updated.all).toEqual([{ id, name: "updated", count: 2 }]);
    expect(updated.delta[0]?.id).toMatch(/^result:02/);

    const removed = manager.handleDelta(
      {
        ...emptyNative,
        terminalOperations: [{ root_key: key, path: [], edit: { Remove: { key } } }],
      },
      transform,
      nativeColumns,
    );
    expect(removed).toEqual({ delta: [{ kind: 1, id: updated.delta[0]?.id, index: 0 }], all: [] });

    const reopened = manager.handleDelta(
      {
        ...emptyNative,
        terminalOperations: [
          {
            root_key: key,
            rootDescriptor: currentRowTerminalDescriptor(nativeColumns),
            path: [],
            edit: { Insert: { key, index: 0, value: [...terminalRowData(id, "reopened", 3)] } },
          },
        ],
      },
      transform,
      nativeColumns,
    );
    expect(reopened.all).toEqual([{ id, name: "reopened", count: 3 }]);
    expect(reopened.delta[0]?.id).toBe(updated.delta[0]?.id);
  });

  it("bridges a unique legacy snapshot root to its composite terminal address", () => {
    const manager = new SubscriptionManager<TestItem>();
    const id = "00000000-0000-4000-8000-000000000001";
    const joinedId = "00000000-0000-4000-8000-000000000002";
    const key = [10, ...uuidBytes(id), 10, ...uuidBytes(joinedId)];
    manager.handleDelta(
      {
        __jazzNativeRowDelta: true,
        added: nativeAddedRecord(id, 0, "snapshot", 1),
        removed: new Uint8Array(),
        updated: new Uint8Array(),
        addedCount: 1,
        removedCount: 0,
        updatedCount: 0,
      },
      transform,
      nativeColumns,
    );

    const result = manager.handleDelta(
      {
        __jazzNativeRowDelta: true,
        added: new Uint8Array(),
        removed: new Uint8Array(),
        updated: new Uint8Array(),
        addedCount: 0,
        removedCount: 0,
        updatedCount: 0,
        terminalOperations: [
          {
            root_key: key,
            rootDescriptor: currentRowTerminalDescriptor(nativeColumns),
            path: [],
            edit: { Update: { key, value: [...terminalRowData(id, "patched", 2)] } },
          },
        ],
      },
      transform,
      nativeColumns,
    );

    expect(result.all).toEqual([{ id, name: "patched", count: 2 }]);
    expect(result.delta).toMatchObject([{ kind: 2, id }]);
  });

  it("treats a missing UUID-only terminal root update as an idempotent stale patch", () => {
    const manager = new SubscriptionManager<TestItem>();
    const id = "00000000-0000-4000-8000-000000000001";
    const key = [10, ...uuidBytes(id)];

    const result = manager.handleDelta(
      {
        __jazzNativeRowDelta: true,
        added: new Uint8Array(),
        removed: new Uint8Array(),
        updated: new Uint8Array(),
        addedCount: 0,
        removedCount: 0,
        updatedCount: 0,
        terminalOperations: [
          {
            root_key: key,
            rootDescriptor: currentRowTerminalDescriptor(nativeColumns),
            path: [],
            edit: { Update: { key, value: [...terminalRowData(id, "stale", 1)] } },
          },
        ],
      },
      transform,
      nativeColumns,
    );

    expect(result).toEqual({ delta: [], all: [] });
  });

  it("rejects mismatched terminal identities without mutating subscription state", () => {
    const manager = new SubscriptionManager<TestItem>();
    const id = "00000000-0000-4000-8000-000000000001";
    const other = "00000000-0000-4000-8000-000000000002";
    const key = [10, ...uuidBytes(id)];

    manager.handleDelta(
      {
        __jazzNativeRowDelta: true,
        added: nativeAddedRecord(id, 0, "before", 1),
        removed: new Uint8Array(),
        updated: new Uint8Array(),
        addedCount: 1,
        removedCount: 0,
        updatedCount: 0,
      },
      transform,
      nativeColumns,
    );

    expect(() =>
      manager.handleDelta(
        {
          __jazzNativeRowDelta: true,
          added: new Uint8Array(),
          removed: new Uint8Array(),
          updated: new Uint8Array(),
          addedCount: 0,
          removedCount: 0,
          updatedCount: 0,
          terminalOperations: [
            {
              root_key: key,
              rootDescriptor: currentRowTerminalDescriptor(nativeColumns),
              path: [],
              edit: { Update: { key, value: [...terminalRowData(other, "corrupt", 2)] } },
            },
          ],
        },
        transform,
        nativeColumns,
      ),
    ).toThrow(/does not match addressed key/);
    expect(manager.all()).toEqual([{ id, name: "before", count: 1 }]);
  });

  it("publishes explicit Added and Removed changes for terminal roots", () => {
    const manager = new SubscriptionManager<TestItem>();
    const id = "00000000-0000-4000-8000-000000000001";
    const key = [10, ...uuidBytes(id)];
    const emptyNative = {
      __jazzNativeRowDelta: true as const,
      added: new Uint8Array(),
      removed: new Uint8Array(),
      updated: new Uint8Array(),
      addedCount: 0,
      removedCount: 0,
      updatedCount: 0,
    };
    const added = manager.handleDelta(
      {
        ...emptyNative,
        terminalOperations: [
          {
            root_key: key,
            rootDescriptor: currentRowTerminalDescriptor(nativeColumns),
            path: [],
            edit: { Insert: { index: 0, key, value: [...terminalRowData(id, "root", 1)] } },
          },
        ],
      },
      transform,
      nativeColumns,
    );
    expect(added.delta).toEqual([{ kind: 0, id, index: 0, item: { id, name: "root", count: 1 } }]);

    const removed = manager.handleDelta(
      {
        ...emptyNative,
        terminalOperations: [{ root_key: key, path: [], edit: { Remove: { key } } }],
      },
      transform,
      nativeColumns,
    );
    expect(removed).toEqual({ delta: [{ kind: 1, id, index: 0 }], all: [] });
  });

  it("decodes every root terminal payload through the CurrentRow nullable carrier", () => {
    type NullableRoot = { id: string; title: string | null; done: boolean | null };
    const manager = new SubscriptionManager<NullableRoot>();
    const id = "00000000-0000-4000-8000-000000000001";
    const key = [10, ...uuidBytes(id)];
    const columns: ColumnDescriptor[] = [
      { name: "title", column_type: { type: "Text" }, nullable: false },
      { name: "done", column_type: { type: "Boolean" }, nullable: false },
    ];
    const currentRowPayload = Uint8Array.from([
      ...uuidBytes(id),
      ...encodeNativeRowValues(currentRowColumns(columns), [
        { type: "Null" },
        { type: "Boolean", value: false },
      ]),
    ]);

    const result = manager.handleDelta(
      {
        __jazzNativeRowDelta: true,
        added: new Uint8Array(),
        removed: new Uint8Array(),
        updated: new Uint8Array(),
        addedCount: 0,
        removedCount: 0,
        updatedCount: 0,
        terminalOperations: [
          {
            root_key: key,
            rootDescriptor: currentRowTerminalDescriptor(columns),
            path: [],
            edit: { Insert: { index: 0, key, value: [...currentRowPayload] } },
          },
        ],
      },
      (row) => {
        const title = row.values[0];
        const done = row.values[1];
        return {
          id: row.id,
          title: title?.type === "Text" ? title.value : null,
          done: done?.type === "Boolean" ? done.value : null,
        };
      },
      columns,
    );

    expect(result.all).toEqual([{ id, title: null, done: false }]);
  });

  it("uses the producer descriptor for a logical terminal root instead of guessing CurrentRow", () => {
    const manager = new SubscriptionManager<TestItem>();
    const id = "00000000-0000-4000-8000-000000000001";
    const key = [10, ...uuidBytes(id)];
    const logicalPayload = Uint8Array.from([
      ...uuidBytes(id),
      ...encodeNativeRowValues(nativeColumns, [
        { type: "Text", value: "logical" },
        { type: "Integer", value: 7 },
      ]),
    ]);

    const result = manager.handleDelta(
      {
        __jazzNativeRowDelta: true,
        added: new Uint8Array(),
        removed: new Uint8Array(),
        updated: new Uint8Array(),
        addedCount: 0,
        removedCount: 0,
        updatedCount: 0,
        terminalOperations: [
          {
            root_key: key,
            rootDescriptor: terminalDescriptor(nativeColumns),
            path: [],
            edit: { Insert: { index: 0, key, value: [...logicalPayload] } },
          },
        ],
      },
      transform,
      nativeColumns,
    );

    expect(result.all).toEqual([{ id, name: "logical", count: 7 }]);
  });

  it("fails closed for missing, incompatible, unknown, or trailing terminal descriptors", () => {
    const id = "00000000-0000-4000-8000-000000000001";
    const key = [10, ...uuidBytes(id)];
    const payload = Uint8Array.from([
      ...uuidBytes(id),
      ...encodeNativeRowValues(nativeColumns, [
        { type: "Text", value: "logical" },
        { type: "Integer", value: 7 },
      ]),
    ]);
    const wrongOrder = terminalDescriptor([nativeColumns[1]!, nativeColumns[0]!]);
    const wrongType = terminalDescriptor([
      { ...nativeColumns[0]!, column_type: { type: "Integer" } },
      nativeColumns[1]!,
    ]);
    const unknownTagWriter = new PostcardWriter();
    writeDescriptor(unknownTagWriter, [
      { name: "__jazz_terminal_row_key", valueType: { tag: 10 } },
      { name: "name", valueType: { tag: 99 } },
      { name: "count", valueType: { tag: 4 } },
    ]);
    const descriptors: Array<number[] | undefined> = [
      undefined,
      wrongOrder,
      wrongType,
      [...unknownTagWriter.finish()],
      [...terminalDescriptor(nativeColumns), 0],
    ];

    for (const rootDescriptor of descriptors) {
      const manager = new SubscriptionManager<TestItem>();
      expect(() =>
        manager.handleDelta(
          {
            __jazzNativeRowDelta: true,
            added: new Uint8Array(),
            removed: new Uint8Array(),
            updated: new Uint8Array(),
            addedCount: 0,
            removedCount: 0,
            updatedCount: 0,
            terminalOperations: [
              {
                root_key: key,
                ...(rootDescriptor === undefined ? {} : { rootDescriptor }),
                path: [],
                edit: { Insert: { index: 0, key, value: [...payload] } },
              },
            ],
          },
          transform,
          nativeColumns,
        ),
      ).toThrow(/terminal (operation is missing its root descriptor|root descriptor)/);
    }
  });

  it("accepts terminal root descriptors that retain sparse current-row carriers", () => {
    const id = "00000000-0000-4000-8000-000000000001";
    const key = [10, ...uuidBytes(id)];
    const sparseColumns: ColumnDescriptor[] = [
      { name: "name", column_type: { type: "Text" }, nullable: false, sparse: true },
      { name: "ownerId", column_type: { type: "Uuid" }, nullable: true, sparse: true },
    ];
    const payload = Uint8Array.from([
      ...uuidBytes(id),
      ...encodeNativeRowValues(sparseColumns, [
        { type: "Text", value: "logical" },
        { type: "Null" },
      ]),
    ]);
    const manager = new SubscriptionManager<{ id: string; name: string }>();
    const transform = (row: WasmRow) => ({
      id: row.id,
      name: (row.values[0] as { type: "Text"; value: string }).value,
    });

    const result = manager.handleDelta(
      {
        __jazzNativeRowDelta: true,
        added: new Uint8Array(),
        removed: new Uint8Array(),
        updated: new Uint8Array(),
        addedCount: 0,
        removedCount: 0,
        updatedCount: 0,
        terminalOperations: [
          {
            root_key: key,
            rootDescriptor: terminalDescriptor(sparseColumns),
            path: [],
            edit: { Insert: { index: 0, key, value: [...payload] } },
          },
        ],
      },
      transform,
      sparseColumns,
    );

    expect(result.all).toEqual([{ id, name: "logical" }]);
  });

  it("applies root insert positions in producer order after earlier removals", () => {
    const manager = new SubscriptionManager<TestItem>();
    const ids = {
      b: "00000000-0000-4000-8000-00000000000b",
      c: "00000000-0000-4000-8000-00000000000c",
      d: "00000000-0000-4000-8000-00000000000d",
    };
    const key = (id: string) => [10, ...uuidBytes(id)];
    const emptyNative = {
      __jazzNativeRowDelta: true as const,
      added: new Uint8Array(),
      removed: new Uint8Array(),
      updated: new Uint8Array(),
      addedCount: 0,
      removedCount: 0,
      updatedCount: 0,
    };
    manager.handleDelta(
      {
        ...emptyNative,
        terminalOperations: [
          {
            root_key: key(ids.b),
            rootDescriptor: currentRowTerminalDescriptor(nativeColumns),
            path: [],
            edit: {
              Insert: { index: 0, key: key(ids.b), value: [...terminalRowData(ids.b, "B", 2)] },
            },
          },
          {
            root_key: key(ids.c),
            rootDescriptor: currentRowTerminalDescriptor(nativeColumns),
            path: [],
            edit: {
              Insert: { index: 1, key: key(ids.c), value: [...terminalRowData(ids.c, "C", 3)] },
            },
          },
        ],
      },
      transform,
      nativeColumns,
    );

    const result = manager.handleDelta(
      {
        ...emptyNative,
        terminalOperations: [
          { root_key: key(ids.b), path: [], edit: { Remove: { key: key(ids.b) } } },
          {
            root_key: key(ids.d),
            rootDescriptor: currentRowTerminalDescriptor(nativeColumns),
            path: [],
            edit: {
              Insert: { index: 1, key: key(ids.d), value: [...terminalRowData(ids.d, "D", 4)] },
            },
          },
        ],
      },
      transform,
      nativeColumns,
    );

    expect(result.all?.map((row) => row.id)).toEqual([ids.c, ids.d]);
  });

  it("rejects mismatched root addressing and unresolved child paths", () => {
    const manager = new SubscriptionManager<TestItem>();
    const id = "00000000-0000-4000-8000-000000000001";
    const other = "00000000-0000-4000-8000-000000000002";
    const key = [10, ...uuidBytes(id)];
    const otherKey = [10, ...uuidBytes(other)];
    const emptyNative = {
      __jazzNativeRowDelta: true as const,
      added: new Uint8Array(),
      removed: new Uint8Array(),
      updated: new Uint8Array(),
      addedCount: 0,
      removedCount: 0,
      updatedCount: 0,
    };

    expect(() =>
      manager.handleDelta(
        {
          ...emptyNative,
          terminalOperations: [
            {
              root_key: key,
              rootDescriptor: currentRowTerminalDescriptor(nativeColumns),
              path: [],
              edit: {
                Insert: { index: 0, key: otherKey, value: [...terminalRowData(id, "root", 1)] },
              },
            },
          ],
        },
        transform,
        nativeColumns,
      ),
    ).toThrow(/root edit key/);
    expect(manager.all()).toEqual([]);

    manager.handleDelta(
      {
        __jazzNativeRowDelta: true,
        added: nativeAddedRecord(id, 0, "before", 1),
        removed: new Uint8Array(),
        updated: new Uint8Array(),
        addedCount: 1,
        removedCount: 0,
        updatedCount: 0,
      },
      transform,
      nativeColumns,
    );
    expect(() =>
      manager.handleDelta(
        {
          ...emptyNative,
          terminalOperations: [
            {
              root_key: key,
              path: [{ Collection: "missing" }],
              edit: {
                Insert: { index: 0, key: otherKey, value: [...terminalRowData(other, "child", 2)] },
              },
            },
          ],
        },
        transform,
        nativeColumns,
      ),
    ).toThrow(/unresolved path/);
    expect(manager.all()).toEqual([{ id, name: "before", count: 1 }]);
  });

  it("reduces keyed root and hidden child terminal inserts before publishing", () => {
    type IncludedRoot = { id: string; title: string; project: { id: string; name: string } | null };
    const manager = new SubscriptionManager<IncludedRoot>();
    const rootId = "00000000-0000-4000-8000-000000000001";
    const childId = "00000000-0000-4000-8000-000000000002";
    const rootKey = [10, ...uuidBytes(rootId)];
    const childKey = [10, ...uuidBytes(childId)];
    const childColumns: ColumnDescriptor[] = [
      { name: "name", column_type: { type: "Text" }, nullable: false },
    ];
    const rootColumns: ColumnDescriptor[] = [
      { name: "title", column_type: { type: "Text" }, nullable: false },
      {
        name: "project",
        column_type: { type: "Array", element: { type: "Row", columns: childColumns } },
        nullable: false,
      },
    ];
    const transformIncluded = (row: WasmRow): IncludedRoot => {
      const projects = row.values[1];
      const project = projects?.type === "Array" ? projects.value[0] : undefined;
      return {
        id: row.id,
        title: (row.values[0] as { type: "Text"; value: string }).value,
        project:
          project?.type === "Row"
            ? {
                id: project.value.id!,
                name: (project.value.values[0] as { type: "Text"; value: string }).value,
              }
            : null,
      };
    };

    const result = manager.handleDelta(
      {
        __jazzNativeRowDelta: true,
        added: new Uint8Array(),
        removed: new Uint8Array(),
        updated: new Uint8Array(),
        addedCount: 0,
        removedCount: 0,
        updatedCount: 0,
        terminalOperations: [
          {
            root_key: rootKey,
            path: [{ Collection: "__jazz_include_project" }],
            edit: {
              Insert: {
                index: 0,
                key: childKey,
                value: [...terminalTextChild(childId, "Announcements")],
              },
            },
          },
          {
            root_key: rootKey,
            rootDescriptor: currentRowTerminalDescriptor(rootColumns),
            path: [],
            edit: {
              Insert: {
                index: 0,
                key: rootKey,
                value: [...terminalRootWithEmptyChildren(rootId, "Watch subscription")],
              },
            },
          },
        ],
      },
      transformIncluded,
      rootColumns,
    );

    expect(result.all).toEqual([
      {
        id: rootId,
        title: "Watch subscription",
        project: { id: childId, name: "Announcements" },
      },
    ]);
    expect(result.delta).toEqual([{ kind: 0, id: rootId, index: 0, item: result.all![0] }]);

    // Producers canonicalize a weighted child replacement as Remove(old),
    // Insert(new); consumers apply that wire contract without inference.
    const replacement = manager.handleDelta(
      {
        __jazzNativeRowDelta: true,
        added: new Uint8Array(),
        removed: new Uint8Array(),
        updated: new Uint8Array(),
        addedCount: 0,
        removedCount: 0,
        updatedCount: 0,
        terminalOperations: [
          {
            root_key: rootKey,
            path: [{ Collection: "__jazz_include_project" }],
            edit: { Remove: { key: childKey } },
          },
          {
            root_key: rootKey,
            path: [{ Collection: "__jazz_include_project" }],
            edit: {
              Insert: {
                index: 0,
                key: childKey,
                value: [...terminalTextChild(childId, "Revised announcements")],
              },
            },
          },
        ],
      },
      transformIncluded,
      rootColumns,
    );
    expect(replacement.all?.[0]?.project?.name).toBe("Revised announcements");

    expect(() =>
      manager.handleDelta(
        {
          __jazzNativeRowDelta: true,
          added: new Uint8Array(),
          removed: new Uint8Array(),
          updated: new Uint8Array(),
          addedCount: 0,
          removedCount: 0,
          updatedCount: 0,
          terminalOperations: [
            {
              root_key: rootKey,
              path: [{ Collection: "__jazz_include_project" }],
              edit: {
                Update: {
                  key: childKey,
                  value: [...terminalTextChild(rootId, "Cross-root corruption")],
                },
              },
            },
          ],
        },
        transformIncluded,
        rootColumns,
      ),
    ).toThrow(/does not match addressed key/);
    expect(manager.all()).toEqual(replacement.all);

    const nestedUpdate = manager.handleDelta(
      {
        __jazzNativeRowDelta: true,
        added: new Uint8Array(),
        removed: new Uint8Array(),
        updated: new Uint8Array(),
        addedCount: 0,
        removedCount: 0,
        updatedCount: 0,
        terminalOperations: [
          {
            root_key: rootKey,
            path: [{ Collection: "__jazz_include_project" }, { Key: childKey }],
            edit: {
              Update: {
                key: childKey,
                value: [...terminalTextChild(childId, "Updated announcements")],
              },
            },
          },
        ],
      },
      transformIncluded,
      rootColumns,
    );
    expect(nestedUpdate.all?.[0]?.project?.name).toBe("Updated announcements");

    const removed = manager.handleDelta(
      {
        __jazzNativeRowDelta: true,
        added: new Uint8Array(),
        removed: new Uint8Array(),
        updated: new Uint8Array(),
        addedCount: 0,
        removedCount: 0,
        updatedCount: 0,
        terminalOperations: [
          {
            root_key: rootKey,
            path: [{ Collection: "__jazz_include_project" }],
            edit: { Remove: { key: childKey } },
          },
          {
            root_key: rootKey,
            rootDescriptor: currentRowTerminalDescriptor(rootColumns),
            path: [],
            edit: {
              Update: {
                key: rootKey,
                value: [...terminalRootWithEmptyChildren(rootId, "Updated subscription")],
              },
            },
          },
        ],
      },
      transformIncluded,
      rootColumns,
    );
    expect(removed.all).toEqual([{ id: rootId, title: "Updated subscription", project: null }]);
  });

  it("applies descendant terminal inserts to roots retained from native row frames", () => {
    type IncludedRoot = {
      id: string;
      title: string;
      children: Array<{ id: string; name: string }>;
    };
    const manager = new SubscriptionManager<IncludedRoot>();
    const rootId = "00000000-0000-4000-8000-000000000001";
    const childId = "00000000-0000-4000-8000-000000000002";
    const rootKey = [10, ...uuidBytes(rootId)];
    const childKey = [10, ...uuidBytes(childId)];
    const childColumns: ColumnDescriptor[] = [
      { name: "name", column_type: { type: "Text" }, nullable: false },
    ];
    const rootColumns: ColumnDescriptor[] = [
      { name: "title", column_type: { type: "Text" }, nullable: false },
      {
        name: "children",
        column_type: { type: "Array", element: { type: "Row", columns: childColumns } },
        nullable: false,
      },
    ];
    const transformIncluded = (row: WasmRow): IncludedRoot => {
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
    };

    manager.handleDelta(
      {
        __jazzNativeRowDelta: true,
        added: nativeAddedRawRecord(rootId, 0, nativeRootWithEmptyChildren("root")),
        removed: new Uint8Array(),
        updated: new Uint8Array(),
        addedCount: 1,
        removedCount: 0,
        updatedCount: 0,
      },
      transformIncluded,
      rootColumns,
    );

    const result = manager.handleDelta(
      {
        __jazzNativeRowDelta: true,
        added: new Uint8Array(),
        removed: new Uint8Array(),
        updated: new Uint8Array(),
        addedCount: 0,
        removedCount: 0,
        updatedCount: 0,
        terminalOperations: [
          {
            root_key: rootKey,
            path: [{ Collection: "children" }],
            edit: {
              Insert: {
                index: 0,
                key: childKey,
                value: [...terminalTextChild(childId, "child")],
              },
            },
          },
        ],
      },
      transformIncluded,
      rootColumns,
    );

    expect(result.all).toEqual([
      { id: rootId, title: "root", children: [{ id: childId, name: "child" }] },
    ]);
  });

  it("clears tracked state before applying native reset frames", () => {
    const manager = new SubscriptionManager<TestItem>();
    const first = "00000000-0000-4000-8000-000000000001";
    const second = "00000000-0000-4000-8000-000000000002";

    manager.handleDelta(
      {
        __jazzNativeRowDelta: true,
        added: nativeAddedRecord(first, 0, "first", 1),
        removed: new Uint8Array(),
        updated: new Uint8Array(),
        addedCount: 1,
        removedCount: 0,
        updatedCount: 0,
      },
      transform,
      nativeColumns,
    );

    const result = manager.handleDelta(
      {
        __jazzNativeRowDelta: true,
        reset: true,
        added: nativeAddedRecord(second, 0, "second", 2),
        removed: new Uint8Array(),
        updated: new Uint8Array(),
        addedCount: 1,
        removedCount: 0,
        updatedCount: 0,
      },
      transform,
      nativeColumns,
    );

    expect(result.reset).toBe(true);
    if (!result.reset) throw new Error("expected reset delta");
    expect(result.all).toEqual([{ id: second, name: "second", count: 2 }]);
    expect(manager.size).toBe(1);
  });

  it("tracks content updates", () => {
    const manager = new SubscriptionManager<TestItem>();

    const initial = manager.handleDelta(
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
    expect(reduceDeltas(initial, result)[0]!.count).toBe(15);
  });

  it("handles move-only updates without row payload", () => {
    const manager = new SubscriptionManager<TestItem>();

    const initial = manager.handleDelta(
      makeDelta([
        { kind: 0, id: "a", index: 0, row: makeRow("a", "A", 1) },
        { kind: 0, id: "b", index: 1, row: makeRow("b", "B", 2) },
        { kind: 0, id: "c", index: 2, row: makeRow("c", "C", 3) },
      ]),
      transform,
    );

    const result = manager.handleDelta(makeDelta([{ kind: 2, id: "c", index: 0 }]), transform);

    expect(result.delta).toEqual([{ kind: 2, id: "c", index: 0 }]);
    expect(reduceDeltas(initial, result).map((item) => item.id)).toEqual(["c", "a", "b"]);
  });

  it("tracks removals and shifts", () => {
    const manager = new SubscriptionManager<TestItem>();

    const initial = manager.handleDelta(
      makeDelta([
        { kind: 0, id: "1", index: 0, row: makeRow("1", "item1", 10) },
        { kind: 0, id: "2", index: 1, row: makeRow("2", "item2", 20) },
        { kind: 0, id: "3", index: 2, row: makeRow("3", "item3", 30) },
      ]),
      transform,
    );

    const result = manager.handleDelta(makeDelta([{ kind: 1, id: "2", index: 1 }]), transform);

    expect(result.delta).toEqual([{ kind: 1, id: "2", index: 1 }]);
    expect(reduceDeltas(initial, result).map((item) => item.id)).toEqual(["1", "3"]);
  });

  it("handles mixed remove + update + add in one delta", () => {
    const manager = new SubscriptionManager<TestItem>();

    const initial = manager.handleDelta(
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
    expect(reduceDeltas(initial, result).map((item) => item.id)).toEqual(["A", "D", "C", "E"]);
  });

  it("applies index positions correctly for mixed bulk updates", () => {
    const manager = new SubscriptionManager<TestItem>();

    const initial = manager.handleDelta(
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

    const current = reduceDeltas(initial, result);
    expect(current.map((item) => item.id)).toEqual(["C", "D", "A", "E"]);
    expect(current.find((item) => item.id === "D")?.name).toBe("D*");
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

    expect(reduceDeltas(result).map((item) => item.id)).toEqual(["2"]);
  });
});
