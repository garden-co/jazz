import { readFileSync } from "node:fs";

import { describe, expect, it } from "vitest";

import type {
  ColumnDescriptor,
  NativeRowDelta,
  NativeTerminalRootLayout,
  Value,
} from "../../drivers/types.js";
import { SubscriptionManager } from "../subscription-manager.js";
import { PostcardReader } from "./native-codec.js";
import {
  readNativeRelationSubscriptionSnapshot,
  readNativeSubscriptionDelta,
} from "./native-row-codec.js";

type BindingCodecGoldenFixture = {
  format: string;
  relation_snapshots: Array<{ name: string; payload_hex: string }>;
  subscription_deltas: Array<{ name: string; payload_hex: string }>;
  terminal: {
    events: Array<{
      type: "delta";
      terminalLayouts: NativeTerminalRootLayout[];
      terminalOperations: NonNullable<NativeRowDelta["terminalOperations"]>;
    }>;
    rejections: Array<Record<string, unknown>>;
  };
};

const columns: ColumnDescriptor[] = [
  { name: "title", column_type: { type: "Text" }, nullable: false },
];

// Rust owns the fixture and both NAPI/WASM call the same production postcard
// encoder. This keeps byte-level representations and the actual TS reducer in
// one fast contract, rather than waiting for a browser integration failure.
describe("binding codec golden contract", () => {
  it("decodes empty, adjacent, nonadjacent, and deleted-row relation snapshots", () => {
    const fixture = bindingCodecGoldenFixture();
    expect(fixture.format).toBe("jazz-binding-codec-golden-v1");
    const empty = relationCase(fixture, "empty_root_count_zero");
    expect(
      readNativeRelationSubscriptionSnapshot(new PostcardReader(hexToBytes(empty.payload_hex))),
    ).toEqual({
      rootCount: 0,
      rows: [],
    });

    const batching = relationCase(fixture, "adjacent_and_nonadjacent_batches_with_deleted_row");
    const snapshot = readNativeRelationSubscriptionSnapshot(
      new PostcardReader(hexToBytes(batching.payload_hex)),
    );
    expect(snapshot.rootCount).toBe(4);
    expect(snapshot.rows.map((batch) => [batch.table, batch.rows.length])).toEqual([
      ["todos", 2],
      ["notes", 1],
      ["todos", 1],
    ]);
    expect(snapshot.rows[2]!.rows[0]!.deleted).toBe(true);
    expect(bytesToHex(snapshot.rows[0]!.rows[0]!.rowId)).toBe("11".repeat(16));
    expect(bytesToHex(snapshot.rows[1]!.rows[0]!.rowId)).toBe("21".repeat(16));
  });

  it("keeps added, updated, removed, and both ResultKey wire versions aligned", () => {
    const fixture = bindingCodecGoldenFixture();
    const deltaCase = fixture.subscription_deltas.find(
      (candidate) => candidate.name === "added_updated_removed_with_v1_and_v2_occurrence_keys",
    )!;
    const delta = readNativeSubscriptionDelta(
      new PostcardReader(hexToBytes(deltaCase.payload_hex)),
    );

    expect(delta.added.map((batch) => batch.table)).toEqual(["todos"]);
    expect(delta.updated.map((batch) => batch.table)).toEqual(["notes"]);
    expect(delta.removed).toEqual([{ table: "todos", rowId: expect.any(Uint8Array) }]);
    expect(delta.addedOccurrenceKeys.map((key) => key[0])).toEqual([1]);
    expect(delta.updatedOccurrenceKeys.map((key) => key[0])).toEqual([2]);
    expect(delta.removedOccurrenceKeys.map((key) => key[0])).toEqual([2]);
  });

  it("applies actual terminal publication, layout reuse, and edits through SubscriptionManager", () => {
    const fixture = bindingCodecGoldenFixture();
    const manager = new SubscriptionManager<{ id: string; title: string }>();

    const first = manager.handleDelta(
      nativeTerminalDelta(fixture.terminal.events[0]!),
      terminalRow,
      columns,
    );
    expect(first.all).toEqual([{ id: "11111111-1111-1111-1111-111111111111", title: "first" }]);

    const reusedLayout = fixture.terminal.events[2]!;
    expect(reusedLayout.terminalLayouts).toEqual([]);
    const updated = manager.handleDelta(nativeTerminalDelta(reusedLayout), terminalRow, columns);
    expect(updated.all).toEqual([{ id: "11111111-1111-1111-1111-111111111111", title: "updated" }]);

    const logicalManager = new SubscriptionManager<{ id: string; title: string }>();
    const logicalInserted = logicalManager.handleDelta(
      nativeTerminalDelta(fixture.terminal.events[1]!),
      terminalRow,
      columns,
    );
    expect(logicalInserted.all).toEqual([
      { id: "21212121-2121-2121-2121-212121212121", title: "note" },
    ]);
    const logicalRemoved = logicalManager.handleDelta(
      nativeTerminalDelta(fixture.terminal.events[3]!),
      terminalRow,
      columns,
    );
    expect(logicalRemoved.all).toEqual([]);

    const operationKinds = fixture.terminal.events.flatMap((event) =>
      event.terminalOperations.map((operation) => Object.keys(operation.edit)[0]),
    );
    expect(operationKinds).toEqual(["Insert", "Insert", "Update", "Move", "Remove"]);
    expect(fixture.terminal.rejections).toEqual([
      { type: "UnsupportedShapeCapability", detail: "terminal layout missing" },
      { type: "ServerFailure", code: "TableNotFound" },
    ]);
  });

  it("fails closed when a planted CurrentRow or Logical descriptor byte drifts", () => {
    const fixture = bindingCodecGoldenFixture();
    for (const source of [fixture.terminal.events[0]!, fixture.terminal.events[1]!]) {
      const event = structuredClone(source);
      const descriptor = event.terminalLayouts[0]!.rootDescriptor;
      descriptor[descriptor.length - 1] = 4; // String -> I32, still a valid descriptor.
      const manager = new SubscriptionManager<{ id: string; title: string }>();
      expect(() => manager.handleDelta(nativeTerminalDelta(event), terminalRow, columns)).toThrow(
        /terminal root layout/,
      );
    }
  });
});

function terminalRow(row: { id: string; values: Value[] }): { id: string; title: string } {
  const title = row.values[0];
  if (title?.type !== "Text") throw new Error("golden terminal title did not decode as text");
  return { id: row.id, title: title.value };
}

function nativeTerminalDelta(
  event: BindingCodecGoldenFixture["terminal"]["events"][number],
): NativeRowDelta {
  return {
    __jazzNativeRowDelta: true,
    added: new Uint8Array(),
    updated: new Uint8Array(),
    removed: new Uint8Array(),
    addedCount: 0,
    updatedCount: 0,
    removedCount: 0,
    terminalLayouts: event.terminalLayouts,
    terminalOperations: event.terminalOperations,
  };
}

function bindingCodecGoldenFixture(): BindingCodecGoldenFixture {
  return JSON.parse(
    readFileSync(
      new URL("../../../../../crates/jazz/fixtures/binding_codec_golden.json", import.meta.url),
      "utf8",
    ),
  ) as BindingCodecGoldenFixture;
}

function relationCase(
  fixture: BindingCodecGoldenFixture,
  name: string,
): { name: string; payload_hex: string } {
  const testCase = fixture.relation_snapshots.find((candidate) => candidate.name === name);
  if (!testCase) throw new Error(`missing ${name} binding codec fixture`);
  return testCase;
}

function hexToBytes(hex: string): Uint8Array {
  const bytes = new Uint8Array(hex.length / 2);
  for (let index = 0; index < bytes.length; index += 1) {
    bytes[index] = Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16);
  }
  return bytes;
}

function bytesToHex(bytes: Uint8Array): string {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}
