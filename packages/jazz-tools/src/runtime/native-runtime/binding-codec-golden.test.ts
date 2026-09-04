import { readFileSync } from "node:fs";

import { describe, expect, it } from "vitest";

import type { NativeTerminalOperation } from "../../drivers/types.js";
import { PostcardReader, PostcardWriter } from "./native-codec.js";
import {
  readNativeRelationSubscriptionSnapshot,
  readNativeSubscriptionDelta,
} from "./native-row-codec.js";
import { hasJazzNapiBuild, loadNapiModule } from "../testing/napi-runtime-test-utils.js";
import { hasJazzWasmBuild, loadWasmModuleForTest } from "../testing/wasm-runtime-test-utils.js";

type BindingCodecGoldenFixture = {
  format: string;
  relation_snapshots: Array<{ name: string; payload_hex: string }>;
  subscription_deltas: Array<{ name: string; payload_hex: string }>;
  terminal: {
    events: Array<{
      type: "delta";
      terminalOperations: NativeTerminalOperation[];
    }>;
    rejections: Array<Record<string, unknown>>;
  };
};

// Rust owns the fixture and both NAPI/WASM call the same production postcard
// encoder. This keeps byte-level representations and the actual TS reducer in
// one fast contract, rather than waiting for a browser integration failure.
describe("binding codec golden contract", () => {
  it.skipIf(!hasJazzNapiBuild() || !hasJazzWasmBuild())(
    "executes the Rust-owned corpus through both generated native artifacts",
    async () => {
      const expected = bindingCodecGoldenFixture();
      const napi = (await loadNapiModule()) as typeof import("jazz-napi") & {
        __testBindingCodecGoldenFixture(): string;
      };
      const wasm = (await loadWasmModuleForTest()) as {
        __testBindingCodecGoldenFixture(): string;
      };
      const corpus = [
        napi.__testBindingCodecGoldenFixture(),
        wasm.__testBindingCodecGoldenFixture(),
      ].map((encoded) => JSON.parse(encoded) as BindingCodecGoldenFixture);

      expect(corpus).toEqual([expected, expected]);
      for (const nativeFixture of corpus) {
        for (const relation of nativeFixture.relation_snapshots) {
          const snapshot = readNativeRelationSubscriptionSnapshot(
            new PostcardReader(hexToBytes(relation.payload_hex)),
          );
          expect(snapshot.rootCount).toBeGreaterThanOrEqual(0);
        }
        for (const delta of nativeFixture.subscription_deltas) {
          expect(
            readNativeSubscriptionDelta(new PostcardReader(hexToBytes(delta.payload_hex))),
          ).toBeDefined();
        }
      }
    },
  );

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
    expect(delta.addedIndices).toEqual([2]);
    expect(delta.updatedPreviousIndices).toEqual([4]);
    expect(delta.updatedIndices).toEqual([1]);
    expect(delta.removedIndices).toEqual([3]);
  });

  it("keeps the terminal JSON codec contract stable", () => {
    const fixture = bindingCodecGoldenFixture();
    const operationKinds = fixture.terminal.events.flatMap((event) =>
      event.terminalOperations.map((operation) => Object.keys(operation.edit)[0]),
    );
    expect(operationKinds).toEqual(["Insert", "Insert", "Update", "Move", "Remove"]);
    expect(
      fixture.terminal.events.flatMap((event) =>
        event.terminalOperations.map((operation) => operation.path),
      ),
    ).toEqual(Array.from({ length: 5 }, () => [{ Collection: "children" }]));
    expect(fixture.terminal.rejections).toEqual([
      { type: "UnsupportedShapeCapability", detail: "unsupported descendant terminal shape" },
      { type: "ServerFailure", code: "TableNotFound" },
    ]);
  });

  it("rejects trailing bytes after a complete binding payload", () => {
    const fixture = bindingCodecGoldenFixture();
    const relation = relationCase(fixture, "empty_root_count_zero");
    const relationWithSuffix = Uint8Array.from([...hexToBytes(relation.payload_hex), 0]);
    expect(() =>
      readNativeRelationSubscriptionSnapshot(new PostcardReader(relationWithSuffix)),
    ).toThrow("relation snapshot has trailing postcard bytes");

    const delta = fixture.subscription_deltas.find(
      (candidate) => candidate.name === "added_updated_removed_with_v1_and_v2_occurrence_keys",
    )!;
    const deltaWithSuffix = Uint8Array.from([...hexToBytes(delta.payload_hex), 0]);
    expect(() => readNativeSubscriptionDelta(new PostcardReader(deltaWithSuffix))).toThrow(
      "subscription delta has trailing postcard bytes",
    );

    expect(() => new PostcardReader(Uint8Array.from([1, 0xff])).string()).toThrow();
  });

  it("rejects alternate and unsafe number u64 spellings while retaining canonical full-width bigint", () => {
    const maxSafe = Number.MAX_SAFE_INTEGER;
    for (const rootCount of [maxSafe - 1, maxSafe]) {
      expect(
        readNativeRelationSubscriptionSnapshot(
          new PostcardReader(encodeEmptyRelationSnapshot(rootCount)),
        ).rootCount,
      ).toBe(rootCount);
    }

    expect(() =>
      readNativeRelationSubscriptionSnapshot(
        new PostcardReader(encodeEmptyRelationSnapshot(BigInt(maxSafe) + 1n)),
      ),
    ).toThrow("postcard u64 exceeds Number.MAX_SAFE_INTEGER");

    const maxU64 = (1n << 64n) - 1n;
    const maxU64Writer = new PostcardWriter();
    maxU64Writer.u64(maxU64);
    const maxU64Bytes = maxU64Writer.finish();
    expect(maxU64Bytes).toEqual(Uint8Array.from([...Array(9).fill(0xff), 0x01]));
    const maxU64Reader = new PostcardReader(maxU64Bytes);
    expect(maxU64Reader.u64BigInt()).toBe(maxU64);
    expect(maxU64Reader.done()).toBe(true);

    expect(() => new PostcardReader(Uint8Array.from([0x82, 0x00])).u64()).toThrow(
      "postcard u64 is not minimally encoded",
    );
    expect(() =>
      new PostcardReader(Uint8Array.from([...Array(9).fill(0x80), 0x02])).u64BigInt(),
    ).toThrow("postcard u64 overflow");
    expect(() =>
      new PostcardReader(Uint8Array.from([...Array(9).fill(0x80), 0x00])).u64BigInt(),
    ).toThrow("postcard u64 is not minimally encoded");
  });

  it("writes only exact signed i64 values and round-trips their ZigZag boundaries", () => {
    const minI64 = -(1n << 63n);
    const maxI64 = (1n << 63n) - 1n;
    for (const value of [minI64, maxI64, -Number.MAX_SAFE_INTEGER, Number.MAX_SAFE_INTEGER]) {
      const writer = new PostcardWriter();
      writer.i64(value);
      const reader = new PostcardReader(writer.finish());
      expect(reader.i64()).toBe(BigInt(value));
      expect(reader.done()).toBe(true);
    }

    expect(() => new PostcardWriter().i64(Number.MAX_SAFE_INTEGER + 1)).toThrow(
      "i64 must be a safe integer when passed as a number",
    );
    expect(() => new PostcardWriter().i64(-(Number.MAX_SAFE_INTEGER + 1))).toThrow(
      "i64 must be a safe integer when passed as a number",
    );
    expect(() => new PostcardWriter().i64(minI64 - 1n)).toThrow(
      "i64 must be a signed 64-bit integer",
    );
    expect(() => new PostcardWriter().i64(maxI64 + 1n)).toThrow(
      "i64 must be a signed 64-bit integer",
    );

    const u32Writer = new PostcardWriter();
    u32Writer.u32Le(0xffff_ffff);
    expect(u32Writer.finish()).toEqual(Uint8Array.of(0xff, 0xff, 0xff, 0xff));
    expect(() => new PostcardWriter().u32Le(-1)).toThrow(
      "u32Le must be an unsigned 32-bit integer",
    );
    expect(() => new PostcardWriter().u32Le(0x1_0000_0000)).toThrow(
      "u32Le must be an unsigned 32-bit integer",
    );
  });
});

function encodeEmptyRelationSnapshot(rootCount: number | bigint): Uint8Array {
  const writer = new PostcardWriter();
  writer.u64(rootCount);
  writer.vec(() => {}, 0);
  return writer.finish();
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
