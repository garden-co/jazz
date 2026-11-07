import { readFileSync } from "node:fs";
import { expect, it } from "vitest";
import { PostcardReader, PostcardWriter, readNativeSubscriptionDelta } from "./native-codec.js";

function encodeSubscriptionDelta(delta: {
  added: unknown[];
  updated: unknown[];
  addedOccurrenceKeys?: Uint8Array[];
  updatedOccurrenceKeys?: Uint8Array[];
  removed: Array<{ table: string; rowId: Uint8Array }>;
  removedOccurrenceKeys?: Uint8Array[];
}): Uint8Array {
  const writer = new PostcardWriter();
  writer.vec(() => undefined, 0);
  writer.vec(() => undefined, 0);
  writer.vec((entry, index) => {
    const source = delta.removed[index]!;
    entry.string(source.table);
    entry.bytes(source.rowId);
  }, delta.removed.length);
  const addedKeys = delta.addedOccurrenceKeys ?? [];
  const updatedKeys = delta.updatedOccurrenceKeys ?? [];
  const removedKeys = delta.removedOccurrenceKeys ?? [];
  writer.vec((key, index) => key.bytes(addedKeys[index]!), addedKeys.length);
  writer.vec((key, index) => key.bytes(updatedKeys[index]!), updatedKeys.length);
  writer.vec((key, index) => key.bytes(removedKeys[index]!), removedKeys.length);
  writer.vec((indexWriter, index) => indexWriter.u64(index), addedKeys.length);
  writer.vec((indexWriter, index) => indexWriter.u64(index), updatedKeys.length);
  writer.vec((indexWriter, index) => indexWriter.u64(index), updatedKeys.length);
  writer.vec((indexWriter, index) => indexWriter.u64(index), removedKeys.length);
  writer.vec((row) => row.vec(() => undefined, 0), updatedKeys.length); // updated splices
  return writer.finish();
}

it("preserves typed occurrence keys in the native subscription wire sidecar", () => {
  const typedKey = (label: string) => {
    const labelBytes = new TextEncoder().encode(label);
    const key = new Uint8Array(1 + 16 + 4 + 16 + 4 + 4 + 4 + labelBytes.length + 2);
    key[0] = 2;
    key.fill(1, 1, 17);
    new DataView(key.buffer).setUint32(17, 1);
    key.fill(2, 21, 37);
    new DataView(key.buffer).setUint32(37, 1);
    new DataView(key.buffer).setUint32(41, 0);
    new DataView(key.buffer).setUint32(45, labelBytes.length);
    key.set(labelBytes, 49);
    return key;
  };
  const direct = typedKey("direct");
  const inherited = typedKey("inherited");
  const encoded = encodeSubscriptionDelta({
    added: [],
    updated: [],
    addedOccurrenceKeys: [],
    updatedOccurrenceKeys: [],
    removed: [
      { table: "todos", rowId: new Uint8Array(16).fill(1) },
      { table: "todos", rowId: new Uint8Array(16).fill(1) },
    ],
    removedOccurrenceKeys: [direct, inherited],
  });
  const decoded = readNativeSubscriptionDelta(new PostcardReader(encoded));
  expect(decoded.removedOccurrenceKeys).toEqual([direct, inherited]);
  expect(decoded.removedOccurrenceKeys[0]).not.toEqual(decoded.removedOccurrenceKeys[1]);
});

it("rejects malformed or misaligned subscription occurrence sidecars", () => {
  const missing = encodeSubscriptionDelta({
    added: [],
    updated: [],
    removed: [{ table: "todos", rowId: new Uint8Array(16) }],
    removedOccurrenceKeys: [],
  });
  expect(() => readNativeSubscriptionDelta(new PostcardReader(missing))).toThrow(
    "sidecar length mismatch",
  );

  const malformed = encodeSubscriptionDelta({
    added: [],
    updated: [],
    removed: [{ table: "todos", rowId: new Uint8Array(16) }],
    removedOccurrenceKeys: [Uint8Array.from([2, 0])],
  });
  expect(() => readNativeSubscriptionDelta(new PostcardReader(malformed))).toThrow(
    "malformed ResultKey",
  );

  const zeroArm = new Uint8Array(43);
  zeroArm[0] = 2;
  zeroArm.fill(1, 1, 17);
  new DataView(zeroArm.buffer).setUint32(17, 1);
  zeroArm.fill(2, 21, 37);
  const aliasedV1 = encodeSubscriptionDelta({
    added: [],
    updated: [],
    removed: [{ table: "todos", rowId: new Uint8Array(16) }],
    removedOccurrenceKeys: [zeroArm],
  });
  expect(() => readNativeSubscriptionDelta(new PostcardReader(aliasedV1))).not.toThrow();

  const invalidUtf8 = new Uint8Array(51);
  invalidUtf8[0] = 2;
  invalidUtf8.fill(1, 1, 17);
  new DataView(invalidUtf8.buffer).setUint32(17, 1);
  invalidUtf8.fill(2, 21, 37);
  new DataView(invalidUtf8.buffer).setUint32(37, 1);
  new DataView(invalidUtf8.buffer).setUint32(41, 0);
  new DataView(invalidUtf8.buffer).setUint32(45, 1);
  invalidUtf8[49] = 0xff;
  const malformedLabel = encodeSubscriptionDelta({
    added: [],
    updated: [],
    removed: [{ table: "todos", rowId: new Uint8Array(16) }],
    removedOccurrenceKeys: [invalidUtf8],
  });
  expect(() => readNativeSubscriptionDelta(new PostcardReader(malformedLabel))).toThrow(
    "malformed ResultKey",
  );
});

// Alice's Rust-produced version key must survive the native binding unchanged,
// including HLC values above JavaScript's exact integer range.
it("preserves versioned result keys and rejects malformed history sidecars", () => {
  const fixture = JSON.parse(
    readFileSync(
      new URL("../../../../../crates/jazz/fixtures/result-key.json", import.meta.url),
      "utf8",
    ),
  ) as { key: number[]; joined_key: number[] };
  const key = Uint8Array.from(fixture.key);
  const joined = Uint8Array.from(fixture.joined_key);
  const earlier = key.slice();
  earlier[33] = 254;
  const decode = (keys: Uint8Array[]) =>
    readNativeSubscriptionDelta(
      new PostcardReader(
        encodeSubscriptionDelta({
          added: [],
          updated: [],
          removed: keys.map(() => ({ table: "history.todos", rowId: key.slice(1, 17) })),
          removedOccurrenceKeys: keys,
        }),
      ),
    );
  expect(decode([joined]).removedOccurrenceKeys).toEqual([joined]);
  expect(decode([key, earlier]).removedOccurrenceKeys).toEqual([key, earlier]);
  const oldCurrentKey = Uint8Array.from([1, ...key.slice(1, 25)]);
  const oldHistoryKey = Uint8Array.from([...key.slice(0, 25), ...key.slice(26)]);
  const invalidFlag = key.slice();
  invalidFlag[25] = 2;
  for (const invalid of [oldCurrentKey, oldHistoryKey, invalidFlag]) {
    expect(() => decode([invalid])).toThrow("malformed ResultKey");
  }
  for (let length = 0; length < key.length; length++) {
    expect(() => decode([key.slice(0, length)])).toThrow("malformed ResultKey");
  }
  expect(() => decode([Uint8Array.from([...key, 0])])).toThrow("malformed ResultKey");
  for (const offset of [17, 21]) {
    const invalid = key.slice();
    invalid[offset] = 1;
    expect(() => decode([invalid])).toThrow("malformed ResultKey");
  }
});
