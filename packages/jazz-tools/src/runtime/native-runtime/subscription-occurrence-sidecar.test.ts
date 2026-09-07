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
  return writer.finish();
}

it("preserves typed occurrence keys in the native subscription wire sidecar", () => {
  const typedKey = (label: string) => {
    const labelBytes = new TextEncoder().encode(label);
    const key = new Uint8Array(1 + 16 + 4 + 16 + 4 + 4 + 4 + labelBytes.length);
    key[0] = 1;
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
    "malformed ResultKey v1",
  );

  const zeroArm = new Uint8Array(41);
  zeroArm[0] = 1;
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

  const invalidUtf8 = new Uint8Array(50);
  invalidUtf8[0] = 1;
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
    "malformed ResultKey v1",
  );
});
