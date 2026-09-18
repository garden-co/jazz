import { Buffer } from "node:buffer";
import { expect, it } from "vitest";
import { decodeEnvelope, encodeEnvelope } from "./envelope.js";

it("uses the specified version-one bytes and rejects a different mechanism", () => {
  const mechanism = { id: "test", version: 1 };
  // JE2E, format 1, four ASCII ID bytes, big-endian mechanism version, payload.
  const vector = new Uint8Array([74, 69, 50, 69, 1, 4, 116, 101, 115, 116, 0, 0, 0, 1, 170]);
  expect(encodeEnvelope(mechanism, new Uint8Array([170]))).toEqual(vector);
  expect(decodeEnvelope(mechanism, vector)).toEqual(new Uint8Array([170]));
  expect(() => decodeEnvelope({ id: "other", version: 1 }, vector)).toThrow();
  expect(() => decodeEnvelope({ id: "test", version: 2 }, vector)).toThrow();
});

it("owns the decoded bytes when native Node supplies a Buffer", () => {
  const envelope = Buffer.from([74, 69, 50, 69, 1, 1, 97, 0, 0, 0, 1, 42]);
  const payload = decodeEnvelope({ id: "a", version: 1 }, envelope);
  envelope.fill(0);
  expect(Array.from(payload)).toEqual([42]);
});

it("rejects every truncated header and unknown format before returning payload", () => {
  const mechanism = { id: "test", version: 1 };
  const vector = new Uint8Array([74, 69, 50, 69, 1, 4, 116, 101, 115, 116, 0, 0, 0, 1]);
  for (let length = 0; length < vector.length; length++) {
    expect(() => decodeEnvelope(mechanism, vector.slice(0, length))).toThrow();
  }
  for (const [offset, value] of [
    [0, 0],
    [4, 2],
    [5, 0],
    [5, 65],
    [13, 0],
  ]) {
    const invalid = vector.slice();
    invalid[offset!] = value!;
    expect(() => decodeEnvelope(mechanism, invalid)).toThrow();
  }
});

it("accepts an empty payload and honours subarray offsets without sharing mutable storage", () => {
  const mechanism = { id: "a", version: 0xffffffff };
  const empty = new Uint8Array([74, 69, 50, 69, 1, 1, 97, 255, 255, 255, 255]);
  expect(encodeEnvelope(mechanism, new Uint8Array())).toEqual(empty);
  expect(decodeEnvelope(mechanism, empty)).toEqual(new Uint8Array());
  const backing = new Uint8Array([0, ...empty, 42, 0]);
  const payload = decodeEnvelope(mechanism, backing.subarray(1, backing.length - 1));
  backing.fill(0);
  expect(payload).toEqual(new Uint8Array([42]));
});

it("rejects invalid adapter identifiers and versions rather than normalising them", () => {
  for (const id of ["", "a".repeat(65), "TEST", "é", "test\n", "a/b"]) {
    expect(() => encodeEnvelope({ id, version: 1 }, new Uint8Array())).toThrow();
  }
  for (const version of [0, -1, 0.5, NaN, Infinity, 0x100000000]) {
    expect(() => encodeEnvelope({ id: "test", version }, new Uint8Array())).toThrow();
  }
});
