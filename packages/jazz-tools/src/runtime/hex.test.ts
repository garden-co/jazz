import { describe, expect, it } from "vitest";
import { bytesToHex, formatUuidAt } from "./hex.js";

// The previous per-byte formatting, kept as the reference the table must match.
function referenceHex(bytes: ArrayLike<number>): string {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

function referenceUuid(bytes: Uint8Array, offset: number): string {
  const hex = referenceHex(bytes.subarray(offset, offset + 16));
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function pseudoRandomBytes(length: number, seed: number): Uint8Array {
  const bytes = new Uint8Array(length);
  let state = seed >>> 0 || 1;
  for (let index = 0; index < length; index++) {
    state ^= state << 13;
    state ^= state >>> 17;
    state ^= state << 5;
    bytes[index] = state & 0xff;
  }
  return bytes;
}

describe("hex", () => {
  it("formats every byte value exactly like the per-byte reference", () => {
    const all = Uint8Array.from({ length: 256 }, (_, byte) => byte);
    expect(bytesToHex(all)).toBe(referenceHex(all));
    expect(bytesToHex(Array.from(all))).toBe(referenceHex(all));
    expect(bytesToHex(new Uint8Array())).toBe("");
  });

  it("formats random buffers and UUID windows identically to the reference", () => {
    for (let seed = 1; seed <= 200; seed++) {
      const bytes = pseudoRandomBytes(1 + (seed % 64) + 16, seed);
      expect(bytesToHex(bytes)).toBe(referenceHex(bytes));
      const offset = seed % (bytes.length - 15);
      expect(formatUuidAt(bytes, offset)).toBe(referenceUuid(bytes, offset));
    }
  });
});
