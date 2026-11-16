import { describe, expect, it } from "vitest";
import { bytesToHex, formatUuidAt, parseUuidBytes } from "./hex.js";

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

  // Rust's `Uuid::parse_str` reads hyphenated `8-4-4-4-12` and 32 plain hex
  // digits; dashes anywhere else used to be ignored here.
  it("parses only hyphenated or plain UUID text", () => {
    const canonical = "0cae56e7-0f54-421c-ba8b-54fcbfec8dd2";
    const bytes = parseUuidBytes(canonical);
    expect(bytes && formatUuidAt(bytes, 0)).toBe(canonical);
    expect(parseUuidBytes(canonical.replaceAll("-", ""))).toEqual(bytes);
    expect(parseUuidBytes(canonical.toUpperCase())).toEqual(bytes);
    for (const malformed of [
      "0cae56e70f54421cba8b54fcbfec8dd2-",
      "-0cae56e70f54421cba8b54fcbfec8dd2",
      "0cae-56e7-0f54-421c-ba8b-54fcbfec8dd2",
      "0cae56e7-0f54421c-ba8b-54fcbfec8dd2",
      "0c-ae-56-e7-0f-54-42-1c-ba-8b-54-fc-bf-ec-8d-d2",
      "0cae56e7-0f54-421c-ba8b-54fcbfec8dd",
    ]) {
      expect(parseUuidBytes(malformed), malformed).toBeUndefined();
    }
  });
});
