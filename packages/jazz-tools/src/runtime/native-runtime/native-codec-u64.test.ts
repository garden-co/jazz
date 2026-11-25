import { describe, expect, it } from "vitest";
import { PostcardReader, PostcardWriter } from "./native-codec.js";

/** What `u64()` read before its Number fast path: the canonical BigInt read, range-checked. */
function referenceU64(bytes: Uint8Array): { value?: number; error?: string; next?: number } {
  const reader = new PostcardReader(bytes);
  try {
    const value = reader.u64BigInt();
    if (value > BigInt(Number.MAX_SAFE_INTEGER)) {
      return { error: `postcard u64 exceeds Number.MAX_SAFE_INTEGER: ${value}` };
    }
    return { value: Number(value), next: nextByte(reader) };
  } catch (error) {
    return { error: (error as Error).message };
  }
}

function fastU64(bytes: Uint8Array): { value?: number; error?: string; next?: number } {
  const reader = new PostcardReader(bytes);
  try {
    const value = reader.u64();
    return { value, next: nextByte(reader) };
  } catch (error) {
    return { error: (error as Error).message };
  }
}

/** Where the read stopped: both paths must consume the same bytes. */
function nextByte(reader: PostcardReader): number {
  return (reader as unknown as { offset: number }).offset;
}

describe("PostcardReader.u64", () => {
  it("reads every encoding exactly as the canonical BigInt path does", () => {
    let seed = 0x2545f491;
    const random = () => {
      seed ^= seed << 13;
      seed ^= seed >>> 17;
      seed ^= seed << 5;
      return (seed >>> 0) / 0x1_0000_0000;
    };
    for (let round = 0; round < 50_000; round += 1) {
      const length = 1 + Math.floor(random() * 12);
      const bytes = new Uint8Array(length);
      const continued = Math.floor(random() * 12);
      for (let index = 0; index < length; index += 1) {
        const payload = random() < 0.1 ? 0 : Math.floor(random() * 128);
        bytes[index] = index < continued ? payload | 0x80 : payload;
      }
      expect(fastU64(bytes), `bytes ${Array.from(bytes)}`).toEqual(referenceU64(bytes));
    }
  });

  it("round-trips what the writer encodes", () => {
    for (const value of [
      0,
      1,
      127,
      128,
      16_383,
      16_384,
      2 ** 49 - 1,
      2 ** 49,
      Number.MAX_SAFE_INTEGER,
    ]) {
      const writer = new PostcardWriter();
      writer.u64(value);
      expect(new PostcardReader(writer.finish()).u64()).toBe(value);
    }
  });
});
