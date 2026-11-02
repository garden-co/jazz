import { expect, it } from "vitest";
import {
  isRandomNodeUuid,
  randomNodeUuidBytes,
  runtimeConnectionIncarnation,
} from "./runtime-entropy.js";

it("uses fresh entropy after restart and retries zero incarnations", () => {
  const source = (value: bigint) => {
    let calls = 0;
    return () => {
      const bytes = new Uint8Array(16);
      new DataView(bytes.buffer).setBigUint64(0, calls++ === 0 ? 0n : value, true);
      return bytes;
    };
  };
  expect(runtimeConnectionIncarnation(source(900n))).toBe(900n);
  expect(runtimeConnectionIncarnation(source(7n))).toBe(7n);
});

it("mints nodes as random version-4 UUIDs", () => {
  const node = randomNodeUuidBytes(new Uint8Array(16).fill(0xff));
  expect(node[6]).toBe(0x4f);
  expect(node[8]).toBe(0xbf);
  expect(isRandomNodeUuid(node)).toBe(true);
  expect(isRandomNodeUuid(randomNodeUuidBytes())).toBe(true);
  expect(isRandomNodeUuid(new Uint8Array(16).fill(0xff))).toBe(false);
});
