import { expect, it } from "vitest";
import { streamingBytes } from "./streaming-source.js";

it("rejects invalid upload bytes without waiting for an iterable's cleanup", async () => {
  let closed = false;
  const source: AsyncIterable<string> = {
    [Symbol.asyncIterator]() {
      return {
        async next() {
          return { done: false, value: "invalid bytes" };
        },
        return() {
          closed = true;
          return new Promise<IteratorResult<string>>(() => {});
        },
      };
    },
  };
  await expect(streamingBytes(source).next()).rejects.toThrow(
    "Bytea streams require Uint8Array chunks",
  );
  expect(closed).toBe(true);
}, 1000);

it("does not cancel an async upload source that reached its end", async () => {
  let position = 0;
  let closed = false;
  const source: AsyncIterable<Uint8Array> = {
    [Symbol.asyncIterator]() {
      return {
        async next() {
          return position++ === 0
            ? { done: false as const, value: new Uint8Array([2, 3, 5]) }
            : { done: true as const, value: undefined };
        },
        async return() {
          closed = true;
          return { done: true as const, value: undefined };
        },
      };
    },
  };
  const upload = streamingBytes(source);
  expect(await upload.next()).toEqual({ done: false, value: new Uint8Array([2, 3, 5]) });
  expect(await upload.next()).toEqual({ done: true, value: undefined });
  expect(closed).toBe(false);
});
