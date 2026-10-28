import { expect, it, vi } from "vitest";
import { streamingChunks } from "./streaming-source.js";

it("releases a completed readable stream without cancelling its source", async () => {
  const cancel = vi.fn();
  const source = new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(new Uint8Array([1, 2, 3]));
      controller.close();
    },
    cancel,
  });
  const chunks = [];
  for await (const chunk of streamingChunks(source)) chunks.push(chunk);
  expect(chunks).toEqual([new Uint8Array([1, 2, 3])]);
  expect(source.locked).toBe(false);
  expect(cancel).not.toHaveBeenCalled();
});

it("does not return an async iterator that reached its end", async () => {
  const cleanup = vi.fn(async () => ({ done: true as const, value: undefined }));
  let position = 0;
  const source: AsyncIterable<string> = {
    [Symbol.asyncIterator]() {
      return {
        async next() {
          return position++ === 0
            ? { done: false as const, value: "complete" }
            : { done: true as const, value: undefined };
        },
        return: cleanup,
      };
    },
  };
  const chunks = [];
  for await (const chunk of streamingChunks(source)) chunks.push(chunk);
  expect(chunks).toEqual(["complete"]);
  expect(cleanup).not.toHaveBeenCalled();
});

it.each(["throws", "rejects"] as const)(
  "closes a failed iterator and preserves its read error when cleanup %s",
  async (mode) => {
    const readError = new Error("source read failed");
    const cleanupError = new Error("source cleanup failed");
    const cleanup = vi.fn(() => {
      if (mode === "throws") throw cleanupError;
      return Promise.reject(cleanupError);
    });
    const source: AsyncIterable<Uint8Array> = {
      [Symbol.asyncIterator]() {
        return {
          async next() {
            throw readError;
          },
          return: cleanup,
        };
      },
    };
    await expect(streamingChunks(source).next()).rejects.toBe(readError);
    expect(cleanup).toHaveBeenCalledTimes(1);
  },
);

it("preserves a consumer error when an iterator has no return method", async () => {
  const failure = new Error("consumer failed");
  const source: AsyncIterable<string> = {
    [Symbol.asyncIterator]() {
      return {
        async next() {
          return { done: false as const, value: "one chunk" };
        },
      };
    },
  };
  const consume = async () => {
    for await (const chunk of streamingChunks(source)) {
      expect(chunk).toBe("one chunk");
      throw failure;
    }
  };
  await expect(consume()).rejects.toBe(failure);
});

it("releases an errored readable stream without masking its read failure", async () => {
  const failure = new Error("source read failed");
  const source = new ReadableStream<Uint8Array>({
    start(controller) {
      controller.error(failure);
    },
  });
  await expect(streamingChunks(source).next()).rejects.toBe(failure);
  expect(source.locked).toBe(false);
});
