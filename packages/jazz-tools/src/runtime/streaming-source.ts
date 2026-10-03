import type { StreamingValueSource } from "./client.js";

/** Cancel unfinished browser streams and close async generators on failure. */
export async function* streamingChunks(
  source: StreamingValueSource,
): AsyncGenerator<Uint8Array | string> {
  const readable = source as ReadableStream<Uint8Array | string>;
  if (typeof readable.getReader !== "function") {
    const iterator = (source as AsyncIterable<Uint8Array | string>)[Symbol.asyncIterator]();
    let completed = false;
    try {
      while (true) {
        const result = await iterator.next();
        if (result.done) {
          completed = true;
          return;
        }
        yield result.value;
      }
    } finally {
      if (!completed) {
        try {
          void Promise.resolve(iterator.return?.()).catch(() => undefined);
        } catch {
          // Cleanup cannot replace the source/consumer failure or delay publication abort.
        }
      }
    }
  }
  const reader = readable.getReader();
  let completed = false;
  try {
    while (true) {
      const result = await reader.read();
      if (result.done) {
        completed = true;
        return;
      }
      yield result.value;
    }
  } finally {
    if (!completed) {
      // Application cancellation must not delay upload abort or transaction cleanup.
      try {
        void Promise.resolve(reader.cancel()).catch(() => undefined);
      } catch {
        // Preserve the source/consumer failure even for a synchronous cancellation failure.
      }
    }
    reader.releaseLock();
  }
}

export async function* streamingBytes(source: StreamingValueSource): AsyncGenerator<Uint8Array> {
  for await (const chunk of streamingChunks(source)) {
    if (!(chunk instanceof Uint8Array)) throw new Error("Bytea streams require Uint8Array chunks");
    yield chunk;
  }
}
