import { encodeEnvelope } from "./envelope.js";
import { frameCryptoRecord } from "./record-frame.js";
import type { LargeValueCipher } from "./types.js";

/** The platform owns crypto state; framing and stream consumption stay shared. */
export interface SodiumStreamPrimitives {
  hash(key: Uint8Array, input: Uint8Array): Uint8Array;
  encrypt(key: Uint8Array): {
    header: Uint8Array;
    push(message: Uint8Array, context: Uint8Array, final: boolean): Uint8Array;
    dispose(): void;
  };
  decrypt(
    key: Uint8Array,
    header: Uint8Array,
  ): {
    pull(ciphertext: Uint8Array, context: Uint8Array): { message: Uint8Array; final: boolean };
    dispose(): void;
  };
}

const MECHANISM = { id: "jazz.sodium.stream", version: 1 } as const;
const BLOCK = 65_536;
const OVERHEAD = 17;

function concat(first: Uint8Array, second: Uint8Array) {
  const bytes = new Uint8Array(first.length + second.length);
  bytes.set(first);
  bytes.set(second, first.length);
  return bytes;
}

function reader(source: AsyncIterable<Uint8Array>, signal?: AbortSignal) {
  const iterator = source[Symbol.asyncIterator]();
  let chunk: Uint8Array = new Uint8Array();
  let offset = 0;
  let ended = false;
  async function wait<T>(pending: Promise<T>): Promise<T> {
    if (!signal) return pending;
    let abort: () => void = () => {};
    try {
      return await new Promise<T>((resolve, reject) => {
        abort = () => reject(signal.reason);
        pending.then(resolve, reject);
        signal.addEventListener("abort", abort, { once: true });
        if (signal.aborted) abort();
      });
    } finally {
      signal.removeEventListener("abort", abort);
    }
  }
  return {
    async read(length: number): Promise<Uint8Array | undefined> {
      const output = new Uint8Array(length);
      let written = 0;
      while (written < length) {
        signal?.throwIfAborted();
        if (offset === chunk.length) {
          if (ended) break;
          const next = await wait(iterator.next());
          signal?.throwIfAborted();
          if (next.done) {
            ended = true;
            break;
          }
          if (!(next.value instanceof Uint8Array)) throw new Error("Invalid E2EE stream input");
          chunk = next.value;
          offset = 0;
          continue;
        }
        const amount = Math.min(length - written, chunk.length - offset);
        output.set(chunk.subarray(offset, offset + amount), written);
        offset += amount;
        written += amount;
      }
      return written === 0 ? undefined : output.subarray(0, written);
    },
    async close(failed: boolean) {
      chunk = new Uint8Array();
      if (ended || !iterator.return) return;
      try {
        const cleanup = iterator.return();
        // Cleanup must not replace or indefinitely delay an established failure.
        if (failed) cleanup.catch(() => {});
        else await wait(cleanup);
      } catch (error) {
        if (!failed) throw error;
      }
    },
  };
}

/** Bounded records, independent of upstream transport chunk boundaries. */
export function createSodiumLargeValueCipher(sodium: SodiumStreamPrimitives): LargeValueCipher {
  const header = encodeEnvelope(MECHANISM, new Uint8Array());
  function prepare(key: Uint8Array, context: Uint8Array) {
    if (!(key instanceof Uint8Array) || key.length !== 32 || !(context instanceof Uint8Array))
      throw new Error("Invalid E2EE crypto input");
    const aad = frameCryptoRecord([header, context]);
    return { aad, derived: sodium.hash(key, aad) };
  }
  function record(ciphertext: Uint8Array) {
    const length = new Uint8Array(4);
    new DataView(length.buffer).setUint32(0, ciphertext.length, false);
    return concat(length, ciphertext);
  }
  return {
    mechanism: MECHANISM,
    async *encrypt(key, context, source, options) {
      options?.signal?.throwIfAborted();
      const { aad, derived } = prepare(key, context);
      let state: ReturnType<SodiumStreamPrimitives["encrypt"]> | undefined;
      let input: ReturnType<typeof reader> | undefined;
      let failed = false;
      try {
        state = sodium.encrypt(derived);
        derived.fill(0);
        input = reader(source, options?.signal);
        yield concat(header, state.header);
        while (true) {
          const plaintext = await input.read(BLOCK);
          if (!plaintext) break;
          let encrypted: Uint8Array;
          try {
            encrypted = record(state.push(plaintext, aad, false));
          } finally {
            plaintext.fill(0);
          }
          yield encrypted;
        }
        options?.signal?.throwIfAborted();
        yield record(state.push(new Uint8Array(), aad, true));
      } catch (error) {
        failed = true;
        throw error;
      } finally {
        derived.fill(0);
        state?.dispose();
        await input?.close(failed);
      }
    },
    async *decrypt(key, context, source, options) {
      options?.signal?.throwIfAborted();
      const { aad, derived } = prepare(key, context);
      let state: ReturnType<SodiumStreamPrimitives["decrypt"]> | undefined;
      let input: ReturnType<typeof reader> | undefined;
      let failed = false;
      try {
        input = reader(source, options?.signal);
        const prefix = await input.read(header.length + 24);
        if (
          !prefix ||
          prefix.length !== header.length + 24 ||
          !header.every((byte, index) => prefix[index] === byte)
        )
          throw new Error("Invalid E2EE stream header");
        state = sodium.decrypt(derived, prefix.subarray(header.length));
        derived.fill(0);
        while (true) {
          const size = await input.read(4);
          if (!size || size.length !== 4) throw new Error("Truncated E2EE stream");
          const length = new DataView(size.buffer, size.byteOffset, size.byteLength).getUint32(
            0,
            false,
          );
          if (length < OVERHEAD || length > BLOCK + OVERHEAD)
            throw new Error("Invalid E2EE stream record length");
          const ciphertext = await input.read(length);
          if (!ciphertext || ciphertext.length !== length) throw new Error("Truncated E2EE stream");
          const result = state.pull(ciphertext, aad);
          if (result.final) {
            if (result.message.length !== 0 || (await input.read(1)))
              throw new Error("Invalid E2EE stream ending");
            return;
          }
          if (result.message.length === 0) throw new Error("Invalid E2EE stream record");
          yield result.message;
        }
      } catch (error) {
        failed = true;
        throw error;
      } finally {
        derived.fill(0);
        state?.dispose();
        await input?.close(failed);
      }
    },
  };
}
