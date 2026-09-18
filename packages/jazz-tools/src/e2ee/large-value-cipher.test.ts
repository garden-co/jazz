import { describe, expect, it } from "vitest";
import { createBrowserCrypto } from "./browser.js";
import { createNativeCrypto } from "./native.js";
import { bytes, vectors } from "./fixtures/vectors.js";

async function* chunks(bytes: Uint8Array, size: number) {
  for (let offset = 0; offset < bytes.length; offset += size) {
    yield bytes.slice(offset, offset + size);
  }
}

async function collect(source: AsyncIterable<Uint8Array>) {
  const parts: Uint8Array[] = [];
  let length = 0;
  for await (const part of source) {
    parts.push(part);
    length += part.length;
  }
  const result = new Uint8Array(length);
  let offset = 0;
  for (const part of parts) {
    result.set(part, offset);
    offset += part.length;
  }
  return result;
}

describe.each([
  ["browser", createBrowserCrypto],
  ["native", createNativeCrypto],
] as const)("%s stream conformance", (_name, createCrypto) => {
  it.each(["stalls", "rejects"])(
    "preserves the read failure when source cleanup %s",
    async (cleanup) => {
      const cipher = (await createCrypto()).largeValueCipher!;
      const reason = new Error("Primary file read failure");
      for (const operation of ["encrypt", "decrypt"] as const) {
        let closed = false;
        const source: AsyncIterable<Uint8Array> = {
          [Symbol.asyncIterator]() {
            return {
              next: () => Promise.reject(reason),
              return() {
                closed = true;
                return cleanup === "stalls"
                  ? new Promise<IteratorResult<Uint8Array>>(() => {})
                  : Promise.reject(new Error("Secondary cleanup failure"));
              },
            };
          },
        };
        await expect(
          collect(cipher[operation](new Uint8Array(32), new Uint8Array(), source)),
        ).rejects.toBe(reason);
        expect(closed).toBe(true);
      }
    },
    5_000,
  );

  it("opens the independent C/libsodium stream fixture", async () => {
    const cipher = (await createCrypto()).largeValueCipher!;
    expect(
      await collect(
        cipher.decrypt(
          bytes(vectors.root),
          bytes(vectors.context),
          chunks(bytes(vectors.stream), 3),
        ),
      ),
    ).toEqual(new TextEncoder().encode("hellohello"));
  });

  it("authenticates empty values and independently replaces whole values", async () => {
    const cipher = (await createCrypto()).largeValueCipher!;
    const key = new Uint8Array(32).fill(23);
    const context = new Uint8Array([1, 2, 3]);
    const original = new Uint8Array(80_001).fill(5);
    const replacement = new Uint8Array([9, 8, 7]);
    const first = await collect(cipher.encrypt(key, context, chunks(original, 4_001)));
    const second = await collect(cipher.encrypt(key, context, chunks(replacement, 2)));
    const repeated = await collect(cipher.encrypt(key, context, chunks(replacement, 2)));
    const empty = await collect(cipher.encrypt(key, context, chunks(new Uint8Array(), 1)));
    expect(second).not.toEqual(repeated);
    for (const [ciphertext, plaintext] of [
      [first, original],
      [second, replacement],
      [empty, new Uint8Array()],
    ]) {
      expect(await collect(cipher.decrypt(key, context, chunks(ciphertext, 503)))).toEqual(
        plaintext,
      );
    }
  });

  it("requires final authentication even after releasing authenticated data", async () => {
    const cipher = (await createCrypto()).largeValueCipher!;
    const key = new Uint8Array(32);
    const context = new Uint8Array();
    const records: Uint8Array[] = [];
    for await (const record of cipher.encrypt(key, context, chunks(new Uint8Array([42]), 1)))
      records.push(record);
    for (const ending of [
      undefined,
      records[2].slice(0, -1),
      Uint8Array.from(records[2], (byte, index) =>
        index === records[2].length - 1 ? byte ^ 1 : byte,
      ),
    ]) {
      async function* source() {
        yield records[0];
        yield records[1];
        if (ending) yield ending;
      }
      const stream = cipher.decrypt(key, context, source())[Symbol.asyncIterator]();
      expect(await stream.next()).toEqual({ done: false, value: new Uint8Array([42]) });
      await expect(stream.next()).rejects.toThrow();
    }
  });

  it("rejects altered ciphertext, wrong context, wrong keys and trailing bytes", async () => {
    const cipher = (await createCrypto()).largeValueCipher!;
    const key = new Uint8Array(32);
    const context = new Uint8Array([3]);
    const encrypted = await collect(cipher.encrypt(key, context, chunks(new Uint8Array([42]), 1)));
    const altered = encrypted.slice();
    altered[altered.length - 1] ^= 1;
    const trailing = new Uint8Array(encrypted.length + 1);
    trailing.set(encrypted);
    for (const [candidate, candidateKey, candidateContext] of [
      [altered, key, context],
      [trailing, key, context],
      [encrypted, new Uint8Array(32).fill(1), context],
      [encrypted, key, new Uint8Array([4])],
    ]) {
      await expect(
        collect(cipher.decrypt(candidateKey, candidateContext, chunks(candidate, 17))),
      ).rejects.toThrow();
    }
  });

  it("pulls only the current record and closes the source on early return", async () => {
    const cipher = (await createCrypto()).largeValueCipher!;
    let pulled = 0;
    let closed = false;
    async function* source() {
      try {
        while (true) {
          pulled++;
          yield new Uint8Array(65_536);
        }
      } finally {
        closed = true;
      }
    }
    const stream = cipher
      .encrypt(new Uint8Array(32), new Uint8Array(), source())
      [Symbol.asyncIterator]();
    await stream.next();
    expect(pulled).toBe(0);
    await stream.next();
    expect(pulled).toBe(1);
    await stream.next();
    expect(pulled).toBe(2);
    await stream.return!();
    expect(closed).toBe(true);
  });

  it("propagates upstream errors without completing the encrypted stream", async () => {
    const cipher = (await createCrypto()).largeValueCipher!;
    const reason = new Error("File read failed");
    async function* source(): AsyncGenerator<Uint8Array> {
      yield new Uint8Array();
      throw reason;
    }
    for (const operation of ["encrypt", "decrypt"] as const) {
      await expect(
        collect(cipher[operation](new Uint8Array(32), new Uint8Array(), source())),
      ).rejects.toBe(reason);
    }
  });
});

it("encrypts a file stream independently of transport chunk boundaries", async () => {
  const { largeValueCipher: cipher } = await createBrowserCrypto();
  expect(cipher).toBeDefined();
  const key = new Uint8Array(32).fill(7);
  const context = new TextEncoder().encode("file replacement context");
  const plaintext = new TextEncoder().encode("A streamed file with Unicode: café and 日本語.");
  const encrypted = await collect(cipher!.encrypt(key, context, chunks(plaintext, 11)));
  const decrypted = await collect(cipher!.decrypt(key, context, chunks(encrypted, 7)));
  expect(decrypted).toEqual(plaintext);
});

it("exchanges multi-record files between native and browser crypto", async () => {
  const browser = (await createBrowserCrypto()).largeValueCipher;
  const native = (await createNativeCrypto()).largeValueCipher;
  expect(native).toBeDefined();
  const key = new Uint8Array(32).fill(19);
  const context = new TextEncoder().encode("cross-platform file");
  const plaintext = Uint8Array.from({ length: 150_003 }, (_, index) => index % 251);
  for (const [writer, reader] of [
    [native!, browser!],
    [browser!, native!],
  ]) {
    const ciphertext = await collect(writer.encrypt(key, context, chunks(plaintext, 10_001)));
    expect(await collect(reader.decrypt(key, context, chunks(ciphertext, 997)))).toEqual(plaintext);
  }
});

it.each(["encrypt", "decrypt"] as const)(
  "cancels a pending upstream read during %s",
  async (operation) => {
    const { largeValueCipher: cipher } = await createBrowserCrypto();
    const controller = new AbortController();
    const reason = new Error("Cancelled by the caller");
    let returned = false;
    const source: AsyncIterable<Uint8Array> = {
      [Symbol.asyncIterator]() {
        return {
          next: () => new Promise<IteratorResult<Uint8Array>>(() => {}),
          async return() {
            returned = true;
            return { done: true, value: undefined };
          },
        };
      },
    };
    const stream = cipher!
      [operation](new Uint8Array(32), new Uint8Array(), source, {
        signal: controller.signal,
      })
      [Symbol.asyncIterator]();
    if (operation === "encrypt") await stream.next(); // Stream header precedes source reads.
    const pending = stream.next();
    controller.abort(reason);
    await expect(pending).rejects.toBe(reason);
    expect(returned).toBe(true);
  },
  5_000,
);
