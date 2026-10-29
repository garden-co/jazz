import { expect, test } from "vitest";
import { createBrowserCrypto } from "../../dist/e2ee/browser.js";
import { bytes, vectors } from "../../src/e2ee/fixtures/vectors.js";

test("built browser package opens the independent C/libsodium stream fixture", async () => {
  const cipher = (await createBrowserCrypto()).largeValueCipher!;
  async function* source() {
    yield bytes(vectors.stream);
  }
  const records = [];
  for await (const record of cipher.decrypt(bytes(vectors.root), bytes(vectors.context), source()))
    records.push(new TextDecoder().decode(record));
  expect(records).toEqual(["hello", "hello"]);
});

test("built browser package streams bounded records and authenticates the ending", async () => {
  const cipher = (await createBrowserCrypto()).largeValueCipher!;
  const key = new Uint8Array(32).fill(7);
  const context = new Uint8Array([1, 2]);
  let pulled = 0;
  async function* plaintext() {
    for (let index = 0; index < 16; index++) {
      pulled++;
      yield new Uint8Array(65_536).fill(index);
    }
  }
  const encrypt = cipher.encrypt(key, context, plaintext())[Symbol.asyncIterator]();
  const records = [(await encrypt.next()).value!];
  expect(pulled).toBe(0);
  records.push((await encrypt.next()).value!);
  expect(pulled).toBe(1);
  while (true) {
    const record = await encrypt.next();
    if (record.done) break;
    records.push(record.value);
  }
  async function* ciphertext(truncated = false) {
    for (const record of truncated ? records.slice(0, -1) : records) {
      for (let offset = 0; offset < record.length; offset += 997)
        yield record.slice(offset, offset + 997);
    }
  }
  let count = 0;
  for await (const record of cipher.decrypt(key, context, ciphertext())) {
    expect(record).toEqual(new Uint8Array(65_536).fill(count++));
  }
  expect(count).toBe(16);
  const incomplete = async () => {
    let delivered = 0;
    try {
      for await (const record of cipher.decrypt(key, context, ciphertext(true)))
        delivered += record.length;
    } finally {
      expect(delivered).toBe(1_048_576);
    }
  };
  await expect(incomplete()).rejects.toThrow("Truncated E2EE stream");
});

test("built browser package cancels a source whose read and cleanup both stall", async () => {
  const cipher = (await createBrowserCrypto()).largeValueCipher!;
  for (const operation of ["encrypt", "decrypt"] as const) {
    const controller = new AbortController();
    const reason = new Error("Cancelled file transfer");
    let closing = false;
    const source: AsyncIterable<Uint8Array> = {
      [Symbol.asyncIterator]() {
        return {
          next: () => new Promise<IteratorResult<Uint8Array>>(() => {}),
          return() {
            closing = true;
            return new Promise<IteratorResult<Uint8Array>>(() => {});
          },
        };
      },
    };
    const stream = cipher[operation](new Uint8Array(32), new Uint8Array(), source, {
      signal: controller.signal,
    })[Symbol.asyncIterator]();
    if (operation === "encrypt") await stream.next();
    const pending = stream.next();
    controller.abort(reason);
    await expect(pending).rejects.toBe(reason);
    expect(closing).toBe(true);
  }
});
