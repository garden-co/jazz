import { expect, it } from "vitest";
import {
  encodeStreamRecord,
  encryptStreamRecord,
  readStreamRecord,
  decryptStreamRecord,
} from "./stream-record.js";
import { createBrowserCrypto } from "./browser.js";
import { E2eeDataError } from "./data-error.js";

const epoch = "00000000-0000-0000-0000-000000000001";
const hex = (bytes: Uint8Array) =>
  Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
async function collect(source: AsyncIterable<Uint8Array>) {
  const chunks: Uint8Array[] = [];
  for await (const chunk of source) chunks.push(chunk);
  const result = new Uint8Array(chunks.reduce((size, chunk) => size + chunk.length, 0));
  let offset = 0;
  for (const chunk of chunks) {
    result.set(chunk, offset);
    offset += chunk.length;
  }
  return result;
}
async function* source() {
  yield new Uint8Array([0xde, 0xad]);
}

it("pins stream routing independently of adapter payload framing", async () => {
  const bytes = await collect(
    encodeStreamRecord(epoch, { id: "test.corpus", version: 7 }, source()),
  );
  expect(hex(bytes)).toBe(
    "4a45324501176a617a7a2e653265652e73747265616d2d7265636f726400000001" +
      "30303030303030302d303030302d303030302d303030302d303030303030303030303031" +
      "4a453245010b746573742e636f7270757300000007dead",
  );
  const record = readStreamRecord(bytes, { id: "test.corpus", version: 7 });
  expect(record.epoch).toBe(epoch);
  expect(record.ciphertext).toEqual(new Uint8Array([0xde, 0xad]));
  expect(record.ciphertext.buffer).toBe(bytes.buffer);
  expect(() => readStreamRecord(bytes, { id: "test.corpus", version: 8 })).toThrow();
});

it("does not release a decrypted value without final authentication and EOF", async () => {
  const cipher = (await createBrowserCrypto()).largeValueCipher!;
  const key = new Uint8Array(32).fill(42);
  const context = new Uint8Array([1, 2, 3]);
  const ciphertext = await collect(cipher.encrypt(key, context, source()));
  expect(await decryptStreamRecord(cipher, key, context, ciphertext)).toEqual(
    new Uint8Array([0xde, 0xad]),
  );
  await expect(
    decryptStreamRecord(cipher, key, context, ciphertext.subarray(0, ciphertext.length - 1)),
  ).rejects.toThrow();
  const trailing = new Uint8Array(ciphertext.length + 1);
  trailing.set(ciphertext);
  await expect(decryptStreamRecord(cipher, key, context, trailing)).rejects.toThrow();
  await expect(
    decryptStreamRecord(cipher, key, new Uint8Array([1, 2, 4]), ciphertext),
  ).rejects.toThrow();
});

it.each(["synchronous", "iteration"])(
  "redacts %s adapter failures while retaining source errors",
  async (failure) => {
    const cipher = (await createBrowserCrypto()).largeValueCipher!;
    const secret = new Uint8Array(32).fill(42);
    const context = new Uint8Array([1, 2, 3]);
    const adapterFailure = new Error("private-key-sentinel-must-not-escape");
    const broken = {
      ...cipher,
      encrypt() {
        if (failure === "synchronous") throw adapterFailure;
        return (async function* () {
          yield new Uint8Array([1]);
          throw adapterFailure;
        })();
      },
    };
    await expect(
      collect(encryptStreamRecord(broken, secret, context, epoch, source())),
    ).rejects.toEqual(new E2eeDataError("encryption-failed"));
    const sourceFailure = new Error("caller source unavailable");
    const failingSource = (async function* () {
      yield new Uint8Array([1]);
      throw sourceFailure;
    })();
    await expect(
      collect(encryptStreamRecord(cipher, secret, context, epoch, failingSource)),
    ).rejects.toBe(sourceFailure);
  },
);
