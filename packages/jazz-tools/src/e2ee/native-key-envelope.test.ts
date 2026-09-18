import { expect, it } from "vitest";
import { loadNapiModule } from "../runtime/testing/napi-runtime-test-utils.js";
import { createBrowserKeyEnvelope } from "./browser.js";
import { createNativeKeyEnvelope } from "./native.js";

it("exchanges wraps and device envelopes between native and JavaScript libsodium", async () => {
  const browser = await createBrowserKeyEnvelope();
  const native = await createNativeKeyEnvelope();
  const key = new Uint8Array(32).fill(9);
  const wrappingKey = new Uint8Array(32).fill(7);
  const context = new Uint8Array([1, 2, 3]);
  expect(
    await native.unwrap(wrappingKey, context, await browser.wrap(wrappingKey, context, key)),
  ).toEqual(key);
  expect(
    await browser.unwrap(wrappingKey, context, await native.wrap(wrappingKey, context, key)),
  ).toEqual(key);
  const nativeDevice = await native.createKeyPair();
  const browserDevice = await browser.createKeyPair();
  const toNative = await browser.seal(nativeDevice.publicKey, context, key);
  expect(await native.open(nativeDevice, context, toNative)).toEqual(key);
  expect(
    await browser.open(
      browserDevice,
      context,
      await native.seal(browserDevice.publicKey, context, key),
    ),
  ).toEqual(key);
  await expect(native.open(browserDevice, context, toNative)).rejects.toThrow();
  await expect(native.open(nativeDevice, new Uint8Array([1, 2, 4]), toNative)).rejects.toThrow();
});

it("validates Rust primitive boundaries and handles empty plaintext", async () => {
  const n = await loadNapiModule();
  const key = new Uint8Array(32).fill(7);
  const nonce = n.e2eeSodiumNonce();
  const empty = new Uint8Array();
  expect(
    n.e2eeSodiumDecrypt(key, nonce, empty, n.e2eeSodiumEncrypt(key, nonce, empty, empty)),
  ).toEqual(empty);
  expect(() => n.e2eeSodiumHash(new Uint8Array(31), empty)).toThrow();
  expect(() => n.e2eeSodiumEncrypt(key, new Uint8Array(23), empty, empty)).toThrow();
  expect(() => n.e2eeSodiumDecrypt(key, nonce, empty, new Uint8Array(15))).toThrow();
  const device = n.e2eeSodiumKeyPair();
  expect(
    n.e2eeSodiumOpen(
      device.publicKey,
      device.privateKey,
      n.e2eeSodiumSeal(device.publicKey, empty),
    ),
  ).toEqual(empty);
  expect(() => n.e2eeSodiumSeal(new Uint8Array(31), empty)).toThrow();
  expect(() =>
    n.e2eeSodiumOpen(device.publicKey, new Uint8Array(31), new Uint8Array(48)),
  ).toThrow();
  expect(() => n.e2eeSodiumOpen(device.publicKey, device.privateKey, new Uint8Array(47))).toThrow();
});
