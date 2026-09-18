import { expect, it } from "vitest";
import { e2eeSodiumSigningKeyPair, e2eeSodiumSign, e2eeSodiumVerify } from "jazz-napi";
import { createBrowserDeviceSigner } from "./browser.js";
import { createNativeCrypto, createNativeDeviceSigner } from "./native.js";

it("selects the native signer by default and preserves an independent override", async () => {
  const defaults = await createNativeCrypto();
  expect(defaults.deviceSigner).toBeDefined();
  const custom = await createBrowserDeviceSigner();
  const selected = await createNativeCrypto({ deviceSigner: custom });
  expect(selected.deviceSigner).toBe(custom);
  expect(selected.cellCipher.mechanism).toEqual(defaults.cellCipher.mechanism);
  expect(selected.keyEnvelope.mechanism).toEqual(defaults.keyEnvelope.mechanism);
});

it("validates signing inputs at the native binding without relying on the TypeScript adapter", () => {
  const message = new Uint8Array();
  for (const length of [0, 31, 33]) {
    expect(() => e2eeSodiumSigningKeyPair(new Uint8Array(length))).toThrow();
  }
  for (const length of [0, 32, 63, 65]) {
    expect(() => e2eeSodiumSign(new Uint8Array(length), message)).toThrow();
  }
  const pair = e2eeSodiumSigningKeyPair();
  const retained = new Uint8Array(pair.privateKey);
  const signature = e2eeSodiumSign(pair.privateKey, message);
  expect(e2eeSodiumVerify(pair.publicKey, message, signature)).toBe(true);
  expect(e2eeSodiumVerify(new Uint8Array(31), message, signature)).toBe(false);
  expect(e2eeSodiumVerify(pair.publicKey, message, signature.subarray(1))).toBe(false);
  const mismatched = new Uint8Array(pair.privateKey);
  mismatched[63] ^= 1;
  expect(() => e2eeSodiumSign(mismatched, message)).toThrow();
  expect(pair.privateKey).toEqual(retained);
});

it("exchanges device signatures between native and browser implementations", async () => {
  const browser = await createBrowserDeviceSigner();
  const native = await createNativeDeviceSigner();
  const record = new TextEncoder().encode("accepted predecessor and membership revision");
  for (const creator of [native, browser]) {
    const pair = await creator.createKeyPair();
    const nativeSignature = await native.sign(pair.privateKey, record);
    const browserSignature = await browser.sign(pair.privateKey, record);
    expect(nativeSignature).toEqual(browserSignature);
    expect(await browser.verify(pair.publicKey, record, nativeSignature)).toBe(true);
    expect(await native.verify(pair.publicKey, record, browserSignature)).toBe(true);
    expect(await native.verify(pair.publicKey, new Uint8Array(), browserSignature)).toBe(false);
    const changed = browserSignature.slice();
    changed[changed.length - 1] ^= 1;
    expect(await native.verify(pair.publicKey, record, changed)).toBe(false);
    const malformed = new Uint8Array(pair.privateKey);
    malformed[63] ^= 1;
    await expect(native.sign(malformed, record)).rejects.toThrow();
  }
});
