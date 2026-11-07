import { expect, it } from "vitest";
import { createBrowserCrypto, createBrowserDeviceSigner } from "./browser.js";
import { resolveCrypto } from "./crypto.js";
import { bytes } from "./fixtures/vectors.js";

it("selects signing independently and verifies an OpenSSL fixture in the browser", async () => {
  const defaults = await createBrowserCrypto();
  expect(defaults.deviceSigner).toBeDefined();
  const key = bytes("d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a");
  // Node/OpenSSL, RFC 8032 test-one key, signed bytes = mechanism header + empty record.
  const signature = bytes(
    "4a45324501106a617a7a2e736f6469756d2e7369676e00000001d6f44f1f2a12de8dfd060b49287ab03ec6454619a481c4ff1d48458fdeb723e88d3ac8a0f0a636111fc2771f671d35612250a9aeb7992dc71fd4f286d24f0408",
  );
  expect(await defaults.deviceSigner.verify(key, new Uint8Array(), signature)).toBe(true);
  signature[signature.length - 1] ^= 1;
  expect(await defaults.deviceSigner.verify(key, new Uint8Array(), signature)).toBe(false);
  const custom = await createBrowserDeviceSigner();
  const selected = await createBrowserCrypto({ deviceSigner: custom });
  expect(selected.deviceSigner).toBe(custom);
  expect(selected.cellCipher.mechanism).toEqual(defaults.cellCipher.mechanism);
  expect(selected.keyEnvelope.mechanism).toEqual(defaults.keyEnvelope.mechanism);
  const unavailable = () => {
    throw new Error("overridden default must not load");
  };
  const skipped = await resolveCrypto(selected, {
    cellCipher: unavailable,
    keyEnvelope: unavailable,
    deviceSigner: unavailable,
  });
  expect(skipped.deviceSigner).toBe(custom);
  await expect(
    createBrowserCrypto({
      deviceSigner: { ...custom, mechanism: { id: "test.signer", version: 0 } },
    }),
  ).rejects.toThrow();
});
