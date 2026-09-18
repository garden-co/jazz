import { createPrivateKey, createPublicKey, sign, verify } from "node:crypto";
import { expect, it } from "vitest";
import { createBrowserDeviceSigner } from "./browser.js";

// RFC 8032 test 1 key; Node/OpenSSL independently checks the Jazz framing.
const seed = Buffer.from("9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60", "hex");
const publicKey = Buffer.from(
  "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a",
  "hex",
);
const secretKey = Buffer.concat([seed, publicKey]);
const header = Buffer.from("4a45324501106a617a7a2e736f6469756d2e7369676e00000001", "hex");
const privateKey = createPrivateKey({
  key: Buffer.concat([Buffer.from("302e020100300506032b657004220420", "hex"), seed]),
  format: "der",
  type: "pkcs8",
});

it("signs canonical record bytes with independently verified framing and rejects tampering", async () => {
  const adapter = await createBrowserDeviceSigner();
  const record = new TextEncoder().encode("account/epoch/membership revision");
  const expected = Buffer.concat([header, sign(null, Buffer.concat([header, record]), privateKey)]);
  const signature = await adapter.sign(secretKey, record);
  expect(signature).toEqual(new Uint8Array(expected));
  expect(await adapter.verify(publicKey, record, expected)).toBe(true);
  expect(
    verify(
      null,
      Buffer.concat([header, record]),
      createPublicKey(privateKey),
      signature.subarray(header.length),
    ),
  ).toBe(true);
  for (let index = 0; index < signature.length; index++) {
    const changed = signature.slice();
    changed[index] ^= 1;
    expect(await adapter.verify(publicKey, record, changed)).toBe(false);
  }
  expect(await adapter.verify(publicKey, new Uint8Array(), signature)).toBe(false);
  expect(await adapter.verify(new Uint8Array(32), record, signature)).toBe(false);
  expect(await adapter.verify(publicKey, record, signature.subarray(1))).toBe(false);
  const pair = await adapter.createKeyPair();
  expect(pair.publicKey).toHaveLength(32);
  expect(pair.privateKey).toHaveLength(64);
  const generated = await adapter.sign(pair.privateKey, record);
  expect(await adapter.verify(pair.publicKey, record, generated)).toBe(true);
  expect(await adapter.verify(publicKey, record, generated)).toBe(false);
  const corrupt = secretKey.slice();
  // Own the mutation: Buffer.slice aliases its parent.
  const mismatched = new Uint8Array(corrupt);
  mismatched[63] ^= 1;
  await expect(adapter.sign(mismatched, record)).rejects.toThrow();
  await expect(adapter.sign(new Uint8Array(32), record)).rejects.toThrow();
  expect(secretKey).toEqual(Buffer.concat([seed, publicKey]));
});
