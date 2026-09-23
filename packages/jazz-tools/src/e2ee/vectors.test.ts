import { expect, it } from "vitest";
import { createBrowserCrypto } from "./browser.js";
import { encodeCryptoContext } from "./context.js";
import { vectors, bytes } from "./fixtures/vectors.js";

it("matches independent equality tokens and separates keys, contexts and values", async () => {
  const crypto = await createBrowserCrypto();
  const index = crypto.equalityIndex!;
  const input = [bytes(vectors.root), bytes(vectors.context), bytes(vectors.plaintext)] as const;
  const expected = bytes(vectors.equality);
  expect(await index.token(...input)).toEqual(expected);
  expect(await index.token(...input)).toEqual(expected);
  for (let part = 0; part < input.length; part++) {
    const changed = input.map((value) => value.slice()) as [Uint8Array, Uint8Array, Uint8Array];
    changed[part]![0]! ^= 1;
    expect(await index.token(...changed)).not.toEqual(expected);
  }
  await expect(index.token(new Uint8Array(31), input[1], input[2])).rejects.toThrow();
  const { default: sodium } = await import("libsodium-wrappers");
  await sodium.ready;
  expect(sodium.crypto_generichash(32, bytes(vectors.equalityContext), input[0])).toEqual(
    bytes(vectors.equalityDerived),
  );
});

it("reads independent C-generated cell, wrap and sealed-box vectors", async () => {
  const crypto = await createBrowserCrypto();
  const root = bytes(vectors.root);
  const context = bytes(vectors.context);
  expect(
    encodeCryptoContext({
      application: "a",
      policy: "p",
      scope: "s",
      identifier: "i",
      table: "t",
      row: "r",
      column: "c",
      epoch: "e",
      recipient: "d",
    }),
  ).toEqual(context);
  const device = { publicKey: bytes(vectors.publicKey), privateKey: bytes(vectors.privateKey) };
  expect(await crypto.cellCipher.decrypt(root, context, bytes(vectors.cell))).toEqual(
    bytes(vectors.plaintext),
  );
  expect(await crypto.keyEnvelope.unwrap(root, context, bytes(vectors.wrap))).toEqual(
    bytes(vectors.key),
  );
  expect(await crypto.keyEnvelope.open(device, context, bytes(vectors.sealed))).toEqual(
    bytes(vectors.key),
  );
});

it("writes the independent derivation and framing conventions", async () => {
  const crypto = await createBrowserCrypto();
  const { default: sodium } = await import("libsodium-wrappers");
  await sodium.ready;
  const root = bytes(vectors.root);
  const context = bytes(vectors.context);
  const cell = await crypto.cellCipher.encrypt(root, context, bytes(vectors.plaintext));
  expect(cell.subarray(0, 26)).toEqual(bytes(vectors.cell).subarray(0, 26));
  expect(
    sodium.crypto_aead_xchacha20poly1305_ietf_decrypt(
      null,
      cell.subarray(50),
      bytes(vectors.cellAad),
      cell.subarray(26, 50),
      bytes(vectors.cellDerived),
    ),
  ).toEqual(bytes(vectors.plaintext));
  const wrap = await crypto.keyEnvelope.wrap(root, context, bytes(vectors.key));
  expect(wrap.subarray(0, 26)).toEqual(bytes(vectors.wrap).subarray(0, 26));
  expect(
    sodium.crypto_aead_xchacha20poly1305_ietf_decrypt(
      null,
      wrap.subarray(50),
      bytes(vectors.wrapAad),
      wrap.subarray(26, 50),
      bytes(vectors.wrapDerived),
    ),
  ).toEqual(bytes(vectors.key));
  const sealed = await crypto.keyEnvelope.seal(
    bytes(vectors.publicKey),
    context,
    bytes(vectors.key),
  );
  expect(sealed.subarray(0, 26)).toEqual(bytes(vectors.sealed).subarray(0, 26));
  expect(
    sodium.crypto_box_seal_open(
      sealed.subarray(26),
      bytes(vectors.publicKey),
      bytes(vectors.privateKey),
    ),
  ).toEqual(bytes(vectors.sealedPlaintext));
});

it("rejects a changed byte anywhere in each independent envelope", async () => {
  const crypto = await createBrowserCrypto();
  const root = bytes(vectors.root);
  const context = bytes(vectors.context);
  const device = { publicKey: bytes(vectors.publicKey), privateKey: bytes(vectors.privateKey) };
  for (const [hex, open] of [
    [vectors.cell, (value: Uint8Array) => crypto.cellCipher.decrypt(root, context, value)],
    [vectors.wrap, (value: Uint8Array) => crypto.keyEnvelope.unwrap(root, context, value)],
    [vectors.sealed, (value: Uint8Array) => crypto.keyEnvelope.open(device, context, value)],
  ] as const) {
    const original = bytes(hex);
    for (let index = 0; index < original.length; index++) {
      const corrupted = original.slice();
      corrupted[index]! ^= 1;
      await expect(open(corrupted)).rejects.toThrow();
    }
  }
});
