import { expect, it } from "vitest";
import { createNativeCrypto } from "./native.js";
import { vectors, bytes } from "./fixtures/vectors.js";

it("reads the same independent C vectors through native crypto", async () => {
  const crypto = await createNativeCrypto();
  const root = bytes(vectors.root);
  const context = bytes(vectors.context);
  expect(await crypto.equalityIndex!.token(root, context, bytes(vectors.plaintext))).toEqual(
    bytes(vectors.equality),
  );
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
