// Run the same adapter contracts in a real browser, not only a Node-hosted WASM build.
import "../../src/e2ee/cell-cipher.test.js";
import "../../src/e2ee/context.test.js";
import "../../src/e2ee/key-envelope.test.js";
import "../../src/e2ee/crypto.test.js";
import "../../src/e2ee/vectors.test.js";

import { expect, test } from "vitest";
import { createBrowserCrypto } from "../../dist/e2ee/browser.js";
import { bytes, vectors } from "../../src/e2ee/fixtures/vectors.js";

test("built browser package opens independent fixtures", async () => {
  const crypto = await createBrowserCrypto();
  expect(
    await crypto.equalityIndex!.token(
      bytes(vectors.root),
      bytes(vectors.context),
      bytes(vectors.plaintext),
    ),
  ).toEqual(bytes(vectors.equality));
  expect(
    await crypto.cellCipher.decrypt(
      bytes(vectors.root),
      bytes(vectors.context),
      bytes(vectors.cell),
    ),
  ).toEqual(bytes(vectors.plaintext));
  expect(
    await crypto.keyEnvelope.unwrap(
      bytes(vectors.root),
      bytes(vectors.context),
      bytes(vectors.wrap),
    ),
  ).toEqual(bytes(vectors.key));
  expect(
    await crypto.keyEnvelope.open(
      { publicKey: bytes(vectors.publicKey), privateKey: bytes(vectors.privateKey) },
      bytes(vectors.context),
      bytes(vectors.sealed),
    ),
  ).toEqual(bytes(vectors.key));
});
