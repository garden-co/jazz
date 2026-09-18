import { test } from "node:test";
import { strict as assert } from "node:assert";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { bundleE2eeCrypto } from "./bundle-e2ee-crypto.mjs";

test("the published sodium bundle works without consumer dependencies", async () => {
  const directory = await mkdtemp(join(tmpdir(), "jazz-e2ee-bundle-"));
  try {
    await bundleE2eeCrypto(directory);
    const { default: sodium } = await import(pathToFileURL(join(directory, "sodium-browser.js")));
    await sodium.ready;
    assert.equal(sodium.sodium_version_string(), "1.0.22");
    const key = sodium.randombytes_buf(32);
    const nonce = sodium.randombytes_buf(24);
    const plaintext = new Uint8Array([1, 2, 3]);
    const ciphertext = sodium.crypto_aead_xchacha20poly1305_ietf_encrypt(
      plaintext,
      null,
      null,
      nonce,
      key,
    );
    assert.deepEqual(
      sodium.crypto_aead_xchacha20poly1305_ietf_decrypt(null, ciphertext, null, nonce, key),
      plaintext,
    );
    const notice = await readFile(join(directory, "SODIUM-LICENSE.txt"), "utf8");
    assert.match(notice, /Permission to use/);
    assert.match(notice, /0\.8\.3/);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("public crypto entry points do not expose the sodium dependency", async () => {
  const pkg = JSON.parse(await readFile(new URL("../package.json", import.meta.url), "utf8"));
  for (const entry of ["e2ee", "e2ee/browser", "e2ee/native"]) {
    assert.ok(pkg.exports[`./${entry}`]?.types);
    assert.ok(pkg.exports[`./${entry}`]?.default);
  }
  assert.equal(pkg.dependencies.libsodium, undefined);
  assert.equal(pkg.dependencies["libsodium-wrappers"], undefined);
});
