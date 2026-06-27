import {
  createLocalFirstJwtWithSignerAsync,
  type LocalFirstJwtOptions,
} from "./local-first-jwt-shared.js";

export async function createLocalFirstJwtAsync(options: LocalFirstJwtOptions): Promise<string> {
  return createLocalFirstJwtWithSignerAsync(options, signEd25519WithWebCrypto);
}

async function signEd25519WithWebCrypto(
  signingInput: Uint8Array,
  privateKeyDer: Uint8Array,
): Promise<Uint8Array> {
  const subtle = globalThis.crypto?.subtle;
  if (!subtle)
    throw new Error("WebCrypto SubtleCrypto is required for async local-first JWT signing");

  const key = await subtle.importKey(
    "pkcs8",
    arrayBufferFromBytes(privateKeyDer),
    { name: "Ed25519" },
    false,
    ["sign"],
  );
  return new Uint8Array(
    await subtle.sign({ name: "Ed25519" }, key, arrayBufferFromBytes(signingInput)),
  );
}

function arrayBufferFromBytes(bytes: Uint8Array): ArrayBuffer {
  const out = new ArrayBuffer(bytes.byteLength);
  new Uint8Array(out).set(bytes);
  return out;
}
