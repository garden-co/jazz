import { resolveCrypto } from "./crypto.js";
import { createSodiumDeviceSigner } from "./signer.js";
import {
  createSodiumCellCipher,
  createSodiumKeyEnvelope,
  createSodiumEqualityIndex,
} from "./sodium.js";
import type { SodiumKeyPrimitives } from "./sodium.js";
import type { CellCipher, CryptoAdapters, DeviceSigner, JazzCrypto, KeyEnvelope } from "./types.js";

export async function createBrowserCrypto(overrides: JazzCrypto = {}): Promise<CryptoAdapters> {
  return resolveCrypto(overrides, {
    cellCipher: createBrowserCellCipher,
    keyEnvelope: createBrowserKeyEnvelope,
    deviceSigner: createBrowserDeviceSigner,
    equalityIndex: createBrowserEqualityIndex,
  });
}

export async function createBrowserEqualityIndex() {
  return createSodiumEqualityIndex(await browserSodium());
}

/** Initialise the official libsodium.js implementation before preparing writes. */
export async function createBrowserCellCipher(): Promise<CellCipher> {
  return createSodiumCellCipher(await browserSodium());
}

export async function createBrowserKeyEnvelope(): Promise<KeyEnvelope> {
  return createSodiumKeyEnvelope(await browserSodium());
}

export async function createBrowserDeviceSigner(): Promise<DeviceSigner> {
  const { default: sodium } = await import("./sodium-browser.js");
  await sodium.ready;
  return createSodiumDeviceSigner({
    createKeyPair: () => sodium.crypto_sign_keypair(),
    fromSeed: (seed) => sodium.crypto_sign_seed_keypair(seed),
    sign: (key, message) => sodium.crypto_sign_detached(message, key),
    verify: (key, message, signature) =>
      sodium.crypto_sign_verify_detached(signature, message, key),
  });
}

async function browserSodium(): Promise<SodiumKeyPrimitives> {
  const { default: sodium } = await import("./sodium-browser.js");
  await sodium.ready;
  return {
    randomBytes: (length) => sodium.randombytes_buf(length),
    hash: (key, input) => sodium.crypto_generichash(32, input, key),
    encrypt: (key, nonce, aad, plaintext) =>
      sodium.crypto_aead_xchacha20poly1305_ietf_encrypt(plaintext, aad, null, nonce, key),
    decrypt: (key, nonce, aad, ciphertext) =>
      sodium.crypto_aead_xchacha20poly1305_ietf_decrypt(null, ciphertext, aad, nonce, key),
    createKeyPair: () => {
      const pair = sodium.crypto_box_keypair();
      return { publicKey: pair.publicKey, privateKey: pair.privateKey };
    },
    seal: (publicKey, plaintext) => sodium.crypto_box_seal(plaintext, publicKey),
    open: (device, ciphertext) =>
      sodium.crypto_box_seal_open(ciphertext, device.publicKey, device.privateKey),
  };
}
