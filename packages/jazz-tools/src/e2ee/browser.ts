import { resolveCrypto } from "./crypto.js";
import { createSodiumLargeValueCipher } from "./large-value.js";
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
    largeValueCipher: createBrowserLargeValueCipher,
  });
}

export async function createBrowserEqualityIndex() {
  return createSodiumEqualityIndex(await browserSodium());
}

export async function createBrowserLargeValueCipher() {
  // Keep this platform-specific WASM module unloaded when a caller overrides the adapter.
  const { default: sodium } = await import("./sodium-browser.js");
  await sodium.ready;
  // The pinned vendor exposes this memory API but omits it from its public declarations.
  const implementation = sodium as unknown as {
    libsodium: {
      HEAPU8: Uint8Array;
      _free(address: number): void;
      _crypto_secretstream_xchacha20poly1305_statebytes(): number;
    };
  };
  const memory = implementation.libsodium;
  const dispose = (state: unknown) => {
    if (typeof state !== "number" || !Number.isSafeInteger(state) || state <= 0)
      throw new Error("Invalid E2EE stream state");
    memory.HEAPU8.fill(
      0,
      state,
      state + memory._crypto_secretstream_xchacha20poly1305_statebytes(),
    );
    memory._free(state);
  };
  return createSodiumLargeValueCipher({
    hash: (key, input) => sodium.crypto_generichash(32, input, key),
    encrypt(key) {
      const { state, header } = sodium.crypto_secretstream_xchacha20poly1305_init_push(key);
      return {
        header,
        push: (message, context, final) =>
          sodium.crypto_secretstream_xchacha20poly1305_push(
            state,
            message,
            context,
            final ? sodium.crypto_secretstream_xchacha20poly1305_TAG_FINAL : 0,
          ),
        dispose: () => dispose(state),
      };
    },
    decrypt(key, header) {
      const state = sodium.crypto_secretstream_xchacha20poly1305_init_pull(header, key);
      return {
        pull(ciphertext, context) {
          const result = sodium.crypto_secretstream_xchacha20poly1305_pull(
            state,
            ciphertext,
            context,
          );
          if (
            !result ||
            (result.tag !== 0 &&
              result.tag !== sodium.crypto_secretstream_xchacha20poly1305_TAG_FINAL)
          )
            throw new Error("E2EE stream authentication failed");
          return {
            message: result.message,
            final: result.tag === sodium.crypto_secretstream_xchacha20poly1305_TAG_FINAL,
          };
        },
        dispose: () => dispose(state),
      };
    },
  });
}

/** Initialise the official libsodium.js implementation before preparing writes. */
export async function createBrowserCellCipher(): Promise<CellCipher> {
  return createSodiumCellCipher(await browserSodium());
}

export async function createBrowserKeyEnvelope(): Promise<KeyEnvelope> {
  return createSodiumKeyEnvelope(await browserSodium());
}

export async function createBrowserDeviceSigner(): Promise<DeviceSigner> {
  // Keep this platform-specific WASM module unloaded when a caller overrides the adapter.
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
  // Keep this platform-specific WASM module unloaded when a caller overrides the adapter.
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
