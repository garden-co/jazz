import { createRequire } from "node:module";
import { resolveCrypto } from "./crypto.js";
import { createSodiumDeviceSigner } from "./signer.js";
import {
  createSodiumCellCipher,
  createSodiumKeyEnvelope,
  createSodiumEqualityIndex,
} from "./sodium.js";
import type { SodiumKeyPrimitives } from "./sodium.js";
import type { CellCipher, CryptoAdapters, DeviceSigner, JazzCrypto, KeyEnvelope } from "./types.js";

export async function createNativeCrypto(overrides: JazzCrypto = {}): Promise<CryptoAdapters> {
  return resolveCrypto(overrides, {
    cellCipher: createNativeCellCipher,
    keyEnvelope: createNativeKeyEnvelope,
    deviceSigner: createNativeDeviceSigner,
    equalityIndex: createNativeEqualityIndex,
  });
}

export async function createNativeEqualityIndex() {
  return createSodiumEqualityIndex(nativeSodium());
}

/** Native Rust/libsodium implementation; no browser-crypto fallback. */
export async function createNativeCellCipher(): Promise<CellCipher> {
  return createSodiumCellCipher(nativeSodium());
}

export async function createNativeKeyEnvelope(): Promise<KeyEnvelope> {
  return createSodiumKeyEnvelope(nativeSodium());
}

export async function createNativeDeviceSigner(): Promise<DeviceSigner> {
  const { e2eeSodiumSigningKeyPair, e2eeSodiumSign, e2eeSodiumVerify } = createRequire(
    import.meta.url,
  )("jazz-napi") as typeof import("jazz-napi");
  return createSodiumDeviceSigner({
    createKeyPair: () => e2eeSodiumSigningKeyPair(),
    fromSeed: (seed) => e2eeSodiumSigningKeyPair(seed),
    sign: e2eeSodiumSign,
    verify: e2eeSodiumVerify,
  });
}

function nativeSodium(): SodiumKeyPrimitives {
  const {
    e2eeSodiumNonce,
    e2eeSodiumHash,
    e2eeSodiumEncrypt,
    e2eeSodiumDecrypt,
    e2eeSodiumKeyPair,
    e2eeSodiumSeal,
    e2eeSodiumOpen,
  } = createRequire(import.meta.url)("jazz-napi") as typeof import("jazz-napi");
  return {
    randomBytes(length) {
      if (length !== 24) throw new Error("Invalid E2EE nonce length");
      return e2eeSodiumNonce();
    },
    hash: e2eeSodiumHash,
    encrypt: e2eeSodiumEncrypt,
    decrypt: e2eeSodiumDecrypt,
    createKeyPair: e2eeSodiumKeyPair,
    seal: e2eeSodiumSeal,
    open: (device, ciphertext) => e2eeSodiumOpen(device.publicKey, device.privateKey, ciphertext),
  };
}
