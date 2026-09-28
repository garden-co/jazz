import { decodeEnvelope, encodeEnvelope } from "./envelope.js";
import type { CellCipher, DeviceKeyPair, KeyEnvelope, EqualityIndex } from "./types.js";
import { frameCryptoRecord } from "./record-frame.js";

/** BLAKE2b-256, with separate context-derived keys for equality tokens. */
export function createSodiumEqualityIndex(sodium: Pick<SodiumPrimitives, "hash">): EqualityIndex {
  const mechanism = { id: "jazz.sodium.equality", version: 1 } as const;
  const header = encodeEnvelope(mechanism, new Uint8Array());
  return {
    mechanism,
    async token(key, context, plaintext) {
      checkInput(key, context, plaintext);
      const derived = sodium.hash(key, frameCryptoRecord([header, context]));
      try {
        return encodeEnvelope(mechanism, sodium.hash(derived, plaintext));
      } finally {
        derived.fill(0);
      }
    },
  };
}

/** Internal platform seam, not a substitute for the public BYOC interfaces. */
export interface SodiumPrimitives {
  randomBytes(length: number): Uint8Array;
  hash(key: Uint8Array, input: Uint8Array): Uint8Array;
  encrypt(key: Uint8Array, nonce: Uint8Array, aad: Uint8Array, plaintext: Uint8Array): Uint8Array;
  decrypt(key: Uint8Array, nonce: Uint8Array, aad: Uint8Array, ciphertext: Uint8Array): Uint8Array;
}

const CELL = Object.freeze({ id: "jazz.sodium.cell", version: 1 });
const KEY = Object.freeze({ id: "jazz.sodium.key", version: 1 });
const WRAP_KEY_LABEL = new TextEncoder().encode("jazz.e2ee.wrap-key.v1\0");
const CELL_KEY_LABEL = new TextEncoder().encode("jazz.e2ee.cell-key.v1\0");

export interface SodiumKeyPrimitives extends SodiumPrimitives {
  createKeyPair(): DeviceKeyPair;
  seal(publicKey: Uint8Array, plaintext: Uint8Array): Uint8Array;
  open(device: DeviceKeyPair, ciphertext: Uint8Array): Uint8Array;
}

function concat(...parts: Uint8Array[]): Uint8Array {
  const result = new Uint8Array(parts.reduce((length, part) => length + part.length, 0));
  let offset = 0;
  for (const part of parts) {
    result.set(part, offset);
    offset += part.length;
  }
  return result;
}

function checkInput(key: Uint8Array, context: Uint8Array, value: Uint8Array): void {
  if (
    !(key instanceof Uint8Array) ||
    key.length !== 32 ||
    !(context instanceof Uint8Array) ||
    !(value instanceof Uint8Array)
  ) {
    throw new Error("Invalid E2EE crypto input");
  }
}

/** Shared framing and derivation; only libsodium operations vary by platform. */
export function createSodiumCellCipher(sodium: SodiumPrimitives): CellCipher {
  const header = encodeEnvelope(CELL, new Uint8Array());
  return {
    mechanism: CELL,
    async encrypt(key, context, plaintext) {
      checkInput(key, context, plaintext);
      const aad = concat(header, context);
      const derived = sodium.hash(key, concat(CELL_KEY_LABEL, aad));
      try {
        const nonce = sodium.randomBytes(24);
        const ciphertext = sodium.encrypt(derived, nonce, aad, plaintext);
        return encodeEnvelope(CELL, concat(nonce, ciphertext));
      } finally {
        derived.fill(0);
      }
    },
    async decrypt(key, context, envelope) {
      checkInput(key, context, envelope);
      const payload = decodeEnvelope(CELL, envelope);
      if (payload.length < 40) throw new Error("Invalid E2EE ciphertext");
      const aad = concat(header, context);
      const derived = sodium.hash(key, concat(CELL_KEY_LABEL, aad));
      try {
        return Uint8Array.from(
          sodium.decrypt(derived, payload.subarray(0, 24), aad, payload.subarray(24)),
        );
      } catch {
        throw new Error("E2EE authentication failed");
      } finally {
        derived.fill(0);
      }
    },
  };
}

/** A key envelope carries exactly one 32-byte key, never application data. */
export function createSodiumKeyEnvelope(sodium: SodiumKeyPrimitives): KeyEnvelope {
  const header = encodeEnvelope(KEY, new Uint8Array());
  const wrapPrefix = concat(header, new Uint8Array([1]));
  const sealPrefix = concat(header, new Uint8Array([2]));
  function sealedContext(context: Uint8Array): Uint8Array {
    if (context.length > 0xffffffff) throw new Error("Invalid E2EE context length");
    const length = new Uint8Array(4);
    new DataView(length.buffer).setUint32(0, context.length, false);
    return concat(sealPrefix, length, context);
  }
  return {
    mechanism: KEY,
    async createKeyPair() {
      return sodium.createKeyPair();
    },
    async wrap(wrappingKey, context, key) {
      checkInput(wrappingKey, context, key);
      if (key.length !== 32) throw new Error("Invalid E2EE wrapped key length");
      const aad = concat(wrapPrefix, context);
      const derived = sodium.hash(wrappingKey, concat(WRAP_KEY_LABEL, aad));
      try {
        const nonce = sodium.randomBytes(24);
        return encodeEnvelope(
          KEY,
          concat(new Uint8Array([1]), nonce, sodium.encrypt(derived, nonce, aad, key)),
        );
      } finally {
        derived.fill(0);
      }
    },
    async unwrap(wrappingKey, context, envelope) {
      checkInput(wrappingKey, context, envelope);
      const payload = decodeEnvelope(KEY, envelope);
      if (payload.length !== 73 || payload[0] !== 1) throw new Error("Invalid E2EE key wrap");
      const aad = concat(wrapPrefix, context);
      const derived = sodium.hash(wrappingKey, concat(WRAP_KEY_LABEL, aad));
      try {
        return sodium.decrypt(derived, payload.subarray(1, 25), aad, payload.subarray(25));
      } catch {
        throw new Error("E2EE authentication failed");
      } finally {
        derived.fill(0);
      }
    },
    async seal(publicKey, context, key) {
      checkInput(publicKey, context, key);
      if (key.length !== 32) throw new Error("Invalid E2EE wrapped key length");
      const plaintext = concat(sealedContext(context), key);
      try {
        return encodeEnvelope(KEY, concat(new Uint8Array([2]), sodium.seal(publicKey, plaintext)));
      } finally {
        plaintext.fill(0);
      }
    },
    async open(device, context, envelope) {
      checkInput(device.publicKey, context, envelope);
      checkInput(device.privateKey, context, envelope);
      const payload = decodeEnvelope(KEY, envelope);
      const prefix = sealedContext(context);
      if (payload[0] !== 2 || payload.length !== 1 + 48 + prefix.length + 32) {
        throw new Error("Invalid E2EE device envelope");
      }
      let plaintext: Uint8Array | undefined;
      try {
        plaintext = sodium.open(device, payload.subarray(1));
        if (
          plaintext.length !== prefix.length + 32 ||
          !prefix.every((byte, index) => byte === plaintext![index])
        ) {
          throw new Error("Invalid E2EE sealed context");
        }
        return Uint8Array.from(plaintext.subarray(prefix.length));
      } catch {
        throw new Error("E2EE authentication failed");
      } finally {
        plaintext?.fill(0);
      }
    },
  };
}
