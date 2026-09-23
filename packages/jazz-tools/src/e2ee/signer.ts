import { encodeEnvelope, decodeEnvelope } from "./envelope.js";
import type { DeviceKeyPair, DeviceSigner } from "./types.js";

/** Internal platform seam for the same libsodium implementation. */
export interface SodiumSigningPrimitives {
  createKeyPair(): DeviceKeyPair;
  fromSeed(seed: Uint8Array): DeviceKeyPair;
  sign(key: Uint8Array, message: Uint8Array): Uint8Array;
  verify(key: Uint8Array, message: Uint8Array, signature: Uint8Array): boolean;
}

export function createSodiumDeviceSigner(sodium: SodiumSigningPrimitives): DeviceSigner {
  const mechanism = Object.freeze({ id: "jazz.sodium.sign", version: 1 });
  const header = encodeEnvelope(mechanism, new Uint8Array());
  const frame = (record: Uint8Array) => {
    if (!(record instanceof Uint8Array)) throw new Error("Invalid E2EE signing record");
    const message = new Uint8Array(header.length + record.length);
    message.set(header);
    message.set(record, header.length);
    return message;
  };
  return {
    mechanism,
    async createKeyPair() {
      return sodium.createKeyPair();
    },
    async sign(privateKey, record) {
      if (!(privateKey instanceof Uint8Array) || privateKey.length !== 64) {
        throw new Error("Invalid E2EE signing key");
      }
      // libsodium's secret contains seed + public key: reject inconsistent pairs.
      const checked = sodium.fromSeed(privateKey.subarray(0, 32));
      try {
        if (
          checked.privateKey.length !== 64 ||
          checked.privateKey.some((byte, i) => byte !== privateKey[i])
        ) {
          throw new Error("Inconsistent E2EE signing key");
        }
        return encodeEnvelope(mechanism, sodium.sign(checked.privateKey, frame(record)));
      } finally {
        checked.privateKey.fill(0);
      }
    },
    async verify(publicKey, record, signature) {
      if (
        !(publicKey instanceof Uint8Array) ||
        publicKey.length !== 32 ||
        !(record instanceof Uint8Array) ||
        !(signature instanceof Uint8Array)
      )
        return false;
      let payload: Uint8Array;
      try {
        payload = decodeEnvelope(mechanism, signature);
      } catch {
        return false;
      }
      if (payload.length !== 64) return false;
      return sodium.verify(publicKey, frame(record), payload);
    },
  };
}
