// Hashing, signatures and UUIDs (Rust: blake3, sha2, sha1_smol, ed25519-dalek, uuid).
import { blake3 as nobleBlake3 } from "@noble/hashes/blake3";
import { sha256 as nobleSha256, sha512 as nobleSha512 } from "@noble/hashes/sha2";
import { sha1 as nobleSha1 } from "@noble/hashes/sha1";
import { ed25519 } from "@noble/curves/ed25519";
import { type Bytes, concatBytes, toHex } from "./bytes.js";

export const blake3 = (data: Bytes, outLen = 32): Bytes => nobleBlake3(data, { dkLen: outLen });
export const blake3Keyed = (key: Bytes, data: Bytes): Bytes => nobleBlake3(data, { key });
export class Blake3Hasher {
  private readonly h = nobleBlake3.create({});
  update(data: Bytes): this {
    this.h.update(data);
    return this;
  }
  finalize(): Bytes {
    return this.h.digest();
  }
}
export const sha256 = (data: Bytes): Bytes => nobleSha256(data);
export const sha512 = (data: Bytes): Bytes => nobleSha512(data);

export const ed25519Sign = (message: Bytes, secretKey: Bytes): Bytes =>
  ed25519.sign(message, secretKey);
export const ed25519Verify = (signature: Bytes, message: Bytes, publicKey: Bytes): boolean => {
  try {
    return ed25519.verify(signature, message, publicKey);
  } catch {
    return false;
  }
};
export const ed25519PublicKey = (secretKey: Bytes): Bytes => ed25519.getPublicKey(secretKey);
export const randomBytes = (n: number): Bytes => crypto.getRandomValues(new Uint8Array(n));

export type Uuid = Bytes; // 16 bytes
export function uuidToString(u: Uuid): string {
  const h = toHex(u);
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
}
export function uuidFromString(s: string): Uuid {
  const h = s.replace(/-/g, "");
  if (h.length !== 32) throw new Error(`invalid UUID: ${s}`);
  const out = new Uint8Array(16);
  for (let i = 0; i < 16; i++) out[i] = parseInt(h.slice(i * 2, i * 2 + 2), 16);
  return out;
}
export function uuidV4(): Uuid {
  const b = randomBytes(16);
  b[6] = (b[6] & 0x0f) | 0x40;
  b[8] = (b[8] & 0x3f) | 0x80;
  return b;
}
export function uuidV5(namespace: Uuid, name: Bytes): Uuid {
  const b = nobleSha1(concatBytes(namespace, name)).slice(0, 16);
  b[6] = (b[6] & 0x0f) | 0x50;
  b[8] = (b[8] & 0x3f) | 0x80;
  return b;
}
export function uuidV7(nowMs = Date.now()): Uuid {
  const b = randomBytes(16);
  let ms = nowMs;
  for (let i = 5; i >= 0; i--) {
    b[i] = ms % 256;
    ms = Math.floor(ms / 256);
  }
  b[6] = (b[6] & 0x0f) | 0x70;
  b[8] = (b[8] & 0x3f) | 0x80;
  return b;
}
