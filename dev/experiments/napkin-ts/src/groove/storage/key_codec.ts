import { type Bytes, utf8, fromUtf8 } from "../../support/bytes.js";
import { StorageError, validatePhysicalStorageName } from "./index.js";

const KEY_VERSION = 1;

export function encodeColumnFamilyKey(cf: string, key: Bytes): Bytes {
  validatePhysicalStorageName(cf);
  const cfBytes = utf8(cf);
  const out = new Uint8Array(3 + cfBytes.length + key.length);
  out[0] = KEY_VERSION;
  out[1] = cfBytes.length >> 8;
  out[2] = cfBytes.length & 0xff;
  out.set(cfBytes, 3);
  out.set(key, 3 + cfBytes.length);
  return out;
}

export function decodeColumnFamilyKey(encoded: Bytes): [string, Bytes] {
  if (encoded.length < 3 || encoded[0] !== KEY_VERSION)
    throw StorageError.invalidKey("unsupported IndexedDB storage key");
  const keyOffset = 3 + ((encoded[1] << 8) | encoded[2]);
  if (encoded.length < keyOffset) throw StorageError.invalidKey("truncated IndexedDB storage key");
  let cf: string;
  try {
    cf = fromUtf8(encoded.subarray(3, keyOffset));
  } catch {
    throw StorageError.invalidKey("invalid UTF-8 column family name");
  }
  return [cf, encoded.subarray(keyOffset)];
}

export function incrementBytes(bytes: Bytes): Bytes | undefined {
  for (let i = bytes.length - 1; i >= 0; i--) {
    if (bytes[i] !== 0xff) {
      const next = bytes.slice(0, i + 1);
      next[i]++;
      return next;
    }
  }
  return undefined;
}

export const prefixUpperBound = incrementBytes;
