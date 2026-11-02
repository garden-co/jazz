import { sha256 } from "@noble/hashes/sha2.js";
import { base64urlDecode, base64urlEncode, utf8 } from "./encoding.js";

/**
 * The only portable at-rest and export form of a local-first secret:
 * `jazz-auth-v1:` followed by 43 unpadded base64url characters (32 bytes).
 *
 * The `jazz-` prefix is kept from the format's origin so that every secret,
 * recovery phrase and passkey backup created so far keeps working.
 */
export const SECRET_PREFIX = "jazz-auth-v1:";
const SECRET_BYTES = 32;
const SECRET_PAYLOAD = /^[A-Za-z0-9_-]{43}$/;

export class SecretFormatError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SecretFormatError";
  }
}

/** Format exactly 32 bytes of entropy as a secret. */
export function formatSecret(bytes: Uint8Array): string {
  if (bytes.byteLength !== SECRET_BYTES) {
    throw new SecretFormatError(`A local-first secret must contain exactly ${SECRET_BYTES} bytes`);
  }
  return `${SECRET_PREFIX}${base64urlEncode(bytes)}`;
}

/** Strictly parse the canonical form and return its 32 bytes. */
export function parseSecret(secret: string): Uint8Array {
  if (typeof secret !== "string" || !secret.startsWith(SECRET_PREFIX)) {
    throw new SecretFormatError(`A local-first secret must start with ${SECRET_PREFIX}`);
  }
  const payload = secret.slice(SECRET_PREFIX.length);
  if (!SECRET_PAYLOAD.test(payload)) {
    throw new SecretFormatError("A local-first secret must use 43 unpadded base64url characters");
  }
  let bytes: Uint8Array;
  try {
    bytes = base64urlDecode(payload, true);
  } catch {
    throw new SecretFormatError("A local-first secret is not valid base64url");
  }
  if (bytes.byteLength !== SECRET_BYTES || formatSecret(bytes) !== secret) {
    throw new SecretFormatError("A local-first secret is not canonical");
  }
  return bytes;
}

/** A new secret from the platform CSPRNG (`crypto.getRandomValues`). */
export function generateSecret(): string {
  const random = globalThis.crypto;
  if (typeof random?.getRandomValues !== "function") {
    throw new Error(
      "generateSecret needs crypto.getRandomValues. On React Native, install a polyfill such as react-native-get-random-values.",
    );
  }
  const bytes = new Uint8Array(SECRET_BYTES);
  random.getRandomValues(bytes);
  return formatSecret(bytes);
}

/**
 * The storage key for a secret in `scope` (and optional `profile`). The scope is
 * any string the app picks to keep its users apart from other apps' on the same
 * storage, such as an app ID or an endpoint. The derivation is fixed: Jazz has
 * used it since before this package, so the same scope finds the same secret.
 */
export function secretStorageKey(scope: string | null, profile: string | null = null): string {
  // The member names and order are part of the format.
  const material = JSON.stringify({ appId: scope, profile });
  return `jazz-auth-store-v1-${base64urlEncode(sha256(utf8(material)))}`;
}
