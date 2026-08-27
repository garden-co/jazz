import { sha256 } from "@noble/hashes/sha2.js";

/** The only portable at-rest/export representation of a local-first root. */
export const AUTH_SECRET_PREFIX = "jazz-auth-v1:";
const AUTH_SECRET_BYTES = 32;
const AUTH_SECRET_PAYLOAD = 43;
const AUTH_SECRET_PATTERN = /^[A-Za-z0-9_-]{43}$/;
const STORE_KEY_PREFIX = "jazz-auth-store-v1-";

export class AuthSecretFormatError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "AuthSecretFormatError";
  }
}

function bytesToBase64url(bytes: Uint8Array): string {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

function base64urlToBytes(payload: string): Uint8Array {
  const padding = "=".repeat((4 - (payload.length % 4)) % 4);
  const binary = atob(payload.replace(/-/g, "+").replace(/_/g, "/") + padding);
  return Uint8Array.from(binary, (char) => char.charCodeAt(0));
}

/** Format exactly 256 bits of root entropy for storage, backup, or export. */
export function formatAuthSecret(bytes: Uint8Array): string {
  if (bytes.byteLength !== AUTH_SECRET_BYTES) {
    throw new AuthSecretFormatError(
      `A Jazz auth secret must contain exactly ${AUTH_SECRET_BYTES} bytes`,
    );
  }
  return `${AUTH_SECRET_PREFIX}${bytesToBase64url(bytes)}`;
}

/** Strictly parse the canonical versioned representation. */
export function parseAuthSecret(secret: string): Uint8Array {
  if (typeof secret !== "string" || !secret.startsWith(AUTH_SECRET_PREFIX)) {
    throw new AuthSecretFormatError(`A Jazz auth secret must start with ${AUTH_SECRET_PREFIX}`);
  }
  const payload = secret.slice(AUTH_SECRET_PREFIX.length);
  if (!AUTH_SECRET_PATTERN.test(payload) || payload.length !== AUTH_SECRET_PAYLOAD) {
    throw new AuthSecretFormatError("A Jazz auth secret must use 43 unpadded base64url characters");
  }
  let bytes: Uint8Array;
  try {
    bytes = base64urlToBytes(payload);
  } catch {
    throw new AuthSecretFormatError("A Jazz auth secret is not valid base64url");
  }
  if (bytes.byteLength !== AUTH_SECRET_BYTES || formatAuthSecret(bytes) !== secret) {
    throw new AuthSecretFormatError("A Jazz auth secret is not canonical");
  }
  return bytes;
}

/**
 * The stable byte-for-byte seed handed to the self-signed-token implementation.
 * Internal callers that still supply an untyped seed retain their existing
 * low-level behavior; all stored/exported roots are parsed strictly above.
 */
export function localFirstSeed(secret: string): string {
  if (!secret.startsWith(AUTH_SECRET_PREFIX)) return secret;
  parseAuthSecret(secret);
  return secret.slice(AUTH_SECRET_PREFIX.length);
}

export function generateCanonicalAuthSecret(randomBytes: Uint8Array): string {
  return formatAuthSecret(randomBytes);
}

export interface AuthSecretScope {
  /** Stable public application identifier. */
  appId?: string | null;
  /** A local opaque profile label; never a server session or external subject. */
  profile?: string | null;
}

function normalizedScopePart(value: string | null | undefined): string | null {
  if (typeof value !== "string") return null;
  const normalized = value.normalize("NFKD").trim();
  return normalized.length === 0 ? null : normalized;
}

/**
 * A stable, PII-free physical key for a logical app/profile identity scope.
 * The preimage's member order is part of the format contract.
 */
export function authSecretStorageKey(scope: AuthSecretScope = {}): string {
  const canonicalScope = JSON.stringify({
    appId: normalizedScopePart(scope.appId),
    profile: normalizedScopePart(scope.profile),
  });
  return `${STORE_KEY_PREFIX}${bytesToBase64url(sha256(new TextEncoder().encode(canonicalScope)))}`;
}
