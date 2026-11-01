import {
  SECRET_PREFIX,
  SecretFormatError,
  formatSecret,
  parseSecret,
  secretStorageKey,
} from "@garden-co/local-first-auth";

/** The only portable at-rest/export representation of a local-first root. */
export const AUTH_SECRET_PREFIX = SECRET_PREFIX;

export { SecretFormatError as AuthSecretFormatError } from "@garden-co/local-first-auth";

/** Format exactly 256 bits of root entropy for storage, backup, or export. */
export const formatAuthSecret: (bytes: Uint8Array) => string = formatSecret;

/** Strictly parse the canonical versioned representation. */
export const parseAuthSecret: (secret: string) => Uint8Array = parseSecret;

/** @internal Stable seed text for self-signed token minting. */
export function authSecretSeedForMinting(secret: string): string {
  parseAuthSecret(secret);
  return secret.slice(AUTH_SECRET_PREFIX.length);
}

export interface AuthSecretScope {
  /** Stable public application identifier. */
  appId?: string | null;
  /** A local opaque profile label; never a server session or external subject. */
  profile?: string | null;
}

function scopePart(value: string | null | undefined, name: string): string | null {
  if (value === null || value === undefined) return null;
  if (typeof value !== "string") {
    throw new SecretFormatError(`Jazz auth secret scope ${name} must be a string or null`);
  }
  // Scope strings are application identities, not human recovery phrases. Their
  // exact UTF-16 values are semantic: trimming or Unicode normalization could
  // silently make two independently configured apps share one root.
  return value;
}

/**
 * A stable, PII-free physical key for a logical app/profile identity scope:
 * the package's storage key with the app ID as its scope.
 */
export function authSecretStorageKey(scope: AuthSecretScope = {}): string {
  return secretStorageKey(scopePart(scope.appId, "appId"), scopePart(scope.profile, "profile"));
}
