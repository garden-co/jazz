/**
 * Self-signed auth helpers.
 *
 * All crypto is performed in Rust via WASM or NAPI bindings.
 * This module handles seed persistence in client-side storage
 * and exposes a clean API for the rest of the TypeScript client.
 */

export interface SeedStorageLike {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
}

export interface StoredIdentitySeed {
  version: 1;
  /** base64url-encoded 32-byte root seed */
  seed: string;
}

const SEED_STORAGE_PREFIX = "jazz:identity-seed:";

// These are populated by the WASM or NAPI module at init time.
let _generateSeed: () => string;
let _deriveSelfSignedUserId: (seedB64: string) => string;
let _mintSelfSignedToken: (seedB64: string, audience: string, ttlSecs?: number) => string;

/**
 * Initialize the self-signed auth module with WASM or NAPI bindings.
 *
 * Must be called before any other function in this module.
 */
export function initSelfSignedAuth(bindings: {
  generateIdentitySeed: () => string;
  deriveSelfSignedUserId: (seedB64: string) => string;
  mintSelfSignedToken: (seedB64: string, audience: string, ttlSecs?: number) => string;
}): void {
  _generateSeed = bindings.generateIdentitySeed;
  _deriveSelfSignedUserId = bindings.deriveSelfSignedUserId;
  _mintSelfSignedToken = bindings.mintSelfSignedToken;
}

function tryGetStorage(storage?: SeedStorageLike): SeedStorageLike | undefined {
  if (storage) return storage;
  if (typeof globalThis === "undefined") return undefined;
  try {
    return (globalThis as { localStorage?: SeedStorageLike }).localStorage;
  } catch {
    return undefined;
  }
}

/**
 * Load an existing identity seed from storage, or generate and persist a new one.
 *
 * Seed generation happens in Rust (WASM/NAPI). In browser WASM, Rust randomness
 * is backed by the JS/WASM getrandom bridge.
 */
export function loadOrCreateIdentitySeed(
  appId: string,
  options?: { storage?: SeedStorageLike },
): StoredIdentitySeed {
  const storage = tryGetStorage(options?.storage);
  const storageKey = `${SEED_STORAGE_PREFIX}${appId}`;

  if (storage) {
    try {
      const existing = storage.getItem(storageKey);
      if (existing) {
        const parsed = JSON.parse(existing) as StoredIdentitySeed;
        if (parsed.version === 1 && parsed.seed) {
          return parsed;
        }
      }
    } catch {
      // Corrupted — regenerate.
    }
  }

  const seedB64 = _generateSeed();
  const stored: StoredIdentitySeed = { version: 1, seed: seedB64 };

  if (storage) {
    try {
      storage.setItem(storageKey, JSON.stringify(stored));
    } catch {
      // Ignore write failures (private mode/quota).
    }
  }

  return stored;
}

/**
 * Derive the canonical userId from a seed. Calls Rust: HKDF -> Ed25519 -> UUIDv5.
 */
export function deriveSelfSignedUserId(seedB64: string): string {
  return _deriveSelfSignedUserId(seedB64);
}

/**
 * Mint a self-signed JWT from a seed. Calls Rust for all crypto.
 *
 * The returned token is ready to use as a bearer token (`Authorization: Bearer <token>`).
 */
export function mintSelfSignedToken(seedB64: string, audience: string, ttlSecs?: number): string {
  return _mintSelfSignedToken(seedB64, audience, ttlSecs);
}
