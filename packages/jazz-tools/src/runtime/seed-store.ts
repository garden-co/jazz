/**
 * Interface for platform-appropriate seed persistence.
 */
export interface SeedStore {
  loadSeed(): Promise<string | null>;
  saveSeed(seed: string): Promise<void>;
  clearSeed(): Promise<void>;
  getOrCreateSeed(): Promise<string>;
}

const DEFAULT_KEY = "jazz-seed";

/**
 * Generate a new 32-byte seed as a base64url string.
 * Uses the platform's native CSPRNG.
 */
export function generateSeed(): string {
  const bytes = new Uint8Array(32);
  crypto.getRandomValues(bytes);
  return uint8ArrayToBase64url(bytes);
}

function uint8ArrayToBase64url(bytes: Uint8Array): string {
  let binary = "";
  for (const b of bytes) {
    binary += String.fromCharCode(b);
  }
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

export interface LocalStorageSeedStoreOptions {
  /** localStorage key name (default: "jazz-seed") */
  key?: string;
  /** Override storage backend (for testing) */
  storage?: Pick<Storage, "getItem" | "setItem" | "removeItem">;
}

/**
 * SeedStore backed by localStorage.
 *
 * Uses a check-then-write pattern; not atomic across concurrent tabs on
 * first visit. Apps that need strict cross-tab guarantees can use a custom
 * SeedStore with IndexedDB transactions or BroadcastChannel coordination.
 */
export class LocalStorageSeedStore implements SeedStore {
  private readonly key: string;
  private readonly storage: Pick<Storage, "getItem" | "setItem" | "removeItem">;

  constructor(options: LocalStorageSeedStoreOptions = {}) {
    this.key = options.key ?? DEFAULT_KEY;
    this.storage = options.storage ?? globalThis.localStorage;
  }

  async loadSeed(): Promise<string | null> {
    return this.storage.getItem(this.key);
  }

  async saveSeed(seed: string): Promise<void> {
    this.storage.setItem(this.key, seed);
  }

  async clearSeed(): Promise<void> {
    this.storage.removeItem(this.key);
  }

  async getOrCreateSeed(): Promise<string> {
    const existing = this.storage.getItem(this.key);
    if (existing) return existing;

    const seed = generateSeed();
    this.storage.setItem(this.key, seed);
    return seed;
  }
}
