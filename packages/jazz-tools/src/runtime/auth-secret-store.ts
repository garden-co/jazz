/**
 * Interface for platform-appropriate auth secret persistence.
 */
export interface AuthSecretStore {
  loadSecret(): Promise<string | null>;
  saveSecret(secret: string): Promise<void>;
  clearSecret(): Promise<void>;
  getOrCreateSecret(): Promise<string>;
}

const DEFAULT_KEY = "jazz-auth-secret";

/**
 * Generate a new 32-byte auth secret as a base64url string.
 * Uses the platform's native CSPRNG.
 */
export function generateAuthSecret(): string {
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

export interface BrowserAuthSecretStoreOptions {
  /** localStorage key name (default: "jazz-auth-secret") */
  key?: string;
  /** Override storage backend (for testing) */
  storage?: Pick<Storage, "getItem" | "setItem" | "removeItem">;
}

/**
 * AuthSecretStore backed by localStorage.
 *
 * Singleton — call static methods directly: `BrowserAuthSecretStore.getOrCreateSecret()`.
 *
 * Uses a check-then-write pattern; not atomic across concurrent tabs on
 * first visit. Apps that need strict cross-tab guarantees can use a custom
 * AuthSecretStore with IndexedDB transactions or BroadcastChannel coordination.
 */
export class BrowserAuthSecretStore implements AuthSecretStore {
  private static defaultInstance: BrowserAuthSecretStore | null = null;
  private readonly key: string;
  private readonly storage: Pick<Storage, "getItem" | "setItem" | "removeItem">;
  private cachedPromise: Promise<string> | null = null;

  constructor(options: BrowserAuthSecretStoreOptions = {}) {
    this.key = options.key ?? DEFAULT_KEY;
    this.storage = options.storage ?? globalThis.localStorage;
  }

  private static getDefault(): BrowserAuthSecretStore {
    if (!BrowserAuthSecretStore.defaultInstance) {
      BrowserAuthSecretStore.defaultInstance = new BrowserAuthSecretStore();
    }
    return BrowserAuthSecretStore.defaultInstance;
  }

  async loadSecret(): Promise<string | null> {
    return this.storage.getItem(this.key);
  }

  async saveSecret(secret: string): Promise<void> {
    this.storage.setItem(this.key, secret);
  }

  async clearSecret(): Promise<void> {
    this.storage.removeItem(this.key);
    this.cachedPromise = null;
  }

  getOrCreateSecret(): Promise<string> {
    if (!this.cachedPromise) {
      const existing = this.storage.getItem(this.key);
      if (existing) {
        this.cachedPromise = Promise.resolve(existing);
      } else {
        const secret = generateAuthSecret();
        this.storage.setItem(this.key, secret);
        this.cachedPromise = Promise.resolve(secret);
      }
    }
    return this.cachedPromise;
  }

  static loadSecret(): Promise<string | null> {
    return BrowserAuthSecretStore.getDefault().loadSecret();
  }

  static saveSecret(secret: string): Promise<void> {
    return BrowserAuthSecretStore.getDefault().saveSecret(secret);
  }

  static clearSecret(): Promise<void> {
    return BrowserAuthSecretStore.getDefault().clearSecret();
  }

  static getOrCreateSecret(): Promise<string> {
    return BrowserAuthSecretStore.getDefault().getOrCreateSecret();
  }
}
