/**
 * Interface for platform-appropriate auth secret persistence.
 */
export interface AuthSecretStore {
  loadSecret(): Promise<string | null>;
  saveSecret(secret: string): Promise<void>;
  clearSecret(): Promise<void>;
  getOrCreateSecret(): Promise<string>;
}

import {
  authSecretStorageKey,
  formatAuthSecret,
  localFirstSeed,
  parseAuthSecret,
  type AuthSecretScope,
} from "./auth-secret-codec.js";

export {
  authSecretStorageKey,
  AuthSecretFormatError,
  formatAuthSecret,
  localFirstSeed,
  parseAuthSecret,
} from "./auth-secret-codec.js";

/**
 * Generate a new 32-byte auth secret in canonical versioned form.
 * Uses the platform's native CSPRNG.
 */
export function generateAuthSecret(): string {
  const bytes = new Uint8Array(32);
  crypto.getRandomValues(bytes);
  return formatAuthSecret(bytes);
}

export interface BrowserAuthSecretStoreOptions extends AuthSecretScope {
  /** Explicit physical storage key. Prefer appId/profile for portable scoping. */
  key?: string;
  /** Override storage backend (for testing) */
  storage?: Pick<Storage, "getItem" | "setItem" | "removeItem">;
}

function resolveBrowserAuthSecretKey(options: BrowserAuthSecretStoreOptions = {}): string {
  if (options.key) {
    return options.key;
  }

  return authSecretStorageKey(options);
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
  private static globalInstances = new Map<string, BrowserAuthSecretStore>();
  private static storageScopedInstances = new WeakMap<
    Pick<Storage, "getItem" | "setItem" | "removeItem">,
    Map<string, BrowserAuthSecretStore>
  >();
  private readonly key: string;
  private readonly explicitStorage: Pick<Storage, "getItem" | "setItem" | "removeItem"> | undefined;
  private cachedPromise: Promise<string> | null = null;

  constructor(options: BrowserAuthSecretStoreOptions = {}) {
    this.key = resolveBrowserAuthSecretKey(options);
    this.explicitStorage = options.storage;
  }

  private requireStorage(): Pick<Storage, "getItem" | "setItem" | "removeItem"> {
    const storage = this.explicitStorage ?? globalThis.localStorage;
    if (!storage) {
      throw new Error(
        "BrowserAuthSecretStore requires a browser environment. " +
          "Defer reads to a client-only context, or pass `options.storage`.",
      );
    }
    return storage;
  }

  static getDefault(options: BrowserAuthSecretStoreOptions = {}): BrowserAuthSecretStore {
    const storage = options.storage;
    const key = resolveBrowserAuthSecretKey(options);

    if (storage) {
      let instances = BrowserAuthSecretStore.storageScopedInstances.get(storage);
      if (!instances) {
        instances = new Map<string, BrowserAuthSecretStore>();
        BrowserAuthSecretStore.storageScopedInstances.set(storage, instances);
      }

      let instance = instances.get(key);
      if (!instance) {
        instance = new BrowserAuthSecretStore(options);
        instances.set(key, instance);
      }
      return instance;
    }

    let instance = BrowserAuthSecretStore.globalInstances.get(key);
    if (!instance) {
      instance = new BrowserAuthSecretStore(options);
      BrowserAuthSecretStore.globalInstances.set(key, instance);
    }
    return instance;
  }

  async loadSecret(): Promise<string | null> {
    const secret = this.requireStorage().getItem(this.key);
    if (secret === null) return null;
    parseAuthSecret(secret);
    return secret;
  }

  async saveSecret(secret: string): Promise<void> {
    parseAuthSecret(secret);
    this.requireStorage().setItem(this.key, secret);
    this.cachedPromise = Promise.resolve(secret);
  }

  async clearSecret(): Promise<void> {
    this.requireStorage().removeItem(this.key);
    this.cachedPromise = null;
  }

  getOrCreateSecret(): Promise<string> {
    if (!this.cachedPromise) {
      const storage = this.requireStorage();
      const existing = storage.getItem(this.key);
      if (existing) {
        parseAuthSecret(existing);
        this.cachedPromise = Promise.resolve(existing);
      } else {
        const secret = generateAuthSecret();
        storage.setItem(this.key, secret);
        this.cachedPromise = Promise.resolve(secret);
      }
    }
    return this.cachedPromise;
  }

  static loadSecret(options: BrowserAuthSecretStoreOptions = {}): Promise<string | null> {
    return BrowserAuthSecretStore.getDefault(options).loadSecret();
  }

  static saveSecret(secret: string, options: BrowserAuthSecretStoreOptions = {}): Promise<void> {
    return BrowserAuthSecretStore.getDefault(options).saveSecret(secret);
  }

  static clearSecret(options: BrowserAuthSecretStoreOptions = {}): Promise<void> {
    return BrowserAuthSecretStore.getDefault(options).clearSecret();
  }

  static getOrCreateSecret(options: BrowserAuthSecretStoreOptions = {}): Promise<string> {
    return BrowserAuthSecretStore.getDefault(options).getOrCreateSecret();
  }
}

export const browserAuthSecretStore: AuthSecretStore = BrowserAuthSecretStore.getDefault();
