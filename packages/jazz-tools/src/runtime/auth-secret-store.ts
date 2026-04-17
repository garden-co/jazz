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
  /** Optional app identifier to namespace the default key. */
  appId?: string;
  /** Optional principal identifier to isolate secrets per user. */
  userId?: string | null;
  /** Optional session identifier for per-session isolation. */
  sessionId?: string | null;
  /** Override storage backend (for testing) */
  storage?: Pick<Storage, "getItem" | "setItem" | "removeItem">;
}

function normalizeScopeSegment(value?: string | null): string | null {
  if (typeof value !== "string") {
    return null;
  }

  const trimmed = value.trim();
  if (trimmed.length === 0) {
    return null;
  }

  return encodeURIComponent(trimmed);
}

function resolveBrowserAuthSecretKey(options: BrowserAuthSecretStoreOptions = {}): string {
  if (options.key) {
    return options.key;
  }

  const scopeSegments = [
    normalizeScopeSegment(options.appId),
    normalizeScopeSegment(options.userId),
    normalizeScopeSegment(options.sessionId),
  ].filter((segment): segment is string => segment !== null);

  if (scopeSegments.length === 0) {
    return DEFAULT_KEY;
  }

  return `${DEFAULT_KEY}:${scopeSegments.join(":")}`;
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
  private readonly storage: Pick<Storage, "getItem" | "setItem" | "removeItem">;
  private cachedPromise: Promise<string> | null = null;

  constructor(options: BrowserAuthSecretStoreOptions = {}) {
    this.key = resolveBrowserAuthSecretKey(options);
    this.storage = options.storage ?? globalThis.localStorage;
  }

  private static getDefault(options: BrowserAuthSecretStoreOptions = {}): BrowserAuthSecretStore {
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
    return this.storage.getItem(this.key);
  }

  async saveSecret(secret: string): Promise<void> {
    this.storage.setItem(this.key, secret);
    this.cachedPromise = Promise.resolve(secret);
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
