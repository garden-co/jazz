export type AuthSecretStorage = {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
  removeItem(key: string): void;
};

export type AuthSecretStore = {
  getOrCreateSecret(appId?: string): string;
  saveSecret(secret: string, appId?: string): void;
  clearSecret(appId?: string): void;
};

export type AuthSecretStoreOptions = {
  appId?: string;
  storage?: AuthSecretStorage | null;
  keyPrefix?: string;
};

export type LocalFirstAuthState = {
  secret: string | null;
  isLoading: boolean;
  error?: unknown;
};

export type UseLocalFirstAuthOptions = {
  appId?: string;
};

export function generateAuthSecret(): string {
  const bytes = new Uint8Array(32);
  const crypto = globalThis.crypto;
  if (crypto?.getRandomValues) {
    crypto.getRandomValues(bytes);
  } else {
    for (let index = 0; index < bytes.length; index += 1) {
      bytes[index] = Math.floor(Math.random() * 256);
    }
  }
  return base64Url(bytes);
}

export function createAuthSecretStore(options: AuthSecretStoreOptions = {}): AuthSecretStore {
  const storage = options.storage === undefined ? browserLocalStorage() : options.storage;
  const keyPrefix = options.keyPrefix ?? "jazz-tools:local-first-secret";
  const defaultAppId = options.appId ?? defaultBrowserAppId();
  const memory = new Map<string, string>();

  const keyFor = (appId?: string) => `${keyPrefix}:${encodeURIComponent(appId ?? defaultAppId)}`;
  const readSecret = (key: string) => {
    try {
      return storage?.getItem(key) ?? memory.get(key) ?? null;
    } catch {
      return memory.get(key) ?? null;
    }
  };
  const writeSecret = (key: string, secret: string) => {
    memory.set(key, secret);
    try {
      storage?.setItem(key, secret);
    } catch {
      // Keep the in-memory copy for private browsing, denied storage, and SSR-like shells.
    }
  };

  return {
    getOrCreateSecret(appId) {
      const key = keyFor(appId);
      const existing = readSecret(key);
      if (existing) return existing;
      const secret = generateAuthSecret();
      writeSecret(key, secret);
      return secret;
    },
    saveSecret(secret, appId) {
      writeSecret(keyFor(appId), secret);
    },
    clearSecret(appId) {
      const key = keyFor(appId);
      memory.delete(key);
      try {
        storage?.removeItem(key);
      } catch {
        // Clearing the memory fallback is enough when persistent storage is unavailable.
      }
    },
  };
}

export const authSecretStore = createAuthSecretStore();

export function createUseLocalFirstAuth(store: AuthSecretStore) {
  return function useLocalFirstAuth(options: UseLocalFirstAuthOptions = {}): LocalFirstAuthState {
    try {
      return {
        secret: store.getOrCreateSecret(options.appId),
        isLoading: false,
      };
    } catch (error) {
      return {
        secret: null,
        isLoading: false,
        error,
      };
    }
  };
}

function browserLocalStorage(): AuthSecretStorage | null {
  try {
    const browserGlobal = globalThis as typeof globalThis & {
      window?: { localStorage?: AuthSecretStorage };
    };
    return browserGlobal.window?.localStorage ?? null;
  } catch {
    return null;
  }
}

function defaultBrowserAppId(): string {
  const nodeGlobal = globalThis as typeof globalThis & {
    process?: { env?: Record<string, string | undefined> };
  };
  const env = nodeGlobal.process?.env;
  return env?.NEXT_PUBLIC_JAZZ_APP_ID ?? env?.JAZZ_APP_ID ?? "default";
}

function base64Url(bytes: Uint8Array): string {
  if (typeof Buffer !== "undefined") return Buffer.from(bytes).toString("base64url");
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/u, "");
}
