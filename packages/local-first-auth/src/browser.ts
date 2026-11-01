import { createSecretStore, type SecretStore, type SecretStorage } from "./store.js";

export interface BrowserSecretStoreOptions {
  /** The localStorage key, e.g. `/jazz`. localStorage is already separate per origin. */
  key: string;
  /** Older keys to adopt an existing secret from. See {@link createSecretStore}. */
  migrateFrom?: readonly string[];
  /** Defaults to `globalThis.localStorage`. */
  storage?: SecretStorage;
}

const lazyLocalStorage: SecretStorage = {
  getItem: (key) => requireLocalStorage().getItem(key),
  setItem: (key, value) => requireLocalStorage().setItem(key, value),
  removeItem: (key) => requireLocalStorage().removeItem(key),
};

function requireLocalStorage(): Storage {
  const storage = globalThis.localStorage;
  if (!storage) {
    throw new Error(
      "browserSecretStore needs localStorage. Use it only in the browser, or pass `storage`.",
    );
  }
  return storage;
}

/**
 * A secret store in localStorage. Not atomic across tabs on the very first
 * visit; two tabs opened at once may each create a secret and the last write wins.
 */
export function browserSecretStore(options: BrowserSecretStoreOptions): SecretStore {
  return createSecretStore({
    key: options.key,
    storage: options.storage ?? lazyLocalStorage,
    migrateFrom: options.migrateFrom,
  });
}

export * from "./store.js";
