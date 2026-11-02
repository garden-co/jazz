import { createSecretStore, type SecretStore, type SecretStorage } from "./store.js";

export interface BrowserSecretStoreOptions {
  /**
   * Keeps apps on one origin apart. See {@link createSecretStore}. localStorage
   * is already separate per origin, so a single-app origin can omit it.
   */
  scope?: string | null;
  profile?: string | null;
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
    scope: options.scope,
    profile: options.profile,
    storage: options.storage ?? lazyLocalStorage,
  });
}

export * from "./store.js";
