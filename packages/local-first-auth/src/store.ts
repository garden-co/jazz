import { generateSecret, parseSecret, secretStorageKey } from "./secret.js";

/** Where a store keeps its secret. Sync or async; localStorage, SecureStore and friends fit. */
export interface SecretStorage {
  getItem(key: string): string | null | Promise<string | null>;
  setItem(key: string, value: string): void | Promise<void>;
  removeItem(key: string): void | Promise<void>;
}

export interface SecretStoreOptions {
  storage: SecretStorage;
  /**
   * Keeps this app's secret apart from other apps on the same storage: any
   * string, such as an app ID or an endpoint. Jazz passes its app ID. Omit it
   * when the storage already belongs to one app.
   */
  scope?: string | null;
  /** A second namespace inside the scope, e.g. for several accounts on one device. */
  profile?: string | null;
}

export interface SecretState {
  secret: string | null;
  isLoading: boolean;
  /** Set when loading failed, for example because storage held a malformed secret. */
  error: Error | null;
}

export interface SecretStore {
  /** The storage key, derived from scope and profile by {@link secretStorageKey}. */
  readonly key: string;
  /** The stored secret, or null. */
  load(): Promise<string | null>;
  /** The stored secret, creating and persisting one on first use. */
  getOrCreate(): Promise<string>;
  /** Replace the stored secret, e.g. after restoring a backup. */
  save(secret: string): Promise<void>;
  /** Remove the secret. Back it up first if the identity should be recoverable. */
  clear(): Promise<void>;
  /** Current state for UI bindings. Loading starts with the first subscriber. */
  getState(): SecretState;
  subscribe(listener: () => void): () => void;
}

const initialState: SecretState = {
  secret: null,
  isLoading: true,
  error: null,
};

const stores = new WeakMap<SecretStorage, Map<string, SecretStore>>();

/**
 * The store for `scope` in `storage`. Calls with the same storage, scope and
 * profile share one instance, so every consumer sees saves and clears.
 */
export function createSecretStore(options: SecretStoreOptions): SecretStore {
  const key = secretStorageKey(options.scope ?? null, options.profile ?? null);
  let byKey = stores.get(options.storage);
  if (!byKey) {
    byKey = new Map();
    stores.set(options.storage, byKey);
  }
  let store = byKey.get(key);
  if (!store) {
    store = newSecretStore(key, options.storage);
    byKey.set(key, store);
  }
  return store;
}

function toError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

function newSecretStore(key: string, storage: SecretStorage): SecretStore {
  let tail: Promise<unknown> = Promise.resolve();
  const serialize = <T>(operation: () => Promise<T>): Promise<T> => {
    const result = tail.then(operation);
    tail = result.catch(() => {});
    return result;
  };

  const read = async (): Promise<string | null> => {
    const current = await storage.getItem(key);
    if (current !== null) parseSecret(current);
    return current;
  };

  const listeners = new Set<() => void>();
  let state = initialState;
  let generation = 0;
  const setState = (next: SecretState) => {
    state = next;
    for (const listener of listeners) listener();
  };
  const reload = () => {
    if (listeners.size === 0) return;
    const current = ++generation;
    if (!state.isLoading) setState({ ...state, isLoading: true });
    store.getOrCreate().then(
      (secret) => current === generation && setState({ secret, isLoading: false, error: null }),
      (error) =>
        current === generation &&
        setState({ secret: null, isLoading: false, error: toError(error) }),
    );
  };

  const store: SecretStore = {
    key,
    load: () => serialize(read),
    getOrCreate: () =>
      serialize(async () => {
        const existing = await read();
        if (existing !== null) return existing;
        const secret = generateSecret();
        await storage.setItem(key, secret);
        return secret;
      }),
    async save(secret) {
      parseSecret(secret);
      try {
        await serialize(async () => storage.setItem(key, secret));
      } finally {
        reload();
      }
    },
    async clear() {
      try {
        await serialize(async () => storage.removeItem(key));
      } finally {
        reload();
      }
    },
    getState: () => state,
    subscribe(listener) {
      listeners.add(listener);
      if (listeners.size === 1) reload();
      return () => {
        listeners.delete(listener);
        if (listeners.size === 0) {
          generation += 1;
          state = initialState;
        }
      };
    },
  };
  return store;
}
