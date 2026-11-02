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
  /**
   * Remove the secret. Back it up first if the identity should be recoverable.
   * Subscribers then see `secret: null`; the next {@link getOrCreate} creates a
   * new identity.
   */
  clear(): Promise<void>;
  /**
   * Current state for UI bindings. The first subscriber starts loading with
   * {@link getOrCreate}, so subscribing creates an identity on first use.
   */
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
 * Operations on one key run one at a time, even across store instances (for
 * example two inline storage adapters over the same localStorage).
 */
const queues = new Map<string, Promise<unknown>>();

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
  const serialize = <T>(operation: () => Promise<T>): Promise<T> => {
    const result = (queues.get(key) ?? Promise.resolve()).then(operation);
    const tail = result.catch(() => {});
    queues.set(key, tail);
    void tail.then(() => {
      if (queues.get(key) === tail) queues.delete(key);
    });
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
  const reload = (create: boolean) => {
    if (listeners.size === 0) return;
    const current = ++generation;
    if (!state.isLoading) setState({ ...state, isLoading: true });
    (create ? store.getOrCreate() : store.load()).then(
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
        reload(false);
      }
    },
    async clear() {
      try {
        await serialize(async () => storage.removeItem(key));
      } finally {
        reload(false);
      }
    },
    getState: () => state,
    subscribe(listener) {
      listeners.add(listener);
      if (listeners.size === 1) reload(true);
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
