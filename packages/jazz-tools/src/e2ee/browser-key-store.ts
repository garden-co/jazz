import { isBrowserHostRuntime } from "../accounts/browser-host.js";
import type { AccountStore } from "../accounts/persistence.js";

interface KeyStoreScope {
  /** Canonical account registry URL, including the application ID. */
  registry: string;
  env: string;
  accountId: string;
}

// Durable format v1: database/store names and the tuple key are storage contracts.
// Values retain the existing versioned E2EE AccountStore string encodings.
const DATABASE = "jazz-e2ee-keys-v1";
const RECORDS = "accounts";

/** @internal A custom store always wins, including on native and server hosts. */
export function resolveE2eeStore(
  store: AccountStore | undefined,
  scope: KeyStoreScope,
): AccountStore {
  if (store !== undefined) return store;
  if (!isBrowserHostRuntime()) {
    throw new Error("E2EE requires an explicit e2ee.store AccountStore outside the browser");
  }
  return createBrowserKeyStore(scope);
}

/** @internal Separate from account selection and the resettable Jazz row cache. */
export function createBrowserKeyStore(scope: KeyStoreScope): AccountStore {
  if (typeof indexedDB === "undefined") {
    throw new Error("The default E2EE key store requires IndexedDB; supply an atomic e2ee.store");
  }
  const key = JSON.stringify([scope.registry, scope.env, scope.accountId]);
  const factory = indexedDB;

  async function transact(transform?: (current: string | null) => string): Promise<string | null> {
    const database = await openDatabase(factory);
    try {
      return await new Promise<string | null>((resolve, reject) => {
        // One IDB transaction serialises the entire operation across tabs and
        // connections. Strict durability is required for private key updates.
        const transaction = transform
          ? database.transaction(RECORDS, "readwrite", { durability: "strict" })
          : database.transaction(RECORDS, "readonly");
        const records = transaction.objectStore(RECORDS);
        const request = records.get(key);
        let value: string | null = null;
        let failure: unknown;
        request.onsuccess = () => {
          try {
            if (request.result !== undefined && typeof request.result !== "string") {
              throw new Error("Invalid persisted E2EE key state");
            }
            value = request.result ?? null;
            if (transform) {
              const replacement = transform(value);
              if (typeof replacement !== "string") {
                throw new Error("E2EE key store updates must return a string");
              }
              records.put(replacement, key);
            }
          } catch (error) {
            failure = error;
            transaction.abort();
          }
        };
        transaction.oncomplete = () => resolve(value);
        transaction.onabort = () =>
          reject(failure ?? transaction.error ?? new Error("E2EE key store transaction aborted"));
        transaction.onerror = () => {
          failure ??= transaction.error;
        };
      });
    } finally {
      database.close();
    }
  }

  return {
    read: () => transact(),
    async update(transform) {
      await transact(transform);
    },
  };
}

function openDatabase(factory: IDBFactory): Promise<IDBDatabase> {
  return new Promise((resolve, reject) => {
    const request = factory.open(DATABASE, 1);
    let blocked = false;
    request.onupgradeneeded = () => request.result.createObjectStore(RECORDS);
    request.onerror = () => reject(request.error ?? new Error("E2EE key store open failed"));
    request.onblocked = () => {
      blocked = true;
      reject(new Error("E2EE key store open blocked by another connection"));
    };
    request.onsuccess = () => {
      const database = request.result;
      database.onversionchange = () => database.close();
      // An upgrade can finish after a blocked open already rejected.
      if (blocked) database.close();
      else resolve(database);
    };
  });
}
