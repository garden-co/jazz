import type { AccountStore } from "jazz-tools";

interface KeyStoreScope {
  registry: string;
  env: string;
  accountId: string;
}

function openKeyDatabase(): Promise<IDBDatabase> {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open("jazz-e2ee-chat-keys-v1", 1);
    request.onupgradeneeded = () => request.result.createObjectStore("accounts");
    request.onerror = () => reject(request.error);
    request.onsuccess = () => resolve(request.result);
  });
}

// #region persistent-key-store
/** Separate from account selection and Jazz's row cache. No cryptography lives here. */
export function createKeyStore(scope: KeyStoreScope): AccountStore {
  const key = JSON.stringify([scope.registry, scope.env, scope.accountId]);
  return {
    async read() {
      const database = await openKeyDatabase();
      return new Promise<string | null>((resolve, reject) => {
        const transaction = database.transaction("accounts", "readonly");
        const request = transaction.objectStore("accounts").get(key);
        let value: string | null = null;
        let failure: unknown;
        request.onsuccess = () => {
          if (request.result === undefined || typeof request.result === "string") {
            value = request.result ?? null;
          } else {
            failure = new Error("Invalid persisted E2EE key state");
            transaction.abort();
          }
        };
        transaction.oncomplete = () => {
          database.close();
          resolve(value);
        };
        transaction.onabort = () => {
          database.close();
          reject(failure ?? transaction.error);
        };
        transaction.onerror = () => {
          failure ??= transaction.error;
        };
      });
    },
    async update(transform) {
      const database = await openKeyDatabase();
      return new Promise<void>((resolve, reject) => {
        // IDB serialises this entire read/transform/write across tabs and connections.
        const transaction = database.transaction("accounts", "readwrite", { durability: "strict" });
        const records = transaction.objectStore("accounts");
        const request = records.get(key);
        let failure: unknown;
        request.onsuccess = () => {
          try {
            if (request.result !== undefined && typeof request.result !== "string")
              throw new Error("Invalid persisted E2EE key state");
            records.put(transform(request.result ?? null), key);
          } catch (error) {
            failure = error;
            transaction.abort();
          }
        };
        transaction.oncomplete = () => {
          database.close();
          resolve();
        };
        transaction.onabort = () => {
          database.close();
          reject(failure ?? transaction.error);
        };
        transaction.onerror = () => {
          failure ??= transaction.error;
        };
      });
    },
  };
}
// #endregion persistent-key-store
