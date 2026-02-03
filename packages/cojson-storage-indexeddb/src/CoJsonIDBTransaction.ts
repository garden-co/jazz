export type StoreName =
  | "coValues"
  | "sessions"
  | "transactions"
  | "signatureAfter"
  | "deletedCoValues"
  | "unsyncedCoValues"
  | "storageReconciliationLocks";

const DEFAULT_TX_STORES: StoreName[] = [
  "coValues",
  "sessions",
  "transactions",
  "signatureAfter",
  "deletedCoValues",
];

/**
 * An access unit for the IndexedDB Jazz database.
 * It's a wrapper around the IDBTransaction object that helps on batching multiple operations
 * in a single transaction.
 */
export class CoJsonIDBTransaction {
  declare tx: IDBTransaction;

  pendingRequests: ((txEntry: this) => void)[] = [];
  rejectHandlers: (() => void)[] = [];

  id = Math.random();

  running = false;
  failed = false;
  done = false;

  constructor(
    public db: IDBDatabase,
    // The object stores this transaction will operate on
    private storeNames: StoreName[] = DEFAULT_TX_STORES,
  ) {
    this.refresh();
  }

  refresh() {
    this.tx = this.db.transaction(this.storeNames, "readwrite");

    this.tx.oncomplete = () => {
      this.done = true;
    };
    this.tx.onabort = () => {
      this.done = true;
    };
  }

  rollback() {
    this.tx.abort();
  }

  getObjectStore(name: StoreName) {
    try {
      return this.tx.objectStore(name);
    } catch (error) {
      this.refresh();
      return this.tx.objectStore(name);
    }
  }

  private pushRequest<T>(
    handler: (txEntry: this, next: () => void) => Promise<T>,
  ) {
    const next = () => {
      const next = this.pendingRequests.shift();

      if (next) {
        next(this);
      } else {
        this.running = false;
        this.done = true;
      }
    };

    if (this.running) {
      return new Promise<T>((resolve, reject) => {
        this.rejectHandlers.push(reject);
        this.pendingRequests.push(async () => {
          try {
            const result = await handler(this, next);
            resolve(result);
          } catch (error) {
            reject(error);
          }
        });
      });
    }

    this.running = true;
    return handler(this, next);
  }

  handleRequest<T>(handler: (txEntry: this) => IDBRequest<T>) {
    return this.pushRequest<T>((txEntry, next) => {
      return new Promise<T>((resolve, reject) => {
        const request = handler(txEntry);

        request.onerror = () => {
          this.failed = true;
          this.tx.abort();
          console.error(request.error);
          reject(request.error);

          // Don't leave any pending promise
          for (const handler of this.rejectHandlers) {
            handler();
          }
        };

        request.onsuccess = () => {
          resolve(request.result as T);
          next();
        };
      });
    });
  }

  commit() {
    if (!this.done) {
      this.tx.commit();
    }
  }
}

export function queryIndexedDbStore<T>(
  db: IDBDatabase,
  storeName: StoreName,
  callback: (store: IDBObjectStore) => IDBRequest<T>,
) {
  return new Promise<T>((resolve, reject) => {
    const tx = db.transaction(storeName, "readonly");
    const request = callback(tx.objectStore(storeName));

    request.onerror = () => {
      reject(request.error);
    };

    request.onsuccess = () => {
      resolve(request.result as T);
      tx.commit();
    };
  });
}

export function putIndexedDbStore<T, O extends IDBValidKey>(
  db: IDBDatabase,
  storeName: StoreName,
  value: T,
) {
  return new Promise<O>((resolve, reject) => {
    const tx = db.transaction(storeName, "readwrite");
    const request = tx.objectStore(storeName).put(value);

    request.onerror = () => {
      reject(request.error);
    };

    request.onsuccess = () => {
      resolve(request.result as O);
      tx.commit();
    };
  });
}
