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

export function isStorageConnectionClosingError(error: unknown): boolean {
  const message = error instanceof Error ? error.message.toLowerCase() : "";

  return (
    message.includes("database connection is closing") ||
    message.includes("database connection is closed") ||
    message.includes("connection is closing") ||
    message.includes("connection is closed")
  );
}

export function isStorageTransactionFinishedError(error: unknown): boolean {
  const message = error instanceof Error ? error.message.toLowerCase() : "";

  return (
    message.includes("transaction has finished") ||
    message.includes("transaction is inactive") ||
    message.includes("transaction is already committing or done")
  );
}

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
    private onDatabaseClosing?: () => void,
  ) {
    this.refresh();
  }

  refresh() {
    try {
      this.tx = this.db.transaction(this.storeNames, "readwrite");
    } catch (error) {
      if (isStorageConnectionClosingError(error)) {
        this.failed = true;
        this.done = true;
        this.onDatabaseClosing?.();
      }
      throw error;
    }

    this.tx.oncomplete = () => {
      this.done = true;
    };
    this.tx.onabort = () => {
      this.done = true;
    };
  }

  rollback() {
    this.abortIfActive();
  }

  private abortIfActive() {
    if (this.done) {
      return;
    }

    try {
      this.tx.abort();
    } catch (error) {
      if (isStorageConnectionClosingError(error)) {
        this.failed = true;
        this.done = true;
        this.onDatabaseClosing?.();
        return;
      }

      if (!isStorageTransactionFinishedError(error)) {
        throw error;
      }
    }
  }

  getObjectStore(name: StoreName) {
    try {
      return this.tx.objectStore(name);
    } catch (error) {
      if (isStorageConnectionClosingError(error)) {
        this.failed = true;
        this.done = true;
        this.onDatabaseClosing?.();
        throw error;
      }

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
          this.abortIfActive();

          if (isStorageConnectionClosingError(request.error)) {
            this.onDatabaseClosing?.();
          } else {
            console.error(request.error);
          }

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
      try {
        this.tx.commit();
      } catch (error) {
        if (isStorageConnectionClosingError(error)) {
          this.failed = true;
          this.done = true;
          this.onDatabaseClosing?.();
          return;
        }

        if (!isStorageTransactionFinishedError(error)) {
          throw error;
        }
      }
    }
  }
}

function commitTransaction(tx: IDBTransaction) {
  try {
    tx.commit();
  } catch (error) {
    if (
      !isStorageConnectionClosingError(error) &&
      !isStorageTransactionFinishedError(error)
    ) {
      throw error;
    }
  }
}

export function queryIndexedDbStore<T>(
  db: IDBDatabase,
  storeName: StoreName,
  callback: (store: IDBObjectStore) => IDBRequest<T>,
) {
  return new Promise<T>((resolve, reject) => {
    let tx: IDBTransaction;

    try {
      tx = db.transaction(storeName, "readonly");
    } catch (error) {
      reject(error);
      return;
    }

    const request = callback(tx.objectStore(storeName));

    request.onerror = () => {
      reject(request.error);
    };

    request.onsuccess = () => {
      try {
        commitTransaction(tx);
        resolve(request.result as T);
      } catch (error) {
        reject(error);
      }
    };
  });
}

export function putIndexedDbStore<T, O extends IDBValidKey>(
  db: IDBDatabase,
  storeName: StoreName,
  value: T,
) {
  return new Promise<O>((resolve, reject) => {
    let tx: IDBTransaction;

    try {
      tx = db.transaction(storeName, "readwrite");
    } catch (error) {
      reject(error);
      return;
    }

    const request = tx.objectStore(storeName).put(value);

    request.onerror = () => {
      reject(request.error);
    };

    request.onsuccess = () => {
      try {
        commitTransaction(tx);
        resolve(request.result as O);
      } catch (error) {
        reject(error);
      }
    };
  });
}
