export type StoreName =
  | "coValues"
  | "sessions"
  | "transactions"
  | "signatureAfter";

// A access unit for the IndexedDB Jazz database
// It's a wrapper around the IDBTransaction object that helps on batching multiple operations
// in a single transaction.
export class CoJsonIDBTransaction {
  db: IDBDatabase;
  tx: IDBTransaction;

  pendingRequests: ((txEntry: this) => void)[] = [];
  rejectHandlers: (() => void)[] = [];

  id = Math.random();

  running = false;
  failed = false;
  done = false;

  constructor(db: IDBDatabase) {
    this.db = db;

    this.tx = this.db.transaction(
      ["coValues", "sessions", "transactions", "signatureAfter"],
      "readwrite",
    );

    this.tx.oncomplete = () => {
      this.done = true;
    };
    this.tx.onabort = () => {
      this.done = true;
    };
  }

  startedAt = performance.now();
  isReusable() {
    const delta = performance.now() - this.startedAt;
    return !this.done && !this.failed && delta <= 100;
  }

  getObjectStore(name: StoreName) {
    return this.tx.objectStore(name);
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
