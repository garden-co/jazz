import type { CoValueKnownState, NewContentMessage } from "../../sync.js";
import { CoJsonIDBTransaction } from "./CoJsonIDBTransaction.js";
import { IndexedDBStorage } from "./storageAsync.js";

let DATABASE_NAME = "jazz-storage-v2";

export function internal_setDatabaseName(name: string) {
  DATABASE_NAME = name;
}

export async function getIndexedDBStorage(name = DATABASE_NAME) {
  const db = await new Promise<IDBDatabase>((resolve, reject) => {
    const request = indexedDB.open(name, 4);
    request.onerror = () => {
      reject(request.error);
    };
    request.onsuccess = () => {
      resolve(request.result);
    };
    request.onupgradeneeded = async (ev) => {
      const db = request.result;
      if (ev.oldVersion === 0) {
        db.createObjectStore("content", {
          keyPath: ["id", "index"],
        });
      }
    };
  });

  return new IndexedDBStorage(new IDBDriver(db));
}

export type CoValueContent = {
  id: string;
  index: number;
  content: NewContentMessage;
  lastIndex?: number;
  knownState?: CoValueKnownState;
};

export class IDBDriver {
  db: IDBDatabase;

  constructor(db: IDBDatabase) {
    this.db = db;
  }

  activeTransaction: CoJsonIDBTransaction | undefined;
  autoBatchingTransaction: CoJsonIDBTransaction | undefined;

  makeRequest<T>(
    handler: (txEntry: CoJsonIDBTransaction) => IDBRequest<T>,
  ): Promise<T> {
    if (this.activeTransaction) {
      return this.activeTransaction.handleRequest<T>(handler);
    }

    if (this.autoBatchingTransaction?.isReusable()) {
      return this.autoBatchingTransaction.handleRequest<T>(handler);
    }

    const tx = new CoJsonIDBTransaction(this.db);

    this.autoBatchingTransaction = tx;

    return tx.handleRequest<T>(handler);
  }

  async getCoValue(
    coValueId: string,
    index: number,
  ): Promise<CoValueContent | undefined> {
    return this.makeRequest<CoValueContent | undefined>((tx) =>
      tx.getObjectStore("content").get([coValueId, index]),
    );
  }

  async storeCoValue(data: CoValueContent) {
    return this.makeRequest((tx) => tx.getObjectStore("content").put(data));
  }
}
