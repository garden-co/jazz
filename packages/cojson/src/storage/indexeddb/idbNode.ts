import { IndexedDBStorageApi } from "../storageIDB.js";
import { IDBClient } from "./idbClient.js";

let DATABASE_NAME = "jazz-block-storage";

export function internal_setDatabaseName(name: string) {
  DATABASE_NAME = name;
}

export async function getIndexedDBStorage(name = DATABASE_NAME) {
  const dbPromise = new Promise<IDBDatabase>((resolve, reject) => {
    const request = indexedDB.open(name, 1);
    request.onerror = () => {
      reject(request.error);
    };
    request.onsuccess = () => {
      resolve(request.result);
    };
    request.onupgradeneeded = async (ev) => {
      const db = request.result;
      if (ev.oldVersion === 0) {
        const coValueBlocksStore = db.createObjectStore("coValueBlocks", {
          autoIncrement: true,
          keyPath: "_id",
        });
        coValueBlocksStore.createIndex("coValueBlocksByID", "id", {
          multiEntry: true,
        });
        db.createObjectStore("chunks", {
          keyPath: ["id", "position"],
        });
      }
    };
  });

  const db = await dbPromise;

  return new IndexedDBStorageApi(new IDBClient(db));
}
