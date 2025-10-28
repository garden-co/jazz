import { StorageApiAsync } from "cojson";
import { IDBClient } from "./idbClient.js";

let DATABASE_NAME = "jazz-storage";

export function internal_setDatabaseName(name: string) {
  DATABASE_NAME = name;
}

export async function getIndexedDBStorage(name = DATABASE_NAME) {
  const dbPromise = new Promise<IDBDatabase>((resolve, reject) => {
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
        const coValues = db.createObjectStore("coValues", {
          autoIncrement: true,
          keyPath: "rowID",
        });

        coValues.createIndex("coValuesById", "id", {
          unique: true,
        });

        const sessions = db.createObjectStore("sessions", {
          autoIncrement: true,
          keyPath: "rowID",
        });

        sessions.createIndex("sessionsByCoValue", "coValue");
        sessions.createIndex("uniqueSessions", ["coValue", "sessionID"], {
          unique: true,
        });

        db.createObjectStore("transactions", {
          keyPath: ["ses", "idx"],
        });
      }
      if (ev.oldVersion <= 1) {
        db.createObjectStore("signatureAfter", {
          keyPath: ["ses", "idx"],
        });
      }
    };
  });

  const db = await dbPromise;

  return new StorageApiAsync(new IDBClient(db));
}
