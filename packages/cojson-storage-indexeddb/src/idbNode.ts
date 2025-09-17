import { SessionRow, SignatureAfterRow, StorageApiAsync } from "cojson";
import { IDBClient } from "./idbClient.js";

let DATABASE_NAME = "jazz-storage";

export function internal_setDatabaseName(name: string) {
  DATABASE_NAME = name;
}

export async function getIndexedDBStorage(name = DATABASE_NAME) {
  const dbPromise = new Promise<IDBDatabase>((resolve, reject) => {
    const request = indexedDB.open(name, 9);
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
      if (ev.oldVersion <= 7) {
        db.createObjectStore("transactions_v2", {
          keyPath: ["ses", "idx"],
        }).createIndex("transactionsByCoValue", "coValue");
      }
    };
  });

  const db = await dbPromise;

  // const t = db.transaction(["transactions", "transactions_v2", "signatureAfter", "sessions"], "readwrite");

  // t.objectStore("transactions").openCursor().onsuccess = (ev) => {
  //   // @ts-expect-error
  //   const cursor: IDBCursorWithValue  | null = ev.target.result;

  //   if (!cursor) return;

  //   const transaction = cursor.value;

  //   const signatureAfter = promisifyRequest<SignatureAfterRow>(t.objectStore("signatureAfter").get([transaction.ses, transaction.idx]));
  //   const session = promisifyRequest<SessionRow>(t.objectStore("sessions").get(transaction.ses));

  //   Promise.all([signatureAfter, session]).then(([signatureAfter, session]) => {
  //     if (!session) return;

  //     transaction.signature = signatureAfter?.signature;

  //     if (transaction.idx === session.lastIdx - 1) {
  //       transaction.signature = session.lastSignature;
  //       console.log("transaction", transaction);
  //     }

  //     transaction.coValue = session.coValue;
  //     transaction.sessionID = session.sessionID;

  //     t.objectStore("transactions_v2").put(transaction);
  //   })

  //   cursor.continue();
  // }

  // await new Promise((resolve, reject) => {
  //   t.oncomplete = () => {
  //     resolve(undefined);
  //   };
  //   t.onerror = () => {
  //     reject(t.error);
  //   };
  // });

  return new StorageApiAsync(new IDBClient(db));
}

function promisifyRequest<T>(request: IDBRequest<T>): Promise<T | undefined> {
  return new Promise((resolve, reject) => {
    request.onsuccess = () => {
      resolve(request.result);
    };
    request.onerror = () => {
      resolve(undefined);
    };
  });
}
