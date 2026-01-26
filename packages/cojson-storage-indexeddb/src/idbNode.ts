import { StorageApiAsync } from "cojson";
import type {
  SignatureAfterRow,
  StoredCoValueRow,
  StoredNewCoValueRow,
  StoredNewSessionRow,
  StoredSessionRow,
} from "cojson";
import { IDBClient } from "./idbClient.js";

let DATABASE_NAME = "jazz-storage";

export function internal_setDatabaseName(name: string) {
  DATABASE_NAME = name;
}

/**
 * Helper to wrap an IDBRequest in a Promise
 */
function requestToPromise<T>(request: IDBRequest<T>): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}

/**
 * Migrates data from the old object stores (coValues, sessions, signatureAfter)
 * into the new coValuesWithSessions store
 */
async function migrateToCoValuesWithSessions(db: IDBDatabase): Promise<void> {
  const oldStoreNames = ["coValues", "sessions", "signatureAfter"];

  // Read all data in a single read-only transaction
  const readTx = db.transaction(oldStoreNames, "readonly");
  const coValuesPromise = requestToPromise<StoredCoValueRow[]>(
    readTx.objectStore("coValues").getAll(),
  );
  const sessionsPromise = requestToPromise<StoredSessionRow[]>(
    readTx.objectStore("sessions").getAll(),
  );
  const signaturesPromise = requestToPromise<SignatureAfterRow[]>(
    readTx.objectStore("signatureAfter").getAll(),
  );

  const coValuesByRowID = new Map<number, StoredNewCoValueRow>();
  for (const coValue of await coValuesPromise) {
    coValuesByRowID.set(coValue.rowID, {
      id: coValue.id,
      header: coValue.header,
      sessions: {},
      rowID: coValue.rowID,
    });
  }

  const sessionsByRowID = new Map<number, StoredNewSessionRow>();
  for (const session of await sessionsPromise) {
    if (session.rowID && session.coValue) {
      const migratedSession: StoredNewSessionRow = {
        sessionID: session.sessionID,
        // Necessary for backward compatibility. Used to fetch existing transactions
        rowID: session.rowID,
        lastIdx: session.lastIdx,
        lastSignature: session.lastSignature,
        bytesSinceLastSignature: session.bytesSinceLastSignature,
        signatures: {},
      };
      sessionsByRowID.set(session.rowID, migratedSession);
      const coValue = coValuesByRowID.get(session.coValue);
      if (coValue) {
        coValue.sessions[session.sessionID] = migratedSession;
      }
    }
  }

  for (const signature of await signaturesPromise) {
    const session = sessionsByRowID.get(signature.ses);
    if (session) {
      session.signatures[signature.idx] = signature.signature;
    }
  }

  // Write all data in a single transaction
  const writeTx = db.transaction(["coValuesWithSessions"], "readwrite");
  const store = writeTx.objectStore("coValuesWithSessions");
  for (const coValue of coValuesByRowID.values()) {
    store.put(coValue);
  }
  await new Promise<void>((resolve, reject) => {
    writeTx.oncomplete = () => resolve();
    writeTx.onerror = () => reject(writeTx.error);
  });
}

export async function getIndexedDBStorage(name = DATABASE_NAME) {
  let needsDataMigration = false;

  const dbPromise = new Promise<IDBDatabase>((resolve, reject) => {
    const request = indexedDB.open(name, 7);
    request.onerror = () => {
      reject(request.error);
    };
    request.onsuccess = () => {
      resolve(request.result);
    };
    request.onupgradeneeded = (ev) => {
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
      if (ev.oldVersion <= 4) {
        const unsyncedCoValues = db.createObjectStore("unsyncedCoValues", {
          autoIncrement: true,
          keyPath: "rowID",
        });
        unsyncedCoValues.createIndex("byCoValueId", "coValueId");
        unsyncedCoValues.createIndex(
          "uniqueUnsyncedCoValues",
          ["coValueId", "peerId"],
          {
            unique: true,
          },
        );
      }
      if (ev.oldVersion <= 5) {
        const deletedCoValues = db.createObjectStore("deletedCoValues", {
          keyPath: "coValueID",
        });
        deletedCoValues.createIndex("deletedCoValuesByStatus", "status", {
          unique: false,
        });
      }
      if (ev.oldVersion <= 6) {
        const coValuesWithSessions = db.createObjectStore(
          "coValuesWithSessions",
          {
            autoIncrement: true,
            keyPath: "rowID",
          },
        );
        coValuesWithSessions.createIndex("coValuesById", "id", {
          unique: true,
        });
        // Mark that we need to migrate data after the upgrade completes
        // (we can't perform async work as part of the version change transaction)
        needsDataMigration = true;
        // TODO handle multiple open tabs (see `versionchange` event)
        // db.deleteObjectStore("coValues");
        // db.deleteObjectStore("sessions");
        // db.deleteObjectStore("signatureAfter");
      }
    };
  });

  const db = await dbPromise;

  if (needsDataMigration || true) {
    await migrateToCoValuesWithSessions(db);
  }

  return new StorageApiAsync(new IDBClient(db));
}
