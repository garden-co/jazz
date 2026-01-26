import { StorageApiAsync } from "cojson";
import type {
  SignatureAfterRow,
  StoredCoValueRow,
  StoredNewCoValueRow,
  StoredNewSessionRow,
  StoredSessionRow,
} from "cojson";
import { IDBClient } from "./idbClient.js";
import { CoJsonIDBTransaction, StoreName } from "./CoJsonIDBTransaction.js";

let DATABASE_NAME = "jazz-storage";

export function internal_setDatabaseName(name: string) {
  DATABASE_NAME = name;
}

/**
 * Migrates data from the old object stores (coValues, sessions, signatureAfter)
 * into the new coValuesWithSessions store
 */
async function migrateToCoValuesWithSessions(db: IDBDatabase): Promise<void> {
  // Create a single transaction for all operations
  const oldStoreNames = [
    "coValues",
    "sessions",
    "signatureAfter",
  ] as unknown as StoreName[];
  const tx = new CoJsonIDBTransaction(db, [
    ...oldStoreNames,
    "coValuesWithSessions",
  ]);

  try {
    const [coValues = [], sessions = [], signatures = []] = await Promise.all<
      [StoredCoValueRow[], StoredSessionRow[], SignatureAfterRow[]]
    >(
      oldStoreNames.map((storeName) =>
        tx.handleRequest((tx) => {
          return tx.getObjectStore(storeName).getAll();
        }),
      ) as any,
    );

    const coValuesByRowID = new Map<number, StoredNewCoValueRow>();
    for (const coValue of coValues) {
      coValuesByRowID.set(coValue.rowID, {
        id: coValue.id,
        header: coValue.header,
        sessions: {},
        rowID: coValue.rowID,
      });
    }

    const sessionsByRowID = new Map<number, StoredNewSessionRow>();
    for (const session of sessions) {
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

    for (const signature of signatures) {
      const session = sessionsByRowID.get(signature.ses);
      if (session) {
        session.signatures[signature.idx] = signature.signature;
      }
    }

    await Promise.all(
      coValuesByRowID
        .values()
        .map((coValue) =>
          tx.handleRequest((tx) =>
            tx.getObjectStore("coValuesWithSessions").put(coValue),
          ),
        ),
    );

    tx.commit();
  } catch (error) {
    console.error("Failed to migrate Jazz storage", error);
    tx.rollback();
    throw error;
  }
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

  if (needsDataMigration) {
    await migrateToCoValuesWithSessions(db);
  }

  return new StorageApiAsync(new IDBClient(db));
}
