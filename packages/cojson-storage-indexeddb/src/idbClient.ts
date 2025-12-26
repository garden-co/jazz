import type {
  CojsonInternalTypes,
  RawCoID,
  SessionID,
  DBTransactionInterfaceAsync,
} from "cojson";
import type {
  CoValueRow,
  DBClientInterfaceAsync,
  SessionRow,
  SignatureAfterRow,
  StoredCoValueRow,
  StoredSessionRow,
  TransactionRow,
} from "cojson";
import {
  CoJsonIDBTransaction,
  putIndexedDbStore,
  queryIndexedDbStore,
} from "./CoJsonIDBTransaction.js";

export class IDBTransaction implements DBTransactionInterfaceAsync {
  constructor(private tx: CoJsonIDBTransaction) {}

  run<T>(
    handler: (txEntry: CoJsonIDBTransaction) => IDBRequest<T>,
  ): Promise<T> {
    return this.tx.handleRequest<T>(handler);
  }

  async getSingleCoValueSession(
    coValueRowId: number,
    sessionID: SessionID,
  ): Promise<StoredSessionRow | undefined> {
    return this.run((tx) =>
      tx
        .getObjectStore("sessions")
        .index("uniqueSessions")
        .get([coValueRowId, sessionID]),
    );
  }

  async addSessionUpdate({
    sessionUpdate,
    sessionRow,
  }: {
    sessionUpdate: SessionRow;
    sessionRow?: StoredSessionRow;
  }): Promise<number> {
    return this.run<number>(
      (tx) =>
        tx.getObjectStore("sessions").put(
          sessionRow?.rowID
            ? {
                rowID: sessionRow.rowID,
                ...sessionUpdate,
              }
            : sessionUpdate,
        ) as IDBRequest<number>,
    );
  }

  async addTransaction(
    sessionRowID: number,
    idx: number,
    newTransaction: CojsonInternalTypes.Transaction,
  ) {
    await this.run((tx) =>
      tx.getObjectStore("transactions").add({
        ses: sessionRowID,
        idx,
        tx: newTransaction,
      } satisfies TransactionRow),
    );
  }

  async addSignatureAfter({
    sessionRowID,
    idx,
    signature,
  }: {
    sessionRowID: number;
    idx: number;
    signature: CojsonInternalTypes.Signature;
  }) {
    return this.run((tx) =>
      tx.getObjectStore("signatureAfter").put({
        ses: sessionRowID,
        idx,
        signature,
      }),
    );
  }
}

export class IDBClient implements DBClientInterfaceAsync {
  private db;

  activeTransaction: CoJsonIDBTransaction | undefined;
  autoBatchingTransaction: CoJsonIDBTransaction | undefined;

  constructor(db: IDBDatabase) {
    this.db = db;
  }

  async getCoValue(coValueId: RawCoID): Promise<StoredCoValueRow | undefined> {
    return queryIndexedDbStore(this.db, "coValues", (store) =>
      store.index("coValuesById").get(coValueId),
    );
  }

  async getCoValueRowID(coValueId: RawCoID): Promise<number | undefined> {
    return this.getCoValue(coValueId).then((row) => row?.rowID);
  }

  async getCoValueSessions(coValueRowId: number): Promise<StoredSessionRow[]> {
    return queryIndexedDbStore(this.db, "sessions", (store) =>
      store.index("sessionsByCoValue").getAll(coValueRowId),
    );
  }

  async getNewTransactionInSession(
    sessionRowId: number,
    fromIdx: number,
    toIdx: number,
  ): Promise<TransactionRow[]> {
    return queryIndexedDbStore(this.db, "transactions", (store) =>
      store.getAll(
        IDBKeyRange.bound([sessionRowId, fromIdx], [sessionRowId, toIdx]),
      ),
    );
  }

  async getSignatures(
    sessionRowId: number,
    firstNewTxIdx: number,
  ): Promise<SignatureAfterRow[]> {
    return queryIndexedDbStore(this.db, "signatureAfter", (store) =>
      store.getAll(
        IDBKeyRange.bound(
          [sessionRowId, firstNewTxIdx],
          [sessionRowId, Number.POSITIVE_INFINITY],
        ),
      ),
    );
  }

  async upsertCoValue(
    id: RawCoID,
    header?: CojsonInternalTypes.CoValueHeader,
  ): Promise<number | undefined> {
    if (!header) {
      return this.getCoValueRowID(id);
    }

    return putIndexedDbStore<CoValueRow, number>(this.db, "coValues", {
      id,
      header,
    }).catch(() => this.getCoValueRowID(id));
  }

  async transaction(
    operationsCallback: (tx: DBTransactionInterfaceAsync) => Promise<unknown>,
  ) {
    const tx = new CoJsonIDBTransaction(this.db);

    try {
      await operationsCallback(new IDBTransaction(tx));
      tx.commit(); // Tells the browser to not wait for another possible request and commit the transaction immediately
    } catch (error) {
      tx.rollback();
    }
  }

  async trackCoValueSyncStatus(
    id: RawCoID,
    peerId: string,
    synced: boolean,
  ): Promise<void> {
    if (synced) {
      // Delete the record if synced
      await queryIndexedDbStore(this.db, "unsyncedCoValues", (store) =>
        store.delete([id, peerId]),
      );
    } else {
      // Add the record if unsynced
      await putIndexedDbStore(this.db, "unsyncedCoValues", {
        coValueId: id,
        peerId: peerId,
      });
    }
  }

  async getUnsyncedCoValueIDs(): Promise<RawCoID[]> {
    return queryIndexedDbStore<RawCoID[]>(
      this.db,
      "unsyncedCoValues",
      (store) =>
        store.index("byCoValueId").getAllKeys() as IDBRequest<RawCoID[]>,
    );
  }

  async stopTrackingSyncStatus(id: RawCoID): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const tx = this.db.transaction("unsyncedCoValues", "readwrite");
      const store = tx.objectStore("unsyncedCoValues");
      const index = store.index("byCoValueId");

      // Get all records for this CoValue ID
      const getAllRequest = index.getAll(id);

      getAllRequest.onerror = () => {
        reject(getAllRequest.error);
      };

      getAllRequest.onsuccess = () => {
        const records = getAllRequest.result as {
          rowID: number;
          coValueId: RawCoID;
          peerId: string;
        }[];

        if (records.length === 0) {
          resolve();
          tx.commit();
          return;
        }

        // Delete all records in the same transaction
        let completed = 0;
        let hasError = false;

        for (const record of records) {
          const deleteRequest = store.delete(record.rowID);
          deleteRequest.onerror = () => {
            if (!hasError) {
              hasError = true;
              reject(deleteRequest.error);
            }
          };
          deleteRequest.onsuccess = () => {
            completed++;
            if (completed === records.length && !hasError) {
              resolve();
              tx.commit();
            }
          };
        }
      };
    });
  }
}
