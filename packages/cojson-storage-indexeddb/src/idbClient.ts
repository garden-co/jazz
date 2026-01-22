import type {
  CojsonInternalTypes,
  NewCoValueRow,
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
  StoredNewCoValueRow,
  StoredSessionRow,
  TransactionRow,
} from "cojson";
import {
  CoJsonIDBTransaction,
  putIndexedDbStore,
  queryIndexedDbStore,
  StoreName,
} from "./CoJsonIDBTransaction.js";

type DeletedCoValueQueueEntry = {
  coValueID: RawCoID;
  status?: "pending" | "done";
};

/**
 * Contains operations that should run as part of a readwrite transaction.
 *
 * Everything in this class is meant to be executed sequentially and never
 * concurrently, across all the data stores and tabs.
 */
export class IDBTransaction implements DBTransactionInterfaceAsync {
  constructor(private tx: CoJsonIDBTransaction) {}

  private async run<T>(
    handler: (txEntry: CoJsonIDBTransaction) => IDBRequest<T>,
  ): Promise<T> {
    return this.tx.handleRequest<T>(handler);
  }

  async upsertCoValueRow(
    coValueRow: NewCoValueRow,
  ): Promise<StoredNewCoValueRow> {
    const rowID = await this.run((tx) =>
      tx.getObjectStore("coValues2").put(coValueRow),
    );
    return {
      ...coValueRow,
      rowID: Number(rowID),
    };
  }

  async markCoValueAsDeleted(id: RawCoID): Promise<void> {
    await this.run((tx) =>
      tx.getObjectStore("deletedCoValues").put({
        coValueID: id,
        status: "pending",
      } satisfies DeletedCoValueQueueEntry),
    );
  }

  async deleteCoValueContent(coValue: StoredNewCoValueRow): Promise<void> {
    const sessionsToDelete: string[] = [];
    for (const session of Object.values(coValue.sessions)) {
      if (!session.sessionID.endsWith("$")) {
        sessionsToDelete.push(coValue.id + session.sessionID);
        delete coValue.sessions[session.sessionID];
      }
    }

    const ops: Promise<unknown>[] = [];

    for (const ses of sessionsToDelete) {
      ops.push(this.#deleteAllTransactionsBySesPrefix(ses));
    }

    ops.push(this.upsertCoValueRow(coValue));

    ops.push(
      this.run((tx) =>
        tx.getObjectStore("deletedCoValues").put({
          coValueID: coValue.id,
          status: "done",
        } satisfies DeletedCoValueQueueEntry),
      ),
    );

    await Promise.all(ops);
  }

  async #deleteAllTransactionsBySesPrefix(ses: string) {
    const range = IDBKeyRange.bound([ses, 0], [ses, Number.POSITIVE_INFINITY]);
    const keys = await this.run((tx) =>
      tx.getObjectStore("transactions").getAllKeys(range),
    );

    for (const key of keys as IDBValidKey[]) {
      await this.run((tx) => tx.getObjectStore("transactions").delete(key));
    }
  }

  async addTransaction(
    sessionRowID: number | string,
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

  /**
   * Get an unsynced CoValue record by coValueId and peerId.
   */
  async getUnsyncedCoValueRecord(
    coValueId: RawCoID,
    peerId: string,
  ): Promise<
    { rowID: number; coValueId: RawCoID; peerId: string } | undefined
  > {
    return this.run((tx) =>
      tx
        .getObjectStore("unsyncedCoValues")
        .index("uniqueUnsyncedCoValues")
        .get([coValueId, peerId]),
    );
  }

  /**
   * Get all unsynced CoValue records for a given coValueId.
   */
  async getAllUnsyncedCoValueRecords(
    coValueId: RawCoID,
  ): Promise<{ rowID: number; coValueId: RawCoID; peerId: string }[]> {
    return this.run((tx) =>
      tx
        .getObjectStore("unsyncedCoValues")
        .index("byCoValueId")
        .getAll(coValueId),
    );
  }

  /**
   * Delete an unsynced CoValue record by rowID.
   */
  async deleteUnsyncedCoValueRecord(rowID: number): Promise<void> {
    await this.run((tx) => tx.getObjectStore("unsyncedCoValues").delete(rowID));
  }

  /**
   * Insert or update an unsynced CoValue record.
   */
  async putUnsyncedCoValueRecord(record: {
    rowID?: number;
    coValueId: RawCoID;
    peerId: string;
  }): Promise<void> {
    await this.run((tx) => tx.getObjectStore("unsyncedCoValues").put(record));
  }
}

export class IDBClient implements DBClientInterfaceAsync {
  private db;

  activeTransaction: CoJsonIDBTransaction | undefined;
  autoBatchingTransaction: CoJsonIDBTransaction | undefined;

  constructor(db: IDBDatabase) {
    this.db = db;
  }

  async getCoValueRow(
    coValueId: RawCoID,
  ): Promise<StoredNewCoValueRow | undefined> {
    return queryIndexedDbStore(this.db, "coValues2", (store) =>
      store.index("coValuesById").get(coValueId),
    );
  }

  async getNewTransactionInSession(
    sessionRowId: number | string,
    fromIdx: number,
    toIdx: number,
  ): Promise<TransactionRow[]> {
    return queryIndexedDbStore(this.db, "transactions", (store) =>
      store.getAll(
        IDBKeyRange.bound([sessionRowId, fromIdx], [sessionRowId, toIdx]),
      ),
    );
  }

  async getAllCoValuesWaitingForDelete(): Promise<RawCoID[]> {
    const entries = await queryIndexedDbStore<DeletedCoValueQueueEntry[]>(
      this.db,
      "deletedCoValues",
      (store) =>
        store.index("deletedCoValuesByStatus").getAll("pending") as IDBRequest<
          DeletedCoValueQueueEntry[]
        >,
    );
    return entries.map((e) => e.coValueID);
  }

  async transaction(
    operationsCallback: (tx: IDBTransaction) => Promise<unknown>,
    storeNames?: StoreName[],
  ) {
    const tx = new CoJsonIDBTransaction(this.db, storeNames);

    try {
      await operationsCallback(new IDBTransaction(tx));
      tx.commit(); // Tells the browser to not wait for another possible request and commit the transaction immediately
    } catch (error) {
      tx.rollback();
    }
  }

  async trackCoValuesSyncState(
    updates: { id: RawCoID; peerId: string; synced: boolean }[],
  ): Promise<void> {
    if (updates.length === 0) {
      return;
    }

    await this.transaction(
      async (tx) => {
        await Promise.all(
          updates.map(async (update) => {
            const record = await tx.getUnsyncedCoValueRecord(
              update.id,
              update.peerId,
            );
            if (update.synced) {
              // Delete
              if (record) {
                await tx.deleteUnsyncedCoValueRecord(record.rowID);
              }
            } else {
              // Insert or update
              await tx.putUnsyncedCoValueRecord(
                record
                  ? {
                      rowID: record.rowID,
                      coValueId: update.id,
                      peerId: update.peerId,
                    }
                  : {
                      coValueId: update.id,
                      peerId: update.peerId,
                    },
              );
            }
          }),
        );
      },
      ["unsyncedCoValues"],
    );
  }

  async eraseCoValueButKeepTombstone(coValueID: RawCoID): Promise<void> {
    const coValue = await this.getCoValueRow(coValueID);

    if (!coValue) {
      console.warn(`CoValue ${coValueID} not found, skipping deletion`);
      return;
    }

    await this.transaction((tx) => tx.deleteCoValueContent(coValue));
  }

  async getUnsyncedCoValueIDs(): Promise<RawCoID[]> {
    const records = await queryIndexedDbStore<
      { rowID: number; coValueId: RawCoID; peerId: string }[]
    >(this.db, "unsyncedCoValues", (store) => store.getAll());
    const uniqueIds = new Set<RawCoID>();
    for (const record of records) {
      uniqueIds.add(record.coValueId);
    }
    return Array.from(uniqueIds);
  }

  async stopTrackingSyncState(id: RawCoID): Promise<void> {
    await this.transaction(
      async (tx) => {
        const idbTx = tx as IDBTransaction;
        const records = await idbTx.getAllUnsyncedCoValueRecords(id);
        await Promise.all(
          records.map((record) =>
            idbTx.deleteUnsyncedCoValueRecord(record.rowID),
          ),
        );
      },
      ["unsyncedCoValues"],
    );
  }

  async getCoValueKnownState(
    coValueId: string,
  ): Promise<CojsonInternalTypes.CoValueKnownState | undefined> {
    const coValueRow = await this.getCoValueRow(coValueId as RawCoID);

    if (!coValueRow) {
      return undefined;
    }

    const knownState: CojsonInternalTypes.CoValueKnownState = {
      id: coValueId as RawCoID,
      header: true,
      sessions: {},
    };

    for (const session of Object.values(coValueRow.sessions)) {
      knownState.sessions[session.sessionID] = session.lastIdx;
    }

    return knownState;
  }
}
