import type { CojsonInternalTypes, RawCoID, SessionID } from "cojson";
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

export class IDBClient implements DBClientInterfaceAsync {
  private db;

  activeTransaction: CoJsonIDBTransaction | undefined;
  autoBatchingTransaction: CoJsonIDBTransaction | undefined;

  coValues = new Map<RawCoID, StoredCoValueRow>();
  sessions = new Map<number, StoredSessionRow[]>();
  signatureAfter = new Map<number, SignatureAfterRow[]>();

  constructor(db: IDBDatabase) {
    this.db = db;

    /**
     * Preloads the coValues, sessions and signatureAfter into memory
     *
     * In the browsers context, this take a few milliseconds an it makes the coValue loads
     * shaving off around the 25% of the load time
     *
     * The memory allocated is limited, because the header, sessions and signatureAfter sizes are somewhat fixed
     * because they don't contain any user data
     *
     * IndexedDB is slow at doing many small queries, even slower when accessing an index.
     *
     * This preload works around the issue, making the load reaching only the transactions store.
     */
    queryIndexedDbStore(this.db, "coValues", (store) =>
      store.index("coValuesById").getAll(),
    ).then((rows) => {
      for (const row of rows) {
        this.coValues.set(row.id, row);
      }
    });

    queryIndexedDbStore(this.db, "sessions", (store) =>
      store.index("sessionsByCoValue").getAll(),
    ).then((rows) => {
      if (rows.length === 0) {
        return;
      }

      let currentCoValue = rows[0].coValue;
      let currentSessions: StoredSessionRow[] = [];

      for (const row of rows) {
        if (row.coValue !== currentCoValue) {
          this.sessions.set(currentCoValue, currentSessions);
          currentCoValue = row.coValue;
          currentSessions = [];
        }

        currentSessions.push(row);
      }

      this.sessions.set(currentCoValue, currentSessions);
    });

    queryIndexedDbStore(this.db, "signatureAfter", (store) =>
      store.getAll(),
    ).then((rows) => {
      for (const row of rows) {
        this.signatureAfter.set(row.ses, row);
      }
    });
  }

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

  async getCoValue(coValueId: RawCoID): Promise<StoredCoValueRow | undefined> {
    const coValue = this.coValues.get(coValueId);
    if (coValue) {
      return coValue;
    }

    return queryIndexedDbStore(this.db, "coValues", (store) =>
      store.index("coValuesById").get(coValueId),
    );
  }

  async getCoValueRowID(coValueId: RawCoID): Promise<number | undefined> {
    return this.getCoValue(coValueId).then((row) => row?.rowID);
  }

  async getCoValueSessions(coValueRowId: number): Promise<StoredSessionRow[]> {
    const sessions = this.sessions.get(coValueRowId);
    if (sessions) {
      return sessions;
    }

    return queryIndexedDbStore(this.db, "sessions", (store) =>
      store.index("sessionsByCoValue").getAll(coValueRowId),
    );
  }

  async getSingleCoValueSession(
    coValueRowId: number,
    sessionID: SessionID,
  ): Promise<StoredSessionRow | undefined> {
    return queryIndexedDbStore(this.db, "sessions", (store) =>
      store.index("uniqueSessions").get([coValueRowId, sessionID]),
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

  async getSignatures(sessionRowId: number): Promise<SignatureAfterRow[]> {
    const signatures = this.signatureAfter.get(sessionRowId);
    if (signatures) {
      return signatures;
    }

    return queryIndexedDbStore(this.db, "signatureAfter", (store) =>
      store.getAll(
        IDBKeyRange.bound(
          [sessionRowId, 0],
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

    this.coValues.delete(id);

    return putIndexedDbStore<CoValueRow, number>(this.db, "coValues", {
      id,
      header,
    }).catch(() => this.getCoValueRowID(id));
  }

  async addSessionUpdate({
    sessionUpdate,
    sessionRow,
  }: {
    sessionUpdate: SessionRow;
    sessionRow?: StoredSessionRow;
  }): Promise<number> {
    this.sessions.delete(sessionUpdate.coValue);

    return this.makeRequest<number>(
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
    await this.makeRequest((tx) =>
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
    this.signatureAfter.delete(sessionRowID);

    return this.makeRequest((tx) =>
      tx.getObjectStore("signatureAfter").put({
        ses: sessionRowID,
        idx,
        signature,
      }),
    );
  }

  closeTransaction(tx: CoJsonIDBTransaction) {
    tx.commit();

    if (tx === this.activeTransaction) {
      this.activeTransaction = undefined;
    }
  }

  async transaction(operationsCallback: () => unknown) {
    const tx = new CoJsonIDBTransaction(this.db);

    this.activeTransaction = tx;

    try {
      await operationsCallback();
      tx.commit(); // Tells the browser to not wait for another possible request and commit the transaction immediately
    } finally {
      this.activeTransaction = undefined;
    }
  }
}
