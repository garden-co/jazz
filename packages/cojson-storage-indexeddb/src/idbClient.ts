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
  coValuesTransactions = new Map<number, TransactionRow[]>();

  constructor(db: IDBDatabase) {
    this.db = db;

    queryIndexedDbStore(this.db, "coValues", (store) => store.getAll()).then(
      (rows) => {
        for (const row of rows) {
          this.coValues.set(row.id, row);
        }
      },
    );
    // queryIndexedDbStore(this.db, "transactions_v2", (store) =>
    //   store.index("transactionsByCoValue").getAll(),
    // ).then((rows) => {
    //   if (rows.length === 0) {
    //     return;
    //   }

    //   let currentCoValue = rows[0].coValue;
    //   let coValueTransactions: any[] = [];

    //   for (const row of rows) {
    //     if (row.coValue !== currentCoValue) {
    //       this.coValuesTransactions.set(currentCoValue, coValueTransactions);
    //       currentCoValue = row.coValue;
    //       coValueTransactions = this.coValuesTransactions.get(currentCoValue) ?? [];
    //     }

    //     coValueTransactions.push(row);
    //   }
    // });
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
    ).then((row) => {
      if (row) {
        this.coValues.set(coValueId, row);
        return row;
      }
    });
  }

  async getCoValueRowID(coValueId: RawCoID): Promise<number | undefined> {
    return this.getCoValue(coValueId).then((row) => row?.rowID);
  }

  async getCoValueSessions(coValueRowId: number): Promise<StoredSessionRow[]> {
    return queryIndexedDbStore(this.db, "sessions", (store) =>
      store.index("sessionsByCoValue").getAll(coValueRowId),
    );
  }

  async getCoValueTransactions(
    coValueRowId: number,
  ): Promise<TransactionRow[]> {
    const coValueTransactions = this.coValuesTransactions.get(coValueRowId);
    if (coValueTransactions) {
      return coValueTransactions;
    }

    return queryIndexedDbStore(this.db, "transactions_v2", (store) =>
      store.index("transactionsByCoValue").getAll(coValueRowId),
    ).then((rows) => {
      this.coValuesTransactions.set(coValueRowId, rows);
      return rows;
    });
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
    return queryIndexedDbStore(this.db, "transactions_v2", (store) =>
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

  async addSessionUpdate({
    sessionUpdate,
    sessionRow,
  }: {
    sessionUpdate: SessionRow;
    sessionRow?: StoredSessionRow;
  }): Promise<number> {
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
    sessionID: SessionID,
    coValue: number,
    signature?: CojsonInternalTypes.Signature,
  ) {
    this.coValuesTransactions.delete(coValue);
    await this.makeRequest((tx) =>
      tx.getObjectStore("transactions_v2").add({
        ses: sessionRowID,
        idx,
        tx: newTransaction,
        signature,
        sessionID,
        coValue,
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
