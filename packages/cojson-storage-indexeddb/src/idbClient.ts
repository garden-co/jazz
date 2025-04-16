import type { CojsonInternalTypes, RawCoID, SessionID } from "cojson";
import type {
  CoValueRow,
  DBClientInterface,
  SessionRow,
  SignatureAfterRow,
  StoredCoValueRow,
  StoredSessionRow,
  TransactionRow,
} from "cojson-storage";
import { CoJsonIDBTransaction } from "./CoJsonIDBTransaction.js";

export class IDBClient implements DBClientInterface {
  private db;

  activeTransaction: CoJsonIDBTransaction | undefined;
  autoBatchingTransaction: CoJsonIDBTransaction | undefined;

  constructor(db: IDBDatabase) {
    this.db = db;
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
    return this.makeRequest<StoredCoValueRow | undefined>((tx) =>
      tx.getObjectStore("coValues").index("coValuesById").get(coValueId),
    );
  }

  async getCoValueSessions(coValueRowId: number): Promise<StoredSessionRow[]> {
    return this.makeRequest<StoredSessionRow[]>((tx) =>
      tx
        .getObjectStore("sessions")
        .index("sessionsByCoValue")
        .getAll(coValueRowId),
    );
  }

  async getSingleCoValueSession(
    coValueRowId: number,
    sessionID: SessionID,
  ): Promise<StoredSessionRow | undefined> {
    return this.makeRequest<StoredSessionRow>((tx) =>
      tx
        .getObjectStore("sessions")
        .index("uniqueSessions")
        .get([coValueRowId, sessionID]),
    );
  }

  async getNewTransactionInSession(
    sessionRowId: number,
    firstNewTxIdx: number,
  ): Promise<TransactionRow[]> {
    return this.makeRequest<TransactionRow[]>((tx) =>
      tx
        .getObjectStore("transactions")
        .getAll(
          IDBKeyRange.bound(
            [sessionRowId, firstNewTxIdx],
            [sessionRowId, Number.POSITIVE_INFINITY],
          ),
        ),
    );
  }

  async getSignatures(
    sessionRowId: number,
    firstNewTxIdx: number,
  ): Promise<SignatureAfterRow[]> {
    return this.makeRequest<SignatureAfterRow[]>((tx) =>
      tx
        .getObjectStore("signatureAfter")
        .getAll(
          IDBKeyRange.bound(
            [sessionRowId, firstNewTxIdx],
            [sessionRowId, Number.POSITIVE_INFINITY],
          ),
        ),
    );
  }

  async addCoValue(
    msg: Pick<CojsonInternalTypes.NewContentMessage, "id" | "header">,
  ): Promise<number> {
    if (!msg.header) {
      throw new Error(`Header is required, coId: ${msg.id}`);
    }

    return (await this.makeRequest<IDBValidKey>((tx) =>
      tx.getObjectStore("coValues").put({
        id: msg.id,
        // biome-ignore lint/style/noNonNullAssertion: TODO(JAZZ-561): Review
        header: msg.header!,
      } satisfies CoValueRow),
    )) as number;
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
    return this.makeRequest((tx) =>
      tx.getObjectStore("signatureAfter").put({
        ses: sessionRowID,
        idx,
        signature,
      } satisfies SignatureAfterRow),
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
