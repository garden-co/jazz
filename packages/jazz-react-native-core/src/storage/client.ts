import type {
  CojsonInternalTypes,
  OutgoingSyncQueue,
  RawCoID,
  SessionID,
} from "cojson";
import type {
  DBClientInterfaceSync,
  SessionRow,
  SignatureAfterRow,
  StoredCoValueRow,
  StoredSessionRow,
  TransactionRow,
} from "cojson-storage";
import { SQLiteAdapter } from "./sqlite-adapter.js";

export class SQLiteClient implements DBClientInterfaceSync {
  private readonly adapter: SQLiteAdapter;
  private initialization: void | undefined = undefined;
  private isInitialized = false;

  constructor(adapter: SQLiteAdapter, _: OutgoingSyncQueue) {
    this.adapter = adapter;
  }

  private initializeInternal(): void {
    try {
      this.adapter.initialize();
      this.isInitialized = true;
    } catch (error) {
      console.error("[SQLiteClient] ❌ initialization failed:", error);
      this.initialization = undefined;
      throw error;
    }
  }

  ensureInitialized(): void {
    if (this.isInitialized) return;

    if (!this.initialization) {
      this.initialization = this.initializeInternal();
    }

    this.initialization;
  }

  getCoValue(coValueId: RawCoID): StoredCoValueRow | undefined {
    this.ensureInitialized();

    try {
      const { rows } = this.adapter.executeSync(
        "SELECT * FROM coValues WHERE id = ?",
        [coValueId],
      );

      if (!rows || rows.length === 0) return;

      const coValueRow = rows[0] as any & { rowID: number };
      const parsedHeader =
        coValueRow?.header &&
        (JSON.parse(coValueRow.header) as CojsonInternalTypes.CoValueHeader);
      return {
        ...coValueRow,
        header: parsedHeader,
      };
    } catch (e) {
      console.warn("[SQLiteClient] Error getting coValue:", coValueId, e);
      return;
    }
  }

  getCoValueSessions(coValueRowId: number): StoredSessionRow[] {
    this.ensureInitialized();
    const { rows } = this.adapter.executeSync(
      "SELECT * FROM sessions WHERE coValue = ?",
      [coValueRowId],
    );
    return rows as StoredSessionRow[];
  }

  getSingleCoValueSession(
    coValueRowId: number,
    sessionID: SessionID,
  ): StoredSessionRow | undefined {
    this.ensureInitialized();
    const { rows } = this.adapter.executeSync(
      "SELECT * FROM sessions WHERE coValue = ? AND sessionID = ?",
      [coValueRowId, sessionID],
    );
    return rows[0] as StoredSessionRow | undefined;
  }

  getNewTransactionInSession(
    sessionRowId: number,
    firstNewTxIdx: number,
  ): TransactionRow[] {
    const { rows } = this.adapter.executeSync(
      "SELECT * FROM transactions WHERE ses = ? AND idx >= ?",
      [sessionRowId, firstNewTxIdx],
    );

    if (!rows || rows.length === 0) return [];

    try {
      return rows.map((row) => {
        const rowData = row as { ses: number; idx: number; tx: string };
        return {
          ...rowData,
          tx: JSON.parse(rowData.tx) as CojsonInternalTypes.Transaction,
        };
      });
    } catch (e) {
      console.warn("Invalid JSON in transaction", e);
      return [];
    }
  }

  getSignatures(
    sessionRowId: number,
    firstNewTxIdx: number,
  ): SignatureAfterRow[] {
    const { rows } = this.adapter.executeSync(
      "SELECT * FROM signatureAfter WHERE ses = ? AND idx >= ?",
      [sessionRowId, firstNewTxIdx],
    );
    return rows as SignatureAfterRow[];
  }

  addCoValue(msg: CojsonInternalTypes.NewContentMessage): number {
    this.ensureInitialized();
    const { rows } = this.adapter.executeSync(
      "INSERT INTO coValues (id, header) VALUES (?, ?)",
      [msg.id, JSON.stringify(msg.header)],
    );

    return rows.length;
  }

  addSessionUpdate({
    sessionUpdate,
  }: {
    sessionUpdate: SessionRow;
  }): number {
    this.ensureInitialized();
    const { rows } = this.adapter.executeSync(
      `INSERT INTO sessions (coValue, sessionID, lastIdx, lastSignature, bytesSinceLastSignature)
       VALUES (?, ?, ?, ?, ?)
       ON CONFLICT(coValue, sessionID)
       DO UPDATE SET lastIdx=excluded.lastIdx,
                    lastSignature=excluded.lastSignature,
                    bytesSinceLastSignature=excluded.bytesSinceLastSignature
       RETURNING rowID`,
      [
        sessionUpdate.coValue,
        sessionUpdate.sessionID,
        sessionUpdate.lastIdx,
        sessionUpdate.lastSignature,
        sessionUpdate.bytesSinceLastSignature ?? 0,
      ],
    );
    return rows[0]?.rowID as number;
  }

  addTransaction(
    sessionRowID: number,
    nextIdx: number,
    newTransaction: CojsonInternalTypes.Transaction,
  ): number {
    this.ensureInitialized();
    const { rows } = this.adapter.executeSync(
      "INSERT INTO transactions (ses, idx, tx) VALUES (?, ?, ?)",
      [sessionRowID, nextIdx, JSON.stringify(newTransaction)],
    );
    return rows.length;
  }

  addSignatureAfter({
    sessionRowID,
    idx,
    signature,
  }: {
    sessionRowID: number;
    idx: number;
    signature: CojsonInternalTypes.Signature;
  }): number {
    this.ensureInitialized();
    const { rows } = this.adapter.executeSync(
      "INSERT INTO signatureAfter (ses, idx, signature) VALUES (?, ?, ?)",
      [sessionRowID, idx, signature],
    );
    return rows.length;
  }

  transaction(operationsCallback: () => unknown) {
    try {
      this.ensureInitialized();
      this.adapter.transactionSync(() => {
        operationsCallback();
      });
    } catch (e) {
      console.error("Transaction failed:", e);
      throw e;
    }
  }
}
