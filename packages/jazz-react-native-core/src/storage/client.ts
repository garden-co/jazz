import type {
  CojsonInternalTypes,
  OutgoingSyncQueue,
  RawCoID,
  SessionID,
} from "cojson";
import type {
  DBClientInterfaceAsync,
  SessionRow,
  SignatureAfterRow,
  StoredCoValueRow,
  StoredSessionRow,
  TransactionRow,
} from "cojson-storage";
import { SQLiteAdapter } from "./sqlite-adapter.js";
import type { Mode, SQLResult, SQLRow } from "./sqlite-adapter.js";

export class SQLiteClient implements DBClientInterfaceAsync {
  private readonly adapter: SQLiteAdapter;
  private readonly mode: Mode;
  private initializationPromise: Promise<void> | null = null;
  private isInitialized = false;

  constructor(
    adapter: SQLiteAdapter,
    _: OutgoingSyncQueue,
    mode: Mode = "sync",
  ) {
    this.adapter = adapter;
    this.mode = mode;
  }

  private async initializeInternal(): Promise<void> {
    try {
      await this.adapter.initialize();
      this.isInitialized = true;
    } catch (error) {
      console.error("[SQLiteClient] ❌ initialization failed:", error);
      this.initializationPromise = null;
      throw error;
    }
  }

  async ensureInitialized(): Promise<void> {
    if (this.isInitialized) return;

    if (!this.initializationPromise) {
      this.initializationPromise = this.initializeInternal();
    }

    await this.initializationPromise;
  }

  private async exec(sql: string, params?: unknown[]): Promise<SQLResult> {
    if (this.mode === "sync") {
      const { rows } = this.adapter.executeSync(sql, params);
      return Promise.resolve({ rows, rowsAffected: 0 });
    }
    return this.adapter.executeAsync(sql, params);
  }

  private async trans(callback: () => Promise<void>): Promise<void> {
    if (this.mode === "sync") {
      this.adapter.transactionSync(() => {
        callback();
      });
      return Promise.resolve();
    }
    return this.adapter.transactionAsync(callback);
  }

  async getCoValue(coValueId: RawCoID): Promise<StoredCoValueRow | undefined> {
    await this.ensureInitialized();

    try {
      const { rows } = await this.exec("SELECT * FROM coValues WHERE id = ?", [
        coValueId,
      ]);

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

  async getCoValueSessions(coValueRowId: number): Promise<StoredSessionRow[]> {
    await this.ensureInitialized();
    const { rows } = await this.exec(
      "SELECT * FROM sessions WHERE coValue = ?",
      [coValueRowId],
    );
    return rows as StoredSessionRow[];
  }

  async getSingleCoValueSession(
    coValueRowId: number,
    sessionID: SessionID,
  ): Promise<StoredSessionRow | undefined> {
    await this.ensureInitialized();
    const { rows } = await this.exec(
      "SELECT * FROM sessions WHERE coValue = ? AND sessionID = ?",
      [coValueRowId, sessionID],
    );
    return rows[0] as StoredSessionRow | undefined;
  }

  async getNewTransactionInSession(
    sessionRowId: number,
    firstNewTxIdx: number,
  ): Promise<TransactionRow[]> {
    const { rows } = await this.exec(
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

  async getSignatures(
    sessionRowId: number,
    firstNewTxIdx: number,
  ): Promise<SignatureAfterRow[]> {
    const { rows } = await this.exec(
      "SELECT * FROM signatureAfter WHERE ses = ? AND idx >= ?",
      [sessionRowId, firstNewTxIdx],
    );
    return rows as SignatureAfterRow[];
  }

  async addCoValue(
    msg: CojsonInternalTypes.NewContentMessage,
  ): Promise<number> {
    await this.ensureInitialized();
    const { insertId } = await this.exec(
      "INSERT INTO coValues (id, header) VALUES (?, ?)",
      [msg.id, JSON.stringify(msg.header)],
    );

    return insertId ?? 0;
  }

  async addSessionUpdate({
    sessionUpdate,
  }: {
    sessionUpdate: SessionRow;
  }): Promise<number> {
    await this.ensureInitialized();
    const { rows } = await this.exec(
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

  async addTransaction(
    sessionRowID: number,
    nextIdx: number,
    newTransaction: CojsonInternalTypes.Transaction,
  ): Promise<number> {
    await this.ensureInitialized();
    const { rowsAffected } = await this.exec(
      "INSERT INTO transactions (ses, idx, tx) VALUES (?, ?, ?)",
      [sessionRowID, nextIdx, JSON.stringify(newTransaction)],
    );
    return rowsAffected;
  }

  async addSignatureAfter({
    sessionRowID,
    idx,
    signature,
  }: {
    sessionRowID: number;
    idx: number;
    signature: CojsonInternalTypes.Signature;
  }): Promise<number> {
    await this.ensureInitialized();
    const { rowsAffected } = await this.exec(
      "INSERT INTO signatureAfter (ses, idx, signature) VALUES (?, ?, ?)",
      [sessionRowID, idx, signature],
    );
    return rowsAffected;
  }

  async transaction(operationsCallback: () => unknown) {
    try {
      await this.ensureInitialized();
      await this.trans(async () => {
        await operationsCallback();
      });
    } catch (e) {
      console.error("Transaction failed:", e);
      throw e;
    }
  }
}
