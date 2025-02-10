import { CojsonInternalTypes, type OutgoingSyncQueue, RawCoID } from "cojson";
import type {
  DBClientInterface,
  SessionRow,
  SignatureAfterRow,
  StoredCoValueRow,
  StoredSessionRow,
  TransactionRow,
} from "cojson-storage";
import { Transaction } from "cojson/src/coValueCore.js";
import { Signature } from "cojson/src/crypto/crypto.js";
import { SQLiteAdapter } from "./sqliteAdapter.js";

export class SQLiteClient implements DBClientInterface {
  private readonly adapter: SQLiteAdapter;
  private initializationPromise: Promise<void> | null = null;
  private isInitialized = false;

  constructor(adapter: SQLiteAdapter, _: OutgoingSyncQueue) {
    this.adapter = adapter;
  }

  private async initializeInternal(): Promise<void> {
    try {
      await this.adapter.initialize();
      this.isInitialized = true;
    } catch (error) {
      console.error("[SQLiteClient] initialization failed:", error);
      this.initializationPromise = null;
      throw error;
    }
  }

  async ensureInitialized(): Promise<void> {
    if (this.isInitialized) {
      return;
    }

    if (!this.initializationPromise) {
      this.initializationPromise = this.initializeInternal();
    }

    await this.initializationPromise;
  }

  async getCoValue(coValueId: RawCoID): Promise<StoredCoValueRow | undefined> {
    await this.ensureInitialized();

    try {
      const { rows } = await this.adapter.execute(
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

  async getCoValueSessions(coValueRowId: number): Promise<StoredSessionRow[]> {
    await this.ensureInitialized();
    const { rows } = await this.adapter.execute(
      "SELECT * FROM sessions WHERE coValue = ?",
      [coValueRowId],
    );
    return rows as StoredSessionRow[];
  }

  async getNewTransactionInSession(
    sessionRowId: number,
    firstNewTxIdx: number,
  ): Promise<TransactionRow[]> {
    await this.ensureInitialized();
    try {
      const { rows } = await this.adapter.execute(
        "SELECT * FROM transactions WHERE ses = ? AND idx >= ?",
        [sessionRowId, firstNewTxIdx],
      );

      if (!rows || rows.length === 0) return [];

      return rows.map((row: any) => ({
        ...row,
        tx: JSON.parse(row.tx) as Transaction,
      }));
    } catch (e) {
      console.warn("[SQLiteClient] Error in getNewTransactionInSession:", e);
      return [];
    }
  }

  async getSignatures(
    sessionRowId: number,
    firstNewTxIdx: number,
  ): Promise<SignatureAfterRow[]> {
    await this.ensureInitialized();
    if (!this.adapter.executeSync) {
      // If the adapter doesn't support sync execution, fall back to async
      const { rows } = await this.adapter.execute(
        "SELECT * FROM signatureAfter WHERE ses = ? AND idx >= ?",
        [sessionRowId, firstNewTxIdx],
      );
      return rows as SignatureAfterRow[];
    }

    const { rows } = this.adapter.executeSync(
      "SELECT * FROM signatureAfter WHERE ses = ? AND idx >= ?",
      [sessionRowId, firstNewTxIdx],
    );
    return rows as SignatureAfterRow[];
  }

  async addCoValue(
    msg: CojsonInternalTypes.NewContentMessage,
  ): Promise<number> {
    await this.ensureInitialized();
    const { insertId } = await this.adapter.execute(
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
    const { rows } = await this.adapter.execute(
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
        sessionUpdate.bytesSinceLastSignature!,
      ],
    );
    return rows[0]?.rowID as number;
  }

  async addTransaction(
    sessionRowID: number,
    nextIdx: number,
    newTransaction: Transaction,
  ): Promise<number> {
    await this.ensureInitialized();
    const { rowsAffected } = await this.adapter.execute(
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
    signature: Signature;
  }): Promise<number> {
    await this.ensureInitialized();
    const { rowsAffected } = await this.adapter.execute(
      "INSERT INTO signatureAfter (ses, idx, signature) VALUES (?, ?, ?)",
      [sessionRowID, idx, signature],
    );
    return rowsAffected;
  }

  async unitOfWork(
    operationsCallback: () => Promise<unknown>[],
  ): Promise<void> {
    await this.ensureInitialized();
    try {
      await this.adapter.transaction(async () => {
        await Promise.all(operationsCallback());
      });
    } catch (e) {
      console.error("Transaction failed:", e);
      throw e;
    }
  }
}
