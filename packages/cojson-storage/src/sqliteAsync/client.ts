import { type CojsonInternalTypes, type SessionID, logger } from "cojson";
import type {
  DBClientInterfaceAsync,
  SessionRow,
  SignatureAfterRow,
  StoredCoValueRow,
  StoredSessionRow,
  TransactionRow,
} from "../types.js";
import type { SQLiteDatabaseDriverAsync } from "./types.js";

type RawCoID = CojsonInternalTypes.RawCoID;
type Signature = CojsonInternalTypes.Signature;
type Transaction = CojsonInternalTypes.Transaction;

export type RawCoValueRow = {
  id: CojsonInternalTypes.RawCoID;
  header: string;
};

export type RawTransactionRow = {
  ses: number;
  idx: number;
  tx: string;
};

export function getErrorMessage(error: unknown) {
  return error instanceof Error ? error.message : "Unknown error";
}

export class SQLiteClientAsync implements DBClientInterfaceAsync {
  private readonly db: SQLiteDatabaseDriverAsync;

  constructor(db: SQLiteDatabaseDriverAsync) {
    this.db = db;
  }

  async getCoValue(coValueId: RawCoID): Promise<StoredCoValueRow | undefined> {
    const coValueRow = await this.db.get<RawCoValueRow & { rowID: number }>(
      "SELECT * FROM coValues WHERE id = ?",
      [coValueId],
    );

    if (!coValueRow) return;

    try {
      const parsedHeader = (coValueRow?.header &&
        JSON.parse(coValueRow.header)) as CojsonInternalTypes.CoValueHeader;

      return {
        ...coValueRow,
        header: parsedHeader,
      };
    } catch (e) {
      const headerValue = coValueRow?.header ?? "";
      logger.warn(`Invalid JSON in header: ${headerValue}`, {
        id: coValueId,
        err: e,
      });
      return;
    }
  }

  async getCoValueSessions(coValueRowId: number): Promise<StoredSessionRow[]> {
    return this.db.query<StoredSessionRow>(
      "SELECT * FROM sessions WHERE coValue = ?",
      [coValueRowId],
    );
  }

  async getSingleCoValueSession(
    coValueRowId: number,
    sessionID: SessionID,
  ): Promise<StoredSessionRow | undefined> {
    return this.db.get<StoredSessionRow>(
      "SELECT * FROM sessions WHERE coValue = ? AND sessionID = ?",
      [coValueRowId, sessionID],
    );
  }

  async getNewTransactionInSession(
    sessionRowId: number,
    fromIdx: number,
    toIdx: number,
  ): Promise<TransactionRow[]> {
    const txs = await this.db.query<RawTransactionRow>(
      "SELECT * FROM transactions WHERE ses = ? AND idx >= ? AND idx <= ?",
      [sessionRowId, fromIdx, toIdx],
    );

    try {
      return txs.map((transactionRow) => ({
        ...transactionRow,
        tx: JSON.parse(transactionRow.tx) as Transaction,
      }));
    } catch (e) {
      logger.warn("Invalid JSON in transaction", { err: e });
      return [];
    }
  }

  async getSignatures(
    sessionRowId: number,
    firstNewTxIdx: number,
  ): Promise<SignatureAfterRow[]> {
    return this.db.query<SignatureAfterRow>(
      "SELECT * FROM signatureAfter WHERE ses = ? AND idx >= ?",
      [sessionRowId, firstNewTxIdx],
    );
  }

  async addCoValue(
    msg: CojsonInternalTypes.NewContentMessage,
  ): Promise<number> {
    const result = await this.db.get<{ rowID: number }>(
      "INSERT INTO coValues (id, header) VALUES (?, ?) RETURNING rowID",
      [msg.id, JSON.stringify(msg.header)],
    );

    if (!result) {
      throw new Error("Failed to add coValue");
    }

    return result.rowID;
  }

  async addSessionUpdate({
    sessionUpdate,
  }: {
    sessionUpdate: SessionRow;
  }): Promise<number> {
    const result = await this.db.get<{ rowID: number }>(
      `INSERT INTO sessions (coValue, sessionID, lastIdx, lastSignature, bytesSinceLastSignature) VALUES (?, ?, ?, ?, ?)
                            ON CONFLICT(coValue, sessionID) DO UPDATE SET lastIdx=excluded.lastIdx, lastSignature=excluded.lastSignature, bytesSinceLastSignature=excluded.bytesSinceLastSignature
                            RETURNING rowID`,
      [
        sessionUpdate.coValue,
        sessionUpdate.sessionID,
        sessionUpdate.lastIdx,
        sessionUpdate.lastSignature,
        sessionUpdate.bytesSinceLastSignature,
      ],
    );

    if (!result) {
      throw new Error("Failed to add session update");
    }

    return result.rowID;
  }

  addTransaction(
    sessionRowID: number,
    nextIdx: number,
    newTransaction: Transaction,
  ) {
    this.db.run("INSERT INTO transactions (ses, idx, tx) VALUES (?, ?, ?)", [
      sessionRowID,
      nextIdx,
      JSON.stringify(newTransaction),
    ]);
  }

  async addSignatureAfter({
    sessionRowID,
    idx,
    signature,
  }: { sessionRowID: number; idx: number; signature: Signature }) {
    this.db.run(
      "INSERT INTO signatureAfter (ses, idx, signature) VALUES (?, ?, ?)",
      [sessionRowID, idx, signature],
    );
  }

  async transaction(operationsCallback: () => unknown) {
    return this.db.transaction(operationsCallback);
  }
}
