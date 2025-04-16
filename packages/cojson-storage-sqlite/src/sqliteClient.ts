import type { Database as DatabaseT } from "better-sqlite3";
import {
  type CojsonInternalTypes,
  type OutgoingSyncQueue,
  type SessionID,
  logger,
} from "cojson";
import type {
  DBClientInterface,
  SessionRow,
  SignatureAfterRow,
  StoredCoValueRow,
  StoredSessionRow,
  TransactionRow,
} from "cojson-storage";

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

export class SQLiteClient implements DBClientInterface {
  private readonly db: DatabaseT;

  constructor(db: DatabaseT) {
    this.db = db;
  }

  getCoValue(coValueId: RawCoID): StoredCoValueRow | undefined {
    const coValueRow = this.db
      .prepare("SELECT * FROM coValues WHERE id = ?")
      .get(coValueId) as RawCoValueRow & { rowID: number };

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

  getCoValueSessions(coValueRowId: number): StoredSessionRow[] {
    return this.db
      .prepare<number>("SELECT * FROM sessions WHERE coValue = ?")
      .all(coValueRowId) as StoredSessionRow[];
  }

  getSingleCoValueSession(
    coValueRowId: number,
    sessionID: SessionID,
  ): StoredSessionRow | undefined {
    return this.db
      .prepare<[number, string]>(
        "SELECT * FROM sessions WHERE coValue = ? AND sessionID = ?",
      )
      .get(coValueRowId, sessionID) as StoredSessionRow | undefined;
  }

  getNewTransactionInSession(
    sessionRowId: number,
    firstNewTxIdx: number,
  ): TransactionRow[] {
    const txs = this.db
      .prepare<[number, number]>(
        "SELECT * FROM transactions WHERE ses = ? AND idx >= ?",
      )
      .all(sessionRowId, firstNewTxIdx) as RawTransactionRow[];

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

  getSignatures(
    sessionRowId: number,
    firstNewTxIdx: number,
  ): SignatureAfterRow[] {
    return this.db
      .prepare<[number, number]>(
        "SELECT * FROM signatureAfter WHERE ses = ? AND idx >= ?",
      )
      .all(sessionRowId, firstNewTxIdx) as SignatureAfterRow[];
  }

  addCoValue(
    msg: Pick<CojsonInternalTypes.NewContentMessage, "id" | "header">,
  ): number {
    return this.db
      .prepare<[CojsonInternalTypes.RawCoID, string]>(
        "INSERT INTO coValues (id, header) VALUES (?, ?)",
      )
      .run(msg.id, JSON.stringify(msg.header)).lastInsertRowid as number;
  }

  addSessionUpdate({
    sessionUpdate,
  }: {
    sessionUpdate: SessionRow;
  }): number {
    return (
      this.db
        .prepare<[number, string, number, string, number | undefined]>(
          `INSERT INTO sessions (coValue, sessionID, lastIdx, lastSignature, bytesSinceLastSignature) VALUES (?, ?, ?, ?, ?)
                            ON CONFLICT(coValue, sessionID) DO UPDATE SET lastIdx=excluded.lastIdx, lastSignature=excluded.lastSignature, bytesSinceLastSignature=excluded.bytesSinceLastSignature
                            RETURNING rowID`,
        )
        .get(
          sessionUpdate.coValue,
          sessionUpdate.sessionID,
          sessionUpdate.lastIdx,
          sessionUpdate.lastSignature,
          sessionUpdate.bytesSinceLastSignature,
        ) as { rowID: number }
    ).rowID;
  }

  addTransaction(
    sessionRowID: number,
    nextIdx: number,
    newTransaction: Transaction,
  ) {
    this.db
      .prepare<[number, number, string]>(
        "INSERT INTO transactions (ses, idx, tx) VALUES (?, ?, ?)",
      )
      .run(sessionRowID, nextIdx, JSON.stringify(newTransaction));
  }

  addSignatureAfter({
    sessionRowID,
    idx,
    signature,
  }: { sessionRowID: number; idx: number; signature: Signature }) {
    this.db
      .prepare<[number, number, string]>(
        "INSERT INTO signatureAfter (ses, idx, signature) VALUES (?, ?, ?)",
      )
      .run(sessionRowID, idx, signature);
  }

  transaction(operationsCallback: () => unknown) {
    this.db.transaction(operationsCallback)();
    return undefined;
  }
}
