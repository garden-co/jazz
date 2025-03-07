import type { DB as DatabaseT } from "@op-engineering/op-sqlite";
import type {
  CojsonInternalTypes,
  OutgoingSyncQueue,
  RawCoID,
  SessionID,
} from "cojson";
import type {
  DBClientInterface,
  SessionRow,
  SignatureAfterRow,
  StoredCoValueRow,
  StoredSessionRow,
  TransactionRow,
} from "cojson-storage";

export class SQLiteClient implements DBClientInterface {
  private readonly db: DatabaseT;

  constructor(db: DatabaseT, _: OutgoingSyncQueue) {
    this.db = db;
  }

  getCoValue(coValueId: RawCoID): StoredCoValueRow | undefined {
    const { rows } = this.db.executeSync(
      "SELECT * FROM coValues WHERE id = ?",
      [coValueId],
    );

    if (!rows || rows.length === 0) return;

    type DbCoValueRow = {
      id: string;
      header: string;
      rowID: number;
      [key: string]: unknown;
    };

    const coValueRow = rows[0] as DbCoValueRow;
    try {
      const parsedHeader =
        coValueRow?.header && coValueRow.header.trim() !== ""
          ? (JSON.parse(coValueRow.header) as CojsonInternalTypes.CoValueHeader)
          : undefined;

      if (!parsedHeader) return undefined;

      return {
        ...coValueRow,
        id: coValueId,
        header: parsedHeader,
      };
    } catch (e) {
      console.warn(coValueId, "Invalid JSON in header", e, coValueRow?.header);
      return;
    }
  }

  getCoValueSessions(coValueRowId: number): StoredSessionRow[] {
    const { rows } = this.db.executeSync(
      "SELECT * FROM sessions WHERE coValue = ?",
      [coValueRowId],
    );
    return rows as StoredSessionRow[];
  }

  getSingleCoValueSession(
    coValueRowId: number,
    sessionID: SessionID,
  ): StoredSessionRow | undefined {
    const { rows } = this.db.executeSync(
      "SELECT * FROM sessions WHERE coValue = ? AND sessionID = ?",
      [coValueRowId, sessionID],
    );
    return rows[0] as StoredSessionRow | undefined;
  }

  getNewTransactionInSession(
    sessionRowId: number,
    firstNewTxIdx: number,
  ): TransactionRow[] {
    const { rows } = this.db.executeSync(
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
  ): Promise<SignatureAfterRow[]> | SignatureAfterRow[] {
    const { rows } = this.db.executeSync(
      "SELECT * FROM signatureAfter WHERE ses = ? AND idx >= ?",
      [sessionRowId, firstNewTxIdx],
    );
    return rows as SignatureAfterRow[];
  }

  addCoValue(msg: CojsonInternalTypes.NewContentMessage): number {
    const { insertId } = this.db.executeSync(
      "INSERT INTO coValues (id, header) VALUES (?, ?)",
      [msg.id, JSON.stringify(msg.header)],
    );

    return insertId ?? 0;
  }

  addSessionUpdate({
    sessionUpdate,
  }: {
    sessionUpdate: SessionRow;
  }): number {
    const { rows } = this.db.executeSync(
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
  ) {
    this.db.executeSync(
      "INSERT INTO transactions (ses, idx, tx) VALUES (?, ?, ?)",
      [sessionRowID, nextIdx, JSON.stringify(newTransaction)],
    );
  }

  addSignatureAfter({
    sessionRowID,
    idx,
    signature,
  }: {
    sessionRowID: number;
    idx: number;
    signature: CojsonInternalTypes.Signature;
  }) {
    this.db.executeSync(
      "INSERT INTO signatureAfter (ses, idx, signature) VALUES (?, ?, ?)",
      [sessionRowID, idx, signature],
    );
  }

  // There doesn't appear to be a synchronous version of transactions
  async transaction(operationsCallback: () => unknown) {
    try {
      await this.db.transaction(async () => {
        await operationsCallback();
      });
    } catch (e) {
      console.error("Transaction failed:", e);
      throw e;
    }
  }
}
