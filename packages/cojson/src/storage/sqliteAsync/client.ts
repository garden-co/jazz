import type {
  CoValueHeader,
  Transaction,
} from "../../coValueCore/verifiedState.js";
import type { Signature } from "../../crypto/crypto.js";
import type { RawCoID, SessionID } from "../../exports.js";
import { logger } from "../../logger.js";
import type {
  DBClientInterfaceAsync,
  DBTransactionInterfaceAsync,
  SessionRow,
  SignatureAfterRow,
  StoredCoValueRow,
  StoredSessionRow,
  TransactionRow,
} from "../types.js";
import type { SQLiteDatabaseDriverAsync } from "./types.js";
import type { PeerID } from "../../sync.js";

export type RawCoValueRow = {
  id: RawCoID;
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

export class SQLiteClientAsync
  implements DBClientInterfaceAsync, DBTransactionInterfaceAsync
{
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
        JSON.parse(coValueRow.header)) as CoValueHeader;

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

  async getCoValueRowID(id: RawCoID): Promise<number | undefined> {
    const row = await this.db.get<{ rowID: number }>(
      "SELECT rowID FROM coValues WHERE id = ?",
      [id],
    );
    return row?.rowID;
  }

  async upsertCoValue(
    id: RawCoID,
    header?: CoValueHeader,
  ): Promise<number | undefined> {
    if (!header) {
      return this.getCoValueRowID(id);
    }

    const result = await this.db.get<{ rowID: number }>(
      `INSERT INTO coValues (id, header) VALUES (?, ?) 
       ON CONFLICT(id) DO NOTHING 
       RETURNING rowID`,
      [id, JSON.stringify(header)],
    );

    if (!result) {
      return this.getCoValueRowID(id);
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
  }: {
    sessionRowID: number;
    idx: number;
    signature: Signature;
  }) {
    this.db.run(
      "INSERT INTO signatureAfter (ses, idx, signature) VALUES (?, ?, ?)",
      [sessionRowID, idx, signature],
    );
  }

  async transaction(
    operationsCallback: (tx: DBTransactionInterfaceAsync) => Promise<unknown>,
  ) {
    return this.db.transaction(() => operationsCallback(this));
  }

  async trackCoValueSyncStatus(
    id: RawCoID,
    peerId: PeerID,
    synced: boolean,
  ): Promise<void> {
    if (synced) {
      // Delete the record if synced
      await this.db.run(
        "DELETE FROM unsynced_covalues WHERE co_value_id = ? AND peer_id = ?",
        [id, peerId],
      );
    } else {
      // Insert or replace the record if unsynced
      await this.db.run(
        "INSERT OR REPLACE INTO unsynced_covalues (co_value_id, peer_id) VALUES (?, ?)",
        [id, peerId],
      );
    }
  }

  async getUnsyncedCoValueIDs(): Promise<RawCoID[]> {
    const rows = await this.db.query<{ co_value_id: RawCoID }>(
      "SELECT DISTINCT co_value_id FROM unsynced_covalues",
      [],
    );
    return rows.map((row) => row.co_value_id);
  }

  async stopTrackingSyncStatus(id: RawCoID): Promise<void> {
    await this.db.run("DELETE FROM unsynced_covalues WHERE co_value_id = ?", [
      id,
    ]);
  }
}
