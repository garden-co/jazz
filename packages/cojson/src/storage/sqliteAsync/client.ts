import type {
  CoValueHeader,
  Transaction,
} from "../../coValueCore/verifiedState.js";
import type { Signature } from "../../crypto/crypto.js";
import type { RawCoID, SessionID } from "../../exports.js";
import type { CoValueKnownState } from "../../knownState.js";
import { logger } from "../../logger.js";
import type {
  DBClientInterfaceAsync,
  DBTransactionInterfaceAsync,
  SessionRow,
  SignatureAfterRow,
  StoredCoValueRow,
  StoredSessionRow,
  TransactionRow,
  StorageReconciliationAcquireResult,
} from "../types.js";
import { DeletedCoValueDeletionStatus } from "../types.js";
import type { SQLiteDatabaseDriverAsync } from "./types.js";
import type { PeerID } from "../../sync.js";
import { STORAGE_RECONCILIATION_CONFIG } from "../../config.js";

export type RawCoValueRow = {
  id: RawCoID;
  header: string;
};

export type RawTransactionRow = {
  ses: number;
  idx: number;
  tx: string;
};

type DeletedCoValueQueueRow = {
  id: RawCoID;
};

export function getErrorMessage(error: unknown) {
  return error instanceof Error ? error.message : "Unknown error";
}

/**
 * Executes storage operations inside a single DB transaction.
 */
export class SQLiteTransactionAsync implements DBTransactionInterfaceAsync {
  constructor(private readonly tx: SQLiteDatabaseDriverAsync) {}

  async getSingleCoValueSession(
    coValueRowId: number,
    sessionID: SessionID,
  ): Promise<StoredSessionRow | undefined> {
    return this.tx.get<StoredSessionRow>(
      "SELECT * FROM sessions WHERE coValue = ? AND sessionID = ?",
      [coValueRowId, sessionID],
    );
  }

  async markCoValueAsDeleted(id: RawCoID): Promise<void> {
    await this.tx.run(
      `INSERT INTO deletedCoValues (coValueID) VALUES (?) ON CONFLICT(coValueID) DO NOTHING`,
      [id],
    );
  }

  async addSessionUpdate({
    sessionUpdate,
  }: {
    sessionUpdate: SessionRow;
    sessionRow?: StoredSessionRow;
  }): Promise<number> {
    const result = await this.tx.get<{ rowID: number }>(
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

  async addTransaction(
    sessionRowID: number,
    nextIdx: number,
    newTransaction: Transaction,
  ): Promise<void> {
    await this.tx.run(
      "INSERT INTO transactions (ses, idx, tx) VALUES (?, ?, ?)",
      [sessionRowID, nextIdx, JSON.stringify(newTransaction)],
    );
  }

  async addSignatureAfter({
    sessionRowID,
    idx,
    signature,
  }: {
    sessionRowID: number;
    idx: number;
    signature: Signature;
  }): Promise<void> {
    await this.tx.run(
      "INSERT INTO signatureAfter (ses, idx, signature) VALUES (?, ?, ?)",
      [sessionRowID, idx, signature],
    );
  }

  async deleteCoValueContent(
    coValueRow: Pick<StoredCoValueRow, "rowID" | "id">,
  ): Promise<void> {
    await this.tx.run(
      `DELETE FROM transactions
       WHERE ses IN (
         SELECT rowID FROM sessions
         WHERE coValue = ?
           AND sessionID NOT LIKE '%$'
       )`,
      [coValueRow.rowID],
    );

    await this.tx.run(
      `DELETE FROM signatureAfter
       WHERE ses IN (
         SELECT rowID FROM sessions
         WHERE coValue = ?
           AND sessionID NOT LIKE '%$'
       )`,
      [coValueRow.rowID],
    );

    await this.tx.run(
      `DELETE FROM sessions
       WHERE coValue = ?
         AND sessionID NOT LIKE '%$'`,
      [coValueRow.rowID],
    );

    await this.tx.run(
      `INSERT INTO deletedCoValues (coValueID, status) VALUES (?, ?)
       ON CONFLICT(coValueID) DO UPDATE SET status=?`,
      [
        coValueRow.id,
        DeletedCoValueDeletionStatus.Done,
        DeletedCoValueDeletionStatus.Done,
      ],
    );
  }
}

export class SQLiteClientAsync implements DBClientInterfaceAsync {
  private readonly db: SQLiteDatabaseDriverAsync;
  /** Serialize transactions to avoid SQLITE_BUSY errors */
  private txQueue = Promise.resolve() as Promise<unknown>;

  constructor(db: SQLiteDatabaseDriverAsync) {
    this.db = db;
  }

  private enqueueTx<T>(fn: () => Promise<T>): Promise<T> {
    const next = this.txQueue.then(fn, fn);
    this.txQueue = next;
    return next;
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

  async eraseCoValueButKeepTombstone(coValueId: RawCoID) {
    const coValueRow = await this.db.get<RawCoValueRow & { rowID: number }>(
      "SELECT * FROM coValues WHERE id = ?",
      [coValueId],
    );

    if (!coValueRow) {
      logger.warn(`CoValue ${coValueId} not found, skipping deletion`);
      return;
    }

    await this.transaction(async (tx) => {
      await tx.deleteCoValueContent(coValueRow);
    });
  }

  async getAllCoValuesWaitingForDelete(): Promise<RawCoID[]> {
    const rows = await this.db.query<DeletedCoValueQueueRow>(
      `SELECT coValueID as id
         FROM deletedCoValues
         WHERE status = ?`,
      [DeletedCoValueDeletionStatus.Pending],
    );
    return rows.map((r) => r.id);
  }

  async transaction(
    operationsCallback: (tx: DBTransactionInterfaceAsync) => Promise<unknown>,
  ): Promise<unknown> {
    return this.enqueueTx(() =>
      this.db.transaction((tx) =>
        operationsCallback(new SQLiteTransactionAsync(tx)),
      ),
    );
  }

  async getUnsyncedCoValueIDs(): Promise<RawCoID[]> {
    const rows = await this.db.query<{ co_value_id: RawCoID }>(
      "SELECT DISTINCT co_value_id FROM unsynced_covalues",
      [],
    );
    return rows.map((row) => row.co_value_id);
  }

  async trackCoValuesSyncState(
    updates: { id: RawCoID; peerId: PeerID; synced: boolean }[],
  ): Promise<void> {
    await Promise.all(
      updates.map(async (update) => {
        if (update.synced) {
          await this.db.run(
            "DELETE FROM unsynced_covalues WHERE co_value_id = ? AND peer_id = ?",
            [update.id, update.peerId],
          );
        } else {
          await this.db.run(
            "INSERT OR REPLACE INTO unsynced_covalues (co_value_id, peer_id) VALUES (?, ?)",
            [update.id, update.peerId],
          );
        }
      }),
    );
  }

  async stopTrackingSyncState(id: RawCoID): Promise<void> {
    await this.db.run("DELETE FROM unsynced_covalues WHERE co_value_id = ?", [
      id,
    ]);
  }

  async getCoValueIDs(
    limit: number,
    offset: number,
  ): Promise<{ id: RawCoID }[]> {
    return this.db.query<{ id: RawCoID }>(
      "SELECT id FROM coValues ORDER BY rowID LIMIT ? OFFSET ?",
      [limit, offset],
    );
  }

  async tryAcquireStorageReconciliationLock(
    sessionId: SessionID,
    peerId: PeerID,
  ): Promise<StorageReconciliationAcquireResult> {
    let result: StorageReconciliationAcquireResult = {
      acquired: false,
      reason: "not_due",
    };
    await this.transaction(async () => {
      const now = Date.now();
      const lockKey = `lock#${peerId}`;

      const lockRow = await this.db.get<{
        expires_at: number;
        released_at: number;
      }>(
        "SELECT expires_at, released_at FROM storage_reconciliation_locks WHERE key = ?",
        [lockKey],
      );
      if (
        lockRow?.released_at &&
        now - lockRow.released_at <
          STORAGE_RECONCILIATION_CONFIG.RECONCILIATION_INTERVAL_MS
      ) {
        result = { acquired: false, reason: "not_due" };
        return;
      }
      if (lockRow && !lockRow.released_at && lockRow.expires_at >= now) {
        result = { acquired: false, reason: "lock_held" };
        return;
      }

      const expiresAt = now + STORAGE_RECONCILIATION_CONFIG.LOCK_TTL_MS;
      await this.db.run(
        `INSERT OR REPLACE INTO storage_reconciliation_locks (key, holder_session_id, acquired_at, expires_at, released_at) VALUES (?, ?, ?, ?, NULL)`,
        [lockKey, sessionId, now, expiresAt],
      );
      result = { acquired: true };
    });
    return result;
  }

  async releaseStorageReconciliationLock(
    sessionId: SessionID,
    peerId: PeerID,
  ): Promise<void> {
    await this.transaction(async () => {
      const lockKey = `lock#${peerId}`;
      const releasedAt = Date.now();
      const lockRow = await this.db.get<{ holder_session_id: string }>(
        "SELECT holder_session_id FROM storage_reconciliation_locks WHERE key = ?",
        [lockKey],
      );
      if (lockRow && lockRow.holder_session_id === sessionId) {
        await this.db.run(
          "UPDATE storage_reconciliation_locks SET released_at = ? WHERE key = ?",
          [releasedAt, lockKey],
        );
      }
    });
  }

  async getCoValueKnownState(
    coValueId: string,
  ): Promise<CoValueKnownState | undefined> {
    // First check if the CoValue exists
    const coValueRow = await this.db.get<{ rowID: number }>(
      "SELECT rowID FROM coValues WHERE id = ?",
      [coValueId],
    );

    if (!coValueRow) {
      return undefined;
    }

    // Get all session counters without loading transactions
    const sessions = await this.db.query<{
      sessionID: SessionID;
      lastIdx: number;
    }>("SELECT sessionID, lastIdx FROM sessions WHERE coValue = ?", [
      coValueRow.rowID,
    ]);

    const knownState: CoValueKnownState = {
      id: coValueId as RawCoID,
      header: true,
      sessions: {},
    };

    for (const session of sessions) {
      knownState.sessions[session.sessionID] = session.lastIdx;
    }

    return knownState;
  }
}
