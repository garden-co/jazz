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
  NewCoValueRow,
  SessionRow,
  SignatureAfterRow,
  StoredCoValueRow,
  StoredNewCoValueRow,
  StoredNewSessionRow,
  StoredSessionRow,
  TransactionRow,
} from "../types.js";
import { DeletedCoValueDeletionStatus } from "../types.js";
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

type DeletedCoValueQueueRow = {
  id: RawCoID;
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

  async getCoValueRow(
    coValueId: RawCoID,
  ): Promise<StoredNewCoValueRow | undefined> {
    const rows = await this.db.query<{
      coValue_rowID: number;
      coValue_id: RawCoID;
      coValue_header: string;
      session_rowID: number | null;
      session_coValue: number | null;
      session_sessionID: SessionID | null;
      session_lastIdx: number | null;
      session_lastSignature: string | null;
      session_bytesSinceLastSignature: number | null;
      signature_ses: number | null;
      signature_idx: number | null;
      signature_signature: string | null;
    }>(
      `SELECT 
        cv.rowID as coValue_rowID,
        cv.id as coValue_id,
        cv.header as coValue_header,
        s.rowID as session_rowID,
        s.coValue as session_coValue,
        s.sessionID as session_sessionID,
        s.lastIdx as session_lastIdx,
        s.lastSignature as session_lastSignature,
        s.bytesSinceLastSignature as session_bytesSinceLastSignature,
        sa.ses as signature_ses,
        sa.idx as signature_idx,
        sa.signature as signature_signature
      FROM coValues cv
      LEFT JOIN sessions s ON s.coValue = cv.rowID
      LEFT JOIN signatureAfter sa ON sa.ses = s.rowID
      WHERE cv.id = ?`,
      [coValueId],
    );

    if (rows.length === 0) {
      return undefined;
    }

    // Parse header from first row
    const firstRow = rows[0];
    if (!firstRow) {
      return undefined;
    }

    let parsedHeader: CoValueHeader;
    try {
      parsedHeader = (firstRow.coValue_header &&
        JSON.parse(firstRow.coValue_header)) as CoValueHeader;
    } catch (e) {
      const headerValue = firstRow.coValue_header ?? "";
      logger.warn(`Invalid JSON in header: ${headerValue}`, {
        id: coValueId,
        err: e,
      });
      return undefined;
    }

    const rowID = firstRow.coValue_rowID;
    const sessions: Record<SessionID, StoredNewSessionRow> = {};

    // Process rows to build sessions and signatures
    for (const row of rows) {
      // Skip if no session (coValue exists but has no sessions)
      if (
        row.session_rowID === null ||
        row.session_sessionID === null ||
        row.session_coValue === null ||
        row.session_lastIdx === null ||
        row.session_lastSignature === null
      ) {
        continue;
      }

      const sessionID = row.session_sessionID;

      // Initialize session if not seen before
      if (!sessions[sessionID]) {
        sessions[sessionID] = {
          rowID: row.session_rowID,
          coValue: row.session_coValue,
          sessionID: sessionID,
          lastIdx: row.session_lastIdx,
          lastSignature: row.session_lastSignature as Signature,
          bytesSinceLastSignature:
            row.session_bytesSinceLastSignature ?? undefined,
          signatures: {},
        };
      }

      // Add signature if present
      const session = sessions[sessionID];
      if (
        row.signature_ses !== null &&
        row.signature_idx !== null &&
        row.signature_signature !== null &&
        session
      ) {
        session.signatures[row.signature_idx] =
          row.signature_signature as Signature;
      }
    }

    return { id: coValueId, rowID, header: parsedHeader, sessions };
  }

  async upsertCoValueRow(
    coValueRow: NewCoValueRow,
  ): Promise<StoredNewCoValueRow> {
    const id = coValueRow.id;
    const coValueRowID = await this.upsertCoValue(id, coValueRow.header);
    if (!coValueRowID) {
      throw new Error("BOOM: Failed to upsert coValue row");
    }
    for (const session of Object.values(coValueRow.sessions)) {
      if (session.coValue === Infinity) {
        session.coValue = coValueRowID;
      }
    }
    for (const session of Object.values(coValueRow.sessions)) {
      const sessionRowID = await this.addSessionUpdate({
        sessionUpdate: session,
      });
      (session as StoredNewSessionRow).rowID = sessionRowID;

      for (const [idx, signature] of Object.entries(session.signatures)) {
        await this.addSignatureAfter({
          sessionRowID,
          idx: Number(idx),
          signature,
        });
      }
    }
    return {
      ...coValueRow,
      rowID: coValueRowID,
    };
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

  async markCoValueAsDeleted(id: RawCoID) {
    // Work queue entry. Table only stores the coValueID.
    // Idempotent by design.
    await this.db.run(
      `INSERT INTO deletedCoValues (coValueID) VALUES (?) ON CONFLICT(coValueID) DO NOTHING`,
      [id],
    );
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

    await this.transaction(async () => {
      await this.db.run(
        `DELETE FROM transactions
       WHERE ses IN (
         SELECT rowID FROM sessions
         WHERE coValue = ?
           AND sessionID NOT LIKE '%$'
       )`,
        [coValueRow.rowID],
      );

      await this.db.run(
        `DELETE FROM signatureAfter
       WHERE ses IN (
         SELECT rowID FROM sessions
         WHERE coValue = ?
           AND sessionID NOT LIKE '%$'
       )`,
        [coValueRow.rowID],
      );

      await this.db.run(
        `DELETE FROM sessions
       WHERE coValue = ?
         AND sessionID NOT LIKE '%$'`,
        [coValueRow.rowID],
      );

      await this.db.run(
        `INSERT INTO deletedCoValues (coValueID, status) VALUES (?, ?)
       ON CONFLICT(coValueID) DO UPDATE SET status=?`,
        [
          coValueId,
          DeletedCoValueDeletionStatus.Done,
          DeletedCoValueDeletionStatus.Done,
        ],
      );
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
