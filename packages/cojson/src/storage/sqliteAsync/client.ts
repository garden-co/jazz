import type {
  CoValueHeader,
  Transaction,
} from "../../coValueCore/verifiedState.js";
import type { Signature } from "../../crypto/crypto.js";
import type { RawCoID, SessionID } from "../../exports.js";
import type { CoValueKnownState } from "../../knownState.js";
import { logger } from "../../logger.js";
import type {
  CoValueUpdate,
  DBClientInterfaceAsync,
  DBTransactionInterfaceAsync,
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
    const storedCoValueRow = await this.getCoValue(coValueId);
    if (!storedCoValueRow) {
      return undefined;
    }
    const { rowID, header } = storedCoValueRow;
    const allCoValueSessions = (await this.getCoValueSessions(
      rowID,
    )) as StoredNewSessionRow[];
    const sessions = Object.fromEntries(
      allCoValueSessions.map((sessionRow) => [
        sessionRow.sessionID,
        sessionRow,
      ]),
    );
    await Promise.all(
      allCoValueSessions.map(async (sessionRow) => {
        const signatures = await this.getSignatures(sessionRow.rowID, 0);
        sessionRow.signatures = {};
        for (const signature of signatures) {
          sessionRow.signatures[signature.idx] = signature.signature;
        }
      }),
    );
    return { id: coValueId, rowID, header, sessions };
  }

  async upsertCoValueRow(
    coValueRow: CoValueUpdate,
  ): Promise<StoredNewCoValueRow> {
    const id = coValueRow.updatedCoValueRow.id;
    const coValueRowID = await this.upsertCoValue(
      id,
      coValueRow.updatedCoValueRow.header,
    );
    if (!coValueRowID) {
      throw new Error("BOOM: Failed to upsert coValue row");
    }
    for (const session of Object.values(
      coValueRow.updatedCoValueRow.sessions,
    )) {
      if (session.coValue === Infinity) {
        session.coValue = coValueRowID;
      }
    }
    for (const session of Object.values(
      coValueRow.updatedCoValueRow.sessions,
    )) {
      const sessionRowID = await this.addSessionUpdate({
        sessionUpdate: session,
      });
      // @ts-expect-error - convert the session into a StoredNewSessionRow
      session.rowID = sessionRowID;

      for (const [idx, signature] of Object.entries(session.signatures)) {
        await this.addSignatureAfter({
          sessionRowID,
          idx: Number(idx),
          signature,
        });
      }
    }
    // @ts-expect-error - convert the sessions into a StoredNewSessionRow
    return {
      ...coValueRow.updatedCoValueRow,
      rowID: coValueRowID,
    };
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
