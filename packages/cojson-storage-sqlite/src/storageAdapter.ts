import { Database as DatabaseT } from "better-sqlite3";
import {
  MAX_RECOMMENDED_TX_SIZE,
  RawCoID,
  SessionID,
  StorageAdapter,
} from "cojson";
import { StoredSessionLog } from "cojson";
import { CojsonInternalTypes } from "cojson";
import { CoValueHeader } from "cojson/src/coValueCore.js";
import { SQLiteClient } from "./sqliteClient.js";
import { openDatabase } from "./sqliteNode.js";

export class SQLiteStorageAdapter implements StorageAdapter {
  private dbClient: SQLiteClient;

  private constructor(db: DatabaseT) {
    this.dbClient = new SQLiteClient(db);
  }

  static load(filename: string) {
    const db = openDatabase(filename);

    return new SQLiteStorageAdapter(db);
  }

  async get(id: RawCoID): Promise<{
    header: CoValueHeader;
    sessions: Map<SessionID, StoredSessionLog>;
  } | null> {
    const coValueRow = this.dbClient.getCoValue(id);

    if (!coValueRow) {
      return null;
    }

    const { header } = coValueRow;

    const sessions = new Map<SessionID, StoredSessionLog>();

    const allCoValueSessions = this.dbClient.getCoValueSessions(
      coValueRow.rowID,
    );

    for (const sessionRow of allCoValueSessions) {
      const transactions = this.dbClient.getNewTransactionInSession(
        sessionRow.rowID,
        0,
      );

      const signatures = this.dbClient.getSignatures(sessionRow.rowID, 0);

      const signatureAfter: StoredSessionLog["signatureAfter"] = {};

      for (const signature of signatures) {
        signatureAfter[signature.idx] = signature.signature;
      }

      sessions.set(sessionRow.sessionID, {
        transactions: transactions.map((transaction) => transaction.tx),
        signatureAfter,
        lastSignature: sessionRow.lastSignature,
      });
    }

    return {
      header,
      sessions,
    };
  }

  async writeHeader(id: RawCoID, header: CoValueHeader) {
    const coValueRow = this.dbClient.getCoValue(id);

    if (coValueRow) {
      return;
    }

    this.dbClient.addCoValue({
      id,
      header,
    });
  }

  async appendToSession(
    id: RawCoID,
    sessionID: SessionID,
    afterIdx: number,
    tx: CojsonInternalTypes.Transaction[],
    lastSignature: CojsonInternalTypes.Signature,
  ) {
    const coValueRow = this.dbClient.getCoValue(id);

    if (!coValueRow) {
      throw new Error(`CoValue ${id} not found`);
    }

    const allOurSessionsEntries = this.dbClient.getCoValueSessions(
      coValueRow.rowID,
    );

    const sessionRow = allOurSessionsEntries.find(
      (row) => row.sessionID === sessionID,
    );

    const actuallyNewOffset = (sessionRow?.lastIdx || 0) - afterIdx;

    const actuallyNewTransactions = tx.slice(actuallyNewOffset);

    let newBytesSinceLastSignature =
      (sessionRow?.bytesSinceLastSignature || 0) +
      actuallyNewTransactions.reduce(
        (sum, tx) =>
          sum +
          (tx.privacy === "private"
            ? tx.encryptedChanges.length
            : tx.changes.length),
        0,
      );

    const newLastIdx =
      (sessionRow?.lastIdx || 0) + actuallyNewTransactions.length;

    let shouldWriteSignature = false;

    if (newBytesSinceLastSignature > MAX_RECOMMENDED_TX_SIZE) {
      shouldWriteSignature = true;
      newBytesSinceLastSignature = 0;
    }

    const newRowID = this.dbClient.addSessionUpdate({
      sessionUpdate: {
        coValue: coValueRow.rowID,
        sessionID,
        lastIdx: newLastIdx,
        lastSignature,
        bytesSinceLastSignature: newBytesSinceLastSignature,
      },
    });

    if (shouldWriteSignature || !sessionRow) {
      this.dbClient.addSignatureAfter({
        sessionRowID: newRowID,
        idx: newLastIdx,
        signature: lastSignature,
      });
    }

    let nextIdx = sessionRow?.lastIdx || 0;

    for (const transaction of actuallyNewTransactions) {
      this.dbClient.addTransaction(newRowID, nextIdx, transaction);
      nextIdx++;
    }
  }
}
