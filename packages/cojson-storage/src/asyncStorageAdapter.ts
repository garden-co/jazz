import {
  type CojsonInternalTypes,
  MAX_RECOMMENDED_TX_SIZE,
  type RawCoID,
  type SessionID,
  type StorageAdapter as StorageAdapterT,
  type StoredSessionLog,
} from "cojson";
import type { DBClientInterface } from "./types.js";

export class AsyncStorageAdapter implements StorageAdapterT {
  private dbClient: DBClientInterface;

  constructor(db: DBClientInterface) {
    this.dbClient = db;
  }

  async get(id: RawCoID): Promise<{
    header: CojsonInternalTypes.CoValueHeader;
    sessions: Map<SessionID, StoredSessionLog>;
  } | null> {
    const coValueRow = await this.dbClient.getCoValue(id);

    if (!coValueRow) {
      return null;
    }

    const { header } = coValueRow;

    const sessions = new Map<SessionID, StoredSessionLog>();

    const allCoValueSessions = await this.dbClient.getCoValueSessions(
      coValueRow.rowID,
    );

    for (const sessionRow of allCoValueSessions) {
      const transactions = await this.dbClient.getNewTransactionInSession(
        sessionRow.rowID,
        0,
      );

      const signatures = await this.dbClient.getSignatures(sessionRow.rowID, 0);

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

  async writeHeader(id: RawCoID, header: CojsonInternalTypes.CoValueHeader) {
    const coValueRow = await this.dbClient.getCoValue(id);

    if (coValueRow) {
      return;
    }

    await this.dbClient.addCoValue({
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
    const coValueRow = await this.dbClient.getCoValue(id);

    if (!coValueRow) {
      throw new Error(`CoValue ${id} not found`);
    }

    const allOurSessionsEntries = await this.dbClient.getCoValueSessions(
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

    let shouldEnableSignatureStreaming = false;

    if (newBytesSinceLastSignature > MAX_RECOMMENDED_TX_SIZE) {
      shouldEnableSignatureStreaming = true;
      newBytesSinceLastSignature = 0;
    }

    const newRowID = await this.dbClient.addSessionUpdate({
      sessionUpdate: {
        coValue: coValueRow.rowID,
        sessionID,
        lastIdx: newLastIdx,
        lastSignature,
        bytesSinceLastSignature: newBytesSinceLastSignature,
      },
      sessionRow,
    });

    if (shouldEnableSignatureStreaming) {
      await this.dbClient.addSignatureAfter({
        sessionRowID: newRowID,
        idx: newLastIdx - 1,
        signature: lastSignature,
      });
    }

    let nextIdx = sessionRow?.lastIdx || 0;

    for (const transaction of actuallyNewTransactions) {
      await this.dbClient.addTransaction(newRowID, nextIdx, transaction);
      nextIdx++;
    }
  }
}
