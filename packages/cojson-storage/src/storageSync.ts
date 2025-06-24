import {
  CojsonInternalTypes,
  MAX_RECOMMENDED_TX_SIZE,
  type SessionID,
  type StorageAPI,
  cojsonInternals,
} from "cojson";
import { collectNewTxs, getDependedOnCoValues } from "./syncUtils.js";
import type {
  DBClientInterfaceSync,
  SignatureAfterRow,
  StoredCoValueRow,
  StoredSessionRow,
} from "./types.js";

import RawCoID = CojsonInternalTypes.RawCoID;

export class StorageManagerSync implements StorageAPI {
  private readonly dbClient: DBClientInterfaceSync;
  private pushCallback: (
    data:
      | CojsonInternalTypes.NewContentMessage
      | CojsonInternalTypes.KnownStateMessage,
  ) => void = () => {};
  private loadedCoValues = new Set<RawCoID>();

  constructor(dbClient: DBClientInterfaceSync) {
    this.dbClient = dbClient;
  }

  setPushCallback(
    callback: (
      data:
        | CojsonInternalTypes.NewContentMessage
        | CojsonInternalTypes.KnownStateMessage,
    ) => void,
  ): void {
    this.pushCallback = callback;
  }

  async load(id: string) {
    const coValueRow = this.dbClient.getCoValue(id);

    if (!coValueRow) {
      return false;
    }

    const allCoValueSessions = this.dbClient.getCoValueSessions(
      coValueRow.rowID,
    );

    const signaturesBySession = new Map<
      SessionID,
      Pick<SignatureAfterRow, "idx" | "signature">[]
    >();

    let contentStreaming = false;
    for (const sessionRow of allCoValueSessions) {
      const signatures = this.dbClient.getSignatures(sessionRow.rowID, 0);

      if (signatures.length > 0) {
        contentStreaming = true;
        signaturesBySession.set(sessionRow.sessionID, signatures);
      }
    }

    /**
     * If we are going to send the content in streaming, we send before a known state message
     * to let the peer know how many transactions we are going to send.
     */
    if (contentStreaming) {
      const newCoValueKnownState: CojsonInternalTypes.CoValueKnownState = {
        id: coValueRow.id,
        header: true,
        sessions: {},
      };

      for (const sessionRow of allCoValueSessions) {
        newCoValueKnownState.sessions[sessionRow.sessionID] =
          sessionRow.lastIdx;
      }

      this.pushCallback({
        action: "known",
        ...newCoValueKnownState,
      });
    }

    this.loadedCoValues.add(coValueRow.id);

    let contentMessage = {
      action: "content",
      id: coValueRow.id,
      header: coValueRow.header,
      new: {},
      priority: cojsonInternals.getPriorityFromHeader(coValueRow.header),
    } satisfies CojsonInternalTypes.NewContentMessage;

    for (const sessionRow of allCoValueSessions) {
      const signatures = signaturesBySession.get(sessionRow.sessionID) || [];

      let idx = 0;

      signatures.push({
        idx: sessionRow.lastIdx,
        signature: sessionRow.lastSignature,
      });

      for (const signature of signatures) {
        const newTxsInSession = this.dbClient.getNewTransactionInSession(
          sessionRow.rowID,
          idx,
          signature.idx,
        );

        collectNewTxs({
          newTxsInSession,
          contentMessage,
          sessionRow,
          firstNewTxIdx: idx,
          signature: signature.signature,
        });

        idx = signature.idx + 1;

        if (signatures.length > 1) {
          await this.pushContentWithDependencies(coValueRow, contentMessage);
          contentMessage = {
            action: "content",
            id: coValueRow.id,
            header: coValueRow.header,
            new: {},
            priority: cojsonInternals.getPriorityFromHeader(coValueRow.header),
          } satisfies CojsonInternalTypes.NewContentMessage;

          // Introduce a delay to not block the main thread
          // for the entire content processing
          await new Promise((resolve) => setTimeout(resolve));
        }
      }
    }

    if (Object.keys(contentMessage.new).length === 0 && contentStreaming) {
      return true;
    }

    this.pushContentWithDependencies(coValueRow, contentMessage);

    return true;
  }

  async pushContentWithDependencies(
    coValueRow: StoredCoValueRow,
    contentMessage: CojsonInternalTypes.NewContentMessage,
  ) {
    const dependedOnCoValuesList = getDependedOnCoValues(
      coValueRow.header,
      contentMessage,
    );

    for (const dependedOnCoValue of dependedOnCoValuesList) {
      if (this.loadedCoValues.has(dependedOnCoValue)) {
        continue;
      }

      await this.load(dependedOnCoValue);
    }

    this.pushCallback(contentMessage);
  }

  async store(
    msg: CojsonInternalTypes.NewContentMessage,
  ): Promise<CojsonInternalTypes.KnownStateMessage> {
    const coValueRow = this.dbClient.getCoValue(msg.id);

    // We have no info about coValue header
    const invalidAssumptionOnHeaderPresence = !msg.header && !coValueRow;

    if (invalidAssumptionOnHeaderPresence) {
      return {
        action: "known" as const,
        id: msg.id,
        header: false,
        sessions: {},
        isCorrection: true,
      };
    }

    const storedCoValueRowID: number = coValueRow
      ? coValueRow.rowID
      : this.dbClient.addCoValue(msg);

    const ourKnown: CojsonInternalTypes.CoValueKnownState = {
      id: msg.id,
      header: true,
      sessions: {},
    };

    let invalidAssumptions = false;

    for (const sessionID of Object.keys(msg.new) as SessionID[]) {
      this.dbClient.transaction(() => {
        const sessionRow = this.dbClient.getSingleCoValueSession(
          storedCoValueRowID,
          sessionID,
        );

        if (sessionRow) {
          ourKnown.sessions[sessionRow.sessionID] = sessionRow.lastIdx;
        }

        if ((sessionRow?.lastIdx || 0) < (msg.new[sessionID]?.after || 0)) {
          invalidAssumptions = true;
        } else {
          const newLastIdx = this.putNewTxs(
            msg,
            sessionID,
            sessionRow,
            storedCoValueRowID,
          );
          ourKnown.sessions[sessionID] = newLastIdx;
        }
      });
    }

    if (invalidAssumptions) {
      return {
        action: "known" as const,
        ...ourKnown,
        isCorrection: invalidAssumptions,
      };
    }

    return {
      action: "known" as const,
      ...ourKnown,
    };
  }

  private putNewTxs(
    msg: CojsonInternalTypes.NewContentMessage,
    sessionID: SessionID,
    sessionRow: StoredSessionRow | undefined,
    storedCoValueRowID: number,
  ) {
    const newTransactions = msg.new[sessionID]?.newTransactions || [];

    const actuallyNewOffset =
      (sessionRow?.lastIdx || 0) - (msg.new[sessionID]?.after || 0);

    const actuallyNewTransactions = newTransactions.slice(actuallyNewOffset);

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

    const nextIdx = sessionRow?.lastIdx || 0;

    if (!msg.new[sessionID]) throw new Error("Session ID not found");

    const sessionUpdate = {
      coValue: storedCoValueRowID,
      sessionID,
      lastIdx: newLastIdx,
      lastSignature: msg.new[sessionID].lastSignature,
      bytesSinceLastSignature: newBytesSinceLastSignature,
    };

    const sessionRowID: number = this.dbClient.addSessionUpdate({
      sessionUpdate,
      sessionRow,
    });

    if (shouldWriteSignature) {
      this.dbClient.addSignatureAfter({
        sessionRowID,
        idx: newLastIdx - 1,
        signature: msg.new[sessionID].lastSignature,
      });
    }

    actuallyNewTransactions.map((newTransaction, i) =>
      this.dbClient.addTransaction(sessionRowID, nextIdx + i, newTransaction),
    );

    return newLastIdx;
  }
}
