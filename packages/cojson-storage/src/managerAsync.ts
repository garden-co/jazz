import {
  CojsonInternalTypes,
  MAX_RECOMMENDED_TX_SIZE,
  type OutgoingSyncQueue,
  type SessionID,
  type SyncMessage,
  cojsonInternals,
  emptyKnownState,
  logger,
} from "cojson";
import { collectNewTxs, getDependedOnCoValues } from "./syncUtils.js";
import type {
  DBClientInterfaceAsync,
  SignatureAfterRow,
  StoredCoValueRow,
  StoredSessionRow,
} from "./types.js";
import NewContentMessage = CojsonInternalTypes.NewContentMessage;
import KnownStateMessage = CojsonInternalTypes.KnownStateMessage;
import RawCoID = CojsonInternalTypes.RawCoID;

type OutputMessageMap = Record<
  RawCoID,
  { knownMessage: KnownStateMessage; contentMessages?: NewContentMessage[] }
>;

export class StorageManagerAsync {
  private readonly toLocalNode: OutgoingSyncQueue;
  private readonly dbClient: DBClientInterfaceAsync;

  private loadedCoValues = new Set<RawCoID>();

  constructor(
    dbClient: DBClientInterfaceAsync,
    toLocalNode: OutgoingSyncQueue,
  ) {
    this.toLocalNode = toLocalNode;
    this.dbClient = dbClient;
  }

  async handleSyncMessage(msg: SyncMessage) {
    switch (msg.action) {
      case "load":
        await this.handleLoad(msg);
        break;
      case "content":
        await this.handleContent(msg);
        break;
      case "known":
        this.handleKnown(msg);
        break;
      case "done":
        this.handleDone(msg);
        break;
    }
  }

  async sendNewContent(
    coValueKnownState: CojsonInternalTypes.CoValueKnownState,
  ) {
    const coValueRow = await this.dbClient.getCoValue(coValueKnownState.id);

    if (!coValueRow) {
      const emptyKnownMessage: KnownStateMessage = {
        action: "known",
        ...emptyKnownState(coValueKnownState.id),
      };

      this.sendStateMessage(emptyKnownMessage);
      return;
    }

    const allCoValueSessions = await this.dbClient.getCoValueSessions(
      coValueRow.rowID,
    );

    const signaturesBySession = new Map<
      SessionID,
      Pick<SignatureAfterRow, "idx" | "signature">[]
    >();

    let contentStreaming = false;
    for (const sessionRow of allCoValueSessions) {
      const signatures = await this.dbClient.getSignatures(sessionRow.rowID, 0);

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

      this.sendStateMessage({
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
      if (
        sessionRow.lastIdx <=
        (coValueKnownState.sessions[sessionRow.sessionID] || 0)
      ) {
        continue;
      }

      const signatures = signaturesBySession.get(sessionRow.sessionID) || [];

      let idx = 0;

      signatures.push({
        idx: sessionRow.lastIdx,
        signature: sessionRow.lastSignature,
      });

      for (const signature of signatures) {
        const newTxsInSession = await this.dbClient.getNewTransactionInSession(
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
          await this.sendContentMessage(coValueRow, contentMessage);
          contentMessage = {
            action: "content",
            id: coValueRow.id,
            header: coValueRow.header,
            new: {},
            priority: cojsonInternals.getPriorityFromHeader(coValueRow.header),
          } satisfies CojsonInternalTypes.NewContentMessage;
        }
      }
    }

    if (Object.keys(contentMessage.new).length === 0 && contentStreaming) {
      return;
    }

    return this.sendContentMessage(coValueRow, contentMessage);
  }

  async sendContentMessage(
    coValueRow: StoredCoValueRow,
    contentMessage: CojsonInternalTypes.NewContentMessage,
  ) {
    const dependedOnCoValuesList = getDependedOnCoValues({
      coValueRow,
      newContentMessages: [contentMessage],
    });

    for (const dependedOnCoValue of dependedOnCoValuesList) {
      if (this.loadedCoValues.has(dependedOnCoValue)) {
        continue;
      }

      await this.sendNewContent({
        id: dependedOnCoValue,
        header: false,
        sessions: {},
      });
    }

    this.sendStateMessage(contentMessage);
  }

  handleLoad(msg: CojsonInternalTypes.LoadMessage) {
    return this.sendNewContent(msg);
  }

  async handleContent(msg: CojsonInternalTypes.NewContentMessage) {
    const coValueRow = await this.dbClient.getCoValue(msg.id);

    // We have no info about coValue header
    const invalidAssumptionOnHeaderPresence = !msg.header && !coValueRow;

    if (invalidAssumptionOnHeaderPresence) {
      return this.sendStateMessage({
        action: "known",
        id: msg.id,
        header: false,
        sessions: {},
        isCorrection: true,
      });
    }

    const storedCoValueRowID: number = coValueRow
      ? coValueRow.rowID
      : await this.dbClient.addCoValue(msg);

    const ourKnown: CojsonInternalTypes.CoValueKnownState = {
      id: msg.id,
      header: true,
      sessions: {},
    };

    let invalidAssumptions = false;

    for (const sessionID of Object.keys(msg.new) as SessionID[]) {
      await this.dbClient.transaction(async () => {
        const sessionRow = await this.dbClient.getSingleCoValueSession(
          storedCoValueRowID,
          sessionID,
        );

        if (sessionRow) {
          ourKnown.sessions[sessionRow.sessionID] = sessionRow.lastIdx;
        }

        if ((sessionRow?.lastIdx || 0) < (msg.new[sessionID]?.after || 0)) {
          invalidAssumptions = true;
        } else {
          const newLastIdx = await this.putNewTxs(
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
      this.sendStateMessage({
        action: "known",
        ...ourKnown,
        isCorrection: invalidAssumptions,
      });
    } else {
      this.sendStateMessage({
        action: "known",
        ...ourKnown,
      });
    }
  }

  private async putNewTxs(
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

    const sessionRowID: number = await this.dbClient.addSessionUpdate({
      sessionUpdate,
      sessionRow,
    });

    if (shouldWriteSignature) {
      await this.dbClient.addSignatureAfter({
        sessionRowID,
        idx: newLastIdx - 1,
        signature: msg.new[sessionID].lastSignature,
      });
    }

    await Promise.all(
      actuallyNewTransactions.map((newTransaction, i) =>
        this.dbClient.addTransaction(sessionRowID, nextIdx + i, newTransaction),
      ),
    );

    return newLastIdx;
  }

  handleKnown(_msg: CojsonInternalTypes.KnownStateMessage) {
    // We don't intend to use the storage (SQLite,IDB,etc.) itself as a synchronisation mechanism, so we can ignore the known messages
  }

  handleDone(_msg: CojsonInternalTypes.DoneMessage) {}

  async sendStateMessage(
    msg:
      | CojsonInternalTypes.KnownStateMessage
      | CojsonInternalTypes.NewContentMessage,
  ): Promise<unknown> {
    return this.toLocalNode.push(msg).catch((e) =>
      logger.error(`Error sending ${msg.action} state, id ${msg.id}`, {
        err: e,
      }),
    );
  }
}
