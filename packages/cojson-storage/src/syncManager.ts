import {
  CojsonInternalTypes,
  MAX_RECOMMENDED_TX_SIZE,
  OutgoingSyncQueue,
  SessionID,
  SyncMessage,
  cojsonInternals,
  emptyKnownState,
  unknownDataMessage,
} from "cojson";
import { collectNewTxs, getDependedOnCoValues } from "./syncUtils.js";
import {
  DBClientInterface,
  StoredCoValueRow,
  StoredSessionRow,
} from "./types.js";
import DataMessage = CojsonInternalTypes.DataMessage;
import PushMessage = CojsonInternalTypes.PushMessage;
import RawCoID = CojsonInternalTypes.RawCoID;

type OutputMessageMap = Record<
  RawCoID,
  {
    dataMessages: DataMessage[];
  }
>;

export class SyncManager {
  private readonly toLocalNode: OutgoingSyncQueue;
  private readonly dbClient: DBClientInterface;

  constructor(dbClient: DBClientInterface, toLocalNode: OutgoingSyncQueue) {
    this.toLocalNode = toLocalNode;
    this.dbClient = dbClient;
  }

  async handleSyncMessage(msg: SyncMessage) {
    switch (msg.action) {
      case "pull":
        return this.handlePull(msg);
      case "data":
        return this.handleData(msg);
      case "push":
        return this.handlePush(msg);
      case "known":
        return this.handleKnown(msg);
    }
  }

  async handleSessionUpdate({
    sessionRow,
    peerKnownState,
    newDataMessages,
  }: {
    sessionRow: StoredSessionRow;
    peerKnownState: CojsonInternalTypes.CoValueKnownState;
    newDataMessages: CojsonInternalTypes.DataMessage[];
  }) {
    if (
      sessionRow.lastIdx <= (peerKnownState.sessions[sessionRow.sessionID] || 0)
    )
      return;

    const firstNewTxIdx = peerKnownState.sessions[sessionRow.sessionID] || 0;

    const newTxsInSession = await this.dbClient.getNewTransactionInSession(
      sessionRow.rowID,
      firstNewTxIdx,
    );

    collectNewTxs({
      newTxsInSession,
      newDataMessages,
      sessionRow,
      firstNewTxIdx,
    });
  }

  // actually, it's a handlePull method
  async sendNewContent(
    coValueKnownState: CojsonInternalTypes.CoValueKnownState,
  ): Promise<void> {
    const outputMessages: OutputMessageMap =
      await this.collectCoValueData(coValueKnownState);

    // reverse it to send the top level id the last in the order
    const collectedMessages = Object.values(outputMessages).reverse();
    collectedMessages.forEach(({ dataMessages }) => {
      dataMessages?.length &&
        dataMessages.forEach((msg) => this.sendStateMessage(msg));
    });
  }

  private async collectCoValueData(
    peerKnownState: CojsonInternalTypes.CoValueKnownState,
    messageMap: OutputMessageMap = {},
    asDependencyOf?: CojsonInternalTypes.RawCoID,
  ) {
    if (messageMap[peerKnownState.id]) {
      return messageMap;
    }

    const coValueRow = await this.dbClient.getCoValue(peerKnownState.id);

    if (!coValueRow) {
      messageMap[peerKnownState.id] = {
        dataMessages: [unknownDataMessage(peerKnownState.id, asDependencyOf)],
      };
      return messageMap;
    }

    const allCoValueSessions = await this.dbClient.getCoValueSessions(
      coValueRow.rowID,
    );

    const newCoValueKnownState: CojsonInternalTypes.CoValueKnownState = {
      id: coValueRow.id,
      header: true,
      sessions: {},
    };

    const newDataMessages: CojsonInternalTypes.DataMessage[] = [
      {
        action: "data",
        known: true,
        id: coValueRow.id,
        header: coValueRow.header,
        new: {},
        priority: cojsonInternals.getPriorityFromHeader(coValueRow.header),
        asDependencyOf,
      },
    ];

    await Promise.all(
      allCoValueSessions.map((sessionRow) => {
        newCoValueKnownState.sessions[sessionRow.sessionID] =
          sessionRow.lastIdx;
        // Collect new sessions data into newContentMessages
        return this.handleSessionUpdate({
          sessionRow,
          peerKnownState,
          newDataMessages,
        });
      }),
    );

    const dependedOnCoValuesList = getDependedOnCoValues({
      coValueRow,
      newDataMessages,
    });

    messageMap[newCoValueKnownState.id] = {
      dataMessages: newDataMessages,
    };

    await Promise.all(
      dependedOnCoValuesList.map((dependedOnCoValue) =>
        this.collectCoValueData(
          {
            id: dependedOnCoValue,
            header: false,
            sessions: {},
          },
          messageMap,
          asDependencyOf || coValueRow.id,
        ),
      ),
    );

    return messageMap;
  }

  /**
   * "Pull" request must be followed by "data" message response according to the protocol:
   * - Sends new content if it exists.
   * - Sends an empty data message otherwise.
   * - Sends an empty data message with `{ known: false }` in the message if the `coValue` is unknown by local node.
   *
   * Handler initiates a new "pull" requests to load the coValue from peers if it is not known by the node.
   */
  handlePull(msg: CojsonInternalTypes.PullMessage) {
    return this.sendNewContent(msg);
  }

  /**
   * "Push" request
   * - must be followed by "ack" message response according to the protocol.
   * - may carry along new data txs to be added
   */
  async handlePush(msg: PushMessage) {
    const coValueRow = await this.dbClient.getCoValue(msg.id);

    // We have no info about coValue header
    const invalidAssumptionOnHeaderPresence = !msg.header && !coValueRow;

    if (invalidAssumptionOnHeaderPresence) {
      return this.sendStateMessage({
        ...emptyKnownState(msg.id),
        action: "pull",
      });
    }

    const { isMissingTransactions, ourKnown } = await this.addTransactions(
      coValueRow,
      msg,
    );

    if (isMissingTransactions) {
      return this.sendStateMessage({
        action: "pull",
        ...ourKnown,
      });
    }

    return this.sendStateMessage({
      action: "ack",
      ...ourKnown,
    });
  }

  /**
   * "Data" is a response to our "pull" message. It's always some data we asked for, initially.
   * It's a terminal message which must not be responded to.
   * At this stage the coValue state is considered synced between the peer and the node.
   */
  async handleData(msg: DataMessage) {
    const coValueRow = await this.dbClient.getCoValue(msg.id);

    // We have no info about coValue header
    const invalidAssumptionOnHeaderPresence = !msg.header && !coValueRow;

    if (invalidAssumptionOnHeaderPresence) {
      console.error(
        'invalidAssumptionOnHeaderPresence. We should never be here. "Data" action is a response to our specific request.',
      );
      return;
    }

    const { isMissingTransactions } = await this.addTransactions(
      coValueRow,
      msg,
    );

    if (isMissingTransactions) {
      console.error(
        'needMissingTransactions. We should never be here. "Data" action is a response to our specific request.',
      );
      return;
    }
  }

  private async addTransactions(
    coValueRow: StoredCoValueRow | undefined,
    msg: DataMessage | PushMessage,
  ) {
    const storedCoValueRowID: number = coValueRow
      ? coValueRow.rowID
      : await this.dbClient.addCoValue(msg);

    const allOurSessionsEntries =
      await this.dbClient.getCoValueSessions(storedCoValueRowID);

    const allOurSessions: {
      [sessionID: SessionID]: StoredSessionRow;
    } = Object.fromEntries(
      allOurSessionsEntries.map((row) => [row.sessionID, row]),
    );

    const ourKnown: CojsonInternalTypes.CoValueKnownState = {
      id: msg.id,
      header: true,
      sessions: {},
    };

    let isMissingTransactions = false;

    await this.dbClient.unitOfWork(() =>
      (Object.keys(msg.new) as SessionID[]).map((sessionID) => {
        const sessionRow = allOurSessions[sessionID];
        if (sessionRow) {
          ourKnown.sessions[sessionRow.sessionID] = sessionRow.lastIdx;
        }

        if ((sessionRow?.lastIdx || 0) < (msg.new[sessionID]?.after || 0)) {
          isMissingTransactions = true;
        } else {
          return this.putNewTxs(msg, sessionID, sessionRow, storedCoValueRowID);
        }
      }),
    );
    return { ourKnown, isMissingTransactions };
  }

  private async putNewTxs(
    msg: DataMessage | PushMessage,
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

    const sessionUpdate = {
      coValue: storedCoValueRowID,
      sessionID,
      lastIdx: newLastIdx,
      lastSignature: msg.new[sessionID]!.lastSignature,
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
        signature: msg.new[sessionID]!.lastSignature,
      });
    }

    return Promise.all(
      actuallyNewTransactions.map((newTransaction, i) =>
        this.dbClient.addTransaction(sessionRowID, nextIdx + i, newTransaction),
      ),
    );
  }

  handleKnown(_msg: CojsonInternalTypes.KnownStateMessage) {
    // We don't intend to use the storage (SQLite,IDB,etc.) itself as a synchronisation mechanism, so we can ignore the known messages
  }

  async sendStateMessage(msg: any): Promise<unknown> {
    return this.toLocalNode
      .push(msg)
      .catch((e) =>
        console.error(`Error sending ${msg.action} state, id ${msg.id}`, e),
      );
  }
}
