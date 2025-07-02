import { LinkedList } from "../PriorityBasedMessageQueue.js";
import {
  type CoValueCore,
  MAX_RECOMMENDED_TX_SIZE,
  type RawCoID,
  type SessionID,
  type StorageAPI,
} from "../exports.js";
import { getPriorityFromHeader } from "../priority.js";
import {
  CoValueKnownState,
  NewContentMessage,
  emptyKnownState,
} from "../sync.js";
import { StorageKnownState } from "./knownState.js";
import { collectNewTxs, getDependedOnCoValues } from "./syncUtils.js";
import type {
  DBClientInterfaceAsync,
  SignatureAfterRow,
  StoredCoValueRow,
  StoredSessionRow,
} from "./types.js";

export class StorageApiAsync implements StorageAPI {
  private readonly dbClient: DBClientInterfaceAsync;

  private loadedCoValues = new Set<RawCoID>();

  constructor(dbClient: DBClientInterfaceAsync) {
    this.dbClient = dbClient;
  }

  knwonStates = new StorageKnownState();

  getKnownState(id: string): CoValueKnownState {
    return this.knwonStates.getKnownState(id);
  }

  async load(
    id: string,
    callback: (data: NewContentMessage) => void,
    done: (found: boolean) => void,
  ) {
    await this.loadCoValue(id, callback, done);
  }

  async loadCoValue(
    id: string,
    callback: (data: NewContentMessage) => void,
    done: (found: boolean) => void,
  ) {
    const coValueRow = await this.dbClient.getCoValue(id);

    if (!coValueRow) {
      done?.(false);
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

    await Promise.all(
      allCoValueSessions.map(async (sessionRow) => {
        const signatures = await this.dbClient.getSignatures(
          sessionRow.rowID,
          0,
        );

        if (signatures.length > 0) {
          contentStreaming = true;
          signaturesBySession.set(sessionRow.sessionID, signatures);
        }
      }),
    );

    const knownState = this.knwonStates.getKnownState(coValueRow.id);

    for (const sessionRow of allCoValueSessions) {
      knownState.sessions[sessionRow.sessionID] = sessionRow.lastIdx;
    }

    this.loadedCoValues.add(coValueRow.id);

    let contentMessage = {
      action: "content",
      id: coValueRow.id,
      header: coValueRow.header,
      new: {},
      priority: getPriorityFromHeader(coValueRow.header),
    } as NewContentMessage;

    if (contentStreaming) {
      contentMessage.streamingTarget = knownState["sessions"];
    }

    for (const sessionRow of allCoValueSessions) {
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
          // Having more than one signature means that the content needs streaming
          // So we start pushing the content to the client, and start a new content message
          await this.pushContentWithDependencies(
            coValueRow,
            contentMessage,
            callback,
          );
          contentMessage = {
            action: "content",
            id: coValueRow.id,
            header: coValueRow.header,
            new: {},
            priority: getPriorityFromHeader(coValueRow.header),
            streamingTarget: knownState["sessions"],
          } satisfies NewContentMessage;
        }
      }
    }

    const hasNewContent = Object.keys(contentMessage.new).length > 0;

    // If there is no new content but steaming is not active, it's the case for a coValue with the header but no transactions
    // For streaming the push has already been done in the loop above
    if (hasNewContent || !contentStreaming) {
      await this.pushContentWithDependencies(
        coValueRow,
        contentMessage,
        callback,
      );
    }

    this.knwonStates.handleUpdate(coValueRow.id, knownState);
    done?.(true);
  }

  async pushContentWithDependencies(
    coValueRow: StoredCoValueRow,
    contentMessage: NewContentMessage,
    pushCallback: (data: NewContentMessage) => void,
  ) {
    const dependedOnCoValuesList = getDependedOnCoValues(
      coValueRow.header,
      contentMessage,
    );

    const promises = [];

    for (const dependedOnCoValue of dependedOnCoValuesList) {
      if (this.loadedCoValues.has(dependedOnCoValue)) {
        continue;
      }

      promises.push(
        new Promise((resolve) => {
          this.loadCoValue(dependedOnCoValue, pushCallback, resolve);
        }),
      );
    }

    await Promise.all(promises);

    pushCallback(contentMessage);
  }

  storeQueue = createStoreQueue();
  processingStoreQueue = false;

  async store(
    msgs: NewContentMessage[],
    correctionCallback: (data: CoValueKnownState) => void,
  ) {
    /**
     * The store operations must be done one by one, because we can't start a new transaction when there
     * is already a transaction open.
     */
    this.storeQueue.push({ data: msgs, correctionCallback });

    if (this.processingStoreQueue) {
      return;
    }

    this.processingStoreQueue = true;

    let entry:
      | {
          data: NewContentMessage[];
          correctionCallback: (data: CoValueKnownState) => void;
        }
      | undefined;

    while ((entry = this.storeQueue.shift())) {
      for (const msg of entry.data) {
        const success = await this.storeSingle(msg, entry.correctionCallback);

        if (!success) {
          // Stop processing the messages for this entry, because the data is out of sync with storage
          // and the other transactions will be rejected anyway.
          break;
        }
      }
    }

    this.processingStoreQueue = false;
  }

  private async storeSingle(
    msg: NewContentMessage,
    correctionCallback: (data: CoValueKnownState) => void,
  ): Promise<boolean> {
    const id = msg.id;
    const coValueRow = await this.dbClient.getCoValue(id);

    // We have no info about coValue header
    const invalidAssumptionOnHeaderPresence = !msg.header && !coValueRow;

    if (invalidAssumptionOnHeaderPresence) {
      const knownState = emptyKnownState(id as RawCoID);
      this.knwonStates.setKnownState(id, knownState);

      correctionCallback(knownState);
      return false;
    }

    const storedCoValueRowID: number = coValueRow
      ? coValueRow.rowID
      : await this.dbClient.addCoValue(msg);

    const knownState = this.knwonStates.getKnownState(id);
    knownState.header = true;

    let invalidAssumptions = false;

    for (const sessionID of Object.keys(msg.new) as SessionID[]) {
      await this.dbClient.transaction(async () => {
        const sessionRow = await this.dbClient.getSingleCoValueSession(
          storedCoValueRowID,
          sessionID,
        );

        if (sessionRow) {
          knownState.sessions[sessionRow.sessionID] = sessionRow.lastIdx;
        }

        const lastIdx = sessionRow?.lastIdx || 0;
        const after = msg.new[sessionID]?.after || 0;

        if (lastIdx < after) {
          knownState.sessions[sessionID] = lastIdx;
          invalidAssumptions = true;
        } else {
          const newLastIdx = await this.putNewTxs(
            msg,
            sessionID,
            sessionRow,
            storedCoValueRowID,
          );
          knownState.sessions[sessionID] = newLastIdx;
        }
      });
    }

    this.knwonStates.handleUpdate(id, knownState);

    if (invalidAssumptions) {
      correctionCallback(knownState);
      return false;
    }

    return true;
  }

  private async putNewTxs(
    msg: NewContentMessage,
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

  waitForSync(id: string, coValue: CoValueCore) {
    return this.knwonStates.waitForSync(id, coValue);
  }

  close() {
    let entry:
      | {
          data: NewContentMessage[];
          correctionCallback: (data: CoValueKnownState) => void;
        }
      | undefined;

    // Drain the store queue
    while ((entry = this.storeQueue.shift())) {}
  }
}

function createStoreQueue() {
  return new LinkedList<{
    data: NewContentMessage[];
    correctionCallback: (data: CoValueKnownState) => void;
  }>();
}
