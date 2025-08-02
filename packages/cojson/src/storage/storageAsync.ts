import {
  createContentMessage,
  exceedsRecommendedSize,
  getTransactionSize,
} from "../coValueContentMessage.js";
import {
  type CoValueCore,
  type RawCoID,
  type SessionID,
  type StorageAPI,
  logger,
} from "../exports.js";
import { StoreQueue } from "../queue/StoreQueue.js";
import {
  CoValueKnownState,
  NewContentMessage,
  emptyKnownState,
} from "../sync.js";
import { StorageKnownState } from "./knownState.js";
import {
  collectNewTxs,
  getDependedOnCoValues,
  getNewTransactionsSize,
} from "./syncUtils.js";
import type {
  CorrectionCallback,
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
    knownState.header = true;

    for (const sessionRow of allCoValueSessions) {
      knownState.sessions[sessionRow.sessionID] = sessionRow.lastIdx;
    }

    this.loadedCoValues.add(coValueRow.id);

    let contentMessage = createContentMessage(coValueRow.id, coValueRow.header);

    if (contentStreaming) {
      contentMessage.expectContentUntil = knownState["sessions"];
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
          contentMessage = createContentMessage(
            coValueRow.id,
            coValueRow.header,
          );
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

  storeQueue = new StoreQueue();

  async store(msg: NewContentMessage, correctionCallback: CorrectionCallback) {
    /**
     * The store operations must be done one by one, because we can't start a new transaction when there
     * is already a transaction open.
     */
    this.storeQueue.push(msg, correctionCallback);

    this.storeQueue.processQueue(async (data, correctionCallback) => {
      return this.storeSingle(data, correctionCallback);
    });
  }

  /**
   * This function is called when the storage lacks the information required to store the incoming content.
   *
   * It triggers a `correctionCallback` to ask the syncManager to provide the missing information.
   *
   * The correction is applied immediately, to ensure that, when applicable, the dependent content in the queue won't require additional corrections.
   */
  private async handleCorrection(
    knownState: CoValueKnownState,
    correctionCallback: CorrectionCallback,
  ) {
    const correction = correctionCallback(knownState);

    if (!correction) {
      logger.error("Correction callback returned undefined", {
        knownState,
        correction: correction ?? null,
      });
      return false;
    }

    for (const msg of correction) {
      const success = await this.storeSingle(msg, (knownState) => {
        logger.error("Double correction requested", {
          msg,
          knownState,
        });
        return undefined;
      });

      if (!success) {
        return false;
      }
    }

    return true;
  }

  private async storeSingle(
    msg: NewContentMessage,
    correctionCallback: CorrectionCallback,
  ): Promise<boolean> {
    if (this.storeQueue.closed) {
      return false;
    }

    const id = msg.id;
    const coValueRow = await this.dbClient.getCoValue(id);

    // We have no info about coValue header
    const invalidAssumptionOnHeaderPresence = !msg.header && !coValueRow;

    if (invalidAssumptionOnHeaderPresence) {
      const knownState = emptyKnownState(id as RawCoID);
      this.knwonStates.setKnownState(id, knownState);

      return this.handleCorrection(knownState, correctionCallback);
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
      return this.handleCorrection(knownState, correctionCallback);
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
    const lastIdx = sessionRow?.lastIdx || 0;

    const actuallyNewOffset = lastIdx - (msg.new[sessionID]?.after || 0);

    const actuallyNewTransactions = newTransactions.slice(actuallyNewOffset);

    if (actuallyNewTransactions.length === 0) {
      return lastIdx;
    }

    let bytesSinceLastSignature = sessionRow?.bytesSinceLastSignature || 0;
    const newTransactionsSize = getNewTransactionsSize(actuallyNewTransactions);

    const newLastIdx = lastIdx + actuallyNewTransactions.length;

    let shouldWriteSignature = false;

    if (exceedsRecommendedSize(bytesSinceLastSignature, newTransactionsSize)) {
      shouldWriteSignature = true;
      bytesSinceLastSignature = 0;
    } else {
      bytesSinceLastSignature += newTransactionsSize;
    }

    const nextIdx = lastIdx;

    if (!msg.new[sessionID]) throw new Error("Session ID not found");

    const sessionUpdate = {
      coValue: storedCoValueRowID,
      sessionID,
      lastIdx: newLastIdx,
      lastSignature: msg.new[sessionID].lastSignature,
      bytesSinceLastSignature,
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
    return this.storeQueue.close();
  }
}
