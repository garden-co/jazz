import { metrics, UpDownCounter } from "@opentelemetry/api";
import {
  createContentMessage,
  exceedsRecommendedSize,
  getTransactionSize,
} from "../coValueContentMessage.js";
import {
  CoValueCore,
  logger,
  RawCoID,
  type SessionID,
  type StorageAPI,
} from "../exports.js";
import {
  CoValueKnownState,
  emptyKnownState,
  NewContentMessage,
} from "../sync.js";
import { StorageKnownState } from "./knownState.js";
import {
  collectNewTxs,
  getDependedOnCoValues,
  getNewTransactionsSize,
} from "./syncUtils.js";
import type {
  CorrectionCallback,
  DBClientInterfaceSync,
  SignatureAfterRow,
  StoredCoValueRow,
  StoredSessionRow,
} from "./types.js";

export class StorageApiSync implements StorageAPI {
  private streamingCounter: UpDownCounter;

  private readonly dbClient: DBClientInterfaceSync;
  private loadedCoValues = new Set<RawCoID>();

  constructor(dbClient: DBClientInterfaceSync) {
    this.dbClient = dbClient;
    this.streamingCounter = metrics
      .getMeter("cojson")
      .createUpDownCounter(`jazz.storage.streaming`, {
        description: "Number of streaming coValues",
        unit: "1",
      });
    this.streamingCounter.add(0);
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
    done?: (found: boolean) => void,
  ) {
    const coValueRow = this.dbClient.getCoValue(id);

    if (!coValueRow) {
      done?.(false);
      return;
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

    const knownState = this.knwonStates.getKnownState(coValueRow.id);
    knownState.header = true;

    for (const sessionRow of allCoValueSessions) {
      knownState.sessions[sessionRow.sessionID] = sessionRow.lastIdx;
    }

    this.loadedCoValues.add(coValueRow.id);

    let contentMessage = createContentMessage(coValueRow.id, coValueRow.header);

    if (contentStreaming) {
      this.streamingCounter.add(1);
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
          this.pushContentWithDependencies(
            coValueRow,
            contentMessage,
            callback,
          );
          contentMessage = createContentMessage(
            coValueRow.id,
            coValueRow.header,
          );

          // Introduce a delay to not block the main thread
          // for the entire content processing
          await new Promise((resolve) => setTimeout(resolve));
        }
      }
    }

    const hasNewContent = Object.keys(contentMessage.new).length > 0;

    // If there is no new content but steaming is not active, it's the case for a coValue with the header but no transactions
    // For streaming the push has already been done in the loop above
    if (hasNewContent || !contentStreaming) {
      this.pushContentWithDependencies(coValueRow, contentMessage, callback);
    }

    if (contentStreaming) {
      this.streamingCounter.add(-1);
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

    for (const dependedOnCoValue of dependedOnCoValuesList) {
      if (this.loadedCoValues.has(dependedOnCoValue)) {
        continue;
      }

      this.loadCoValue(dependedOnCoValue, pushCallback);
    }

    pushCallback(contentMessage);
  }

  store(msg: NewContentMessage, correctionCallback: CorrectionCallback) {
    return this.storeSingle(msg, correctionCallback);
  }

  /**
   * This function is called when the storage lacks the information required to store the incoming content.
   *
   * It triggers a `correctionCallback` to ask the syncManager to provide the missing information.
   */
  private handleCorrection(
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
      const success = this.storeSingle(msg, (knownState) => {
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

  private storeSingle(
    msg: NewContentMessage,
    correctionCallback: CorrectionCallback,
  ): boolean {
    const id = msg.id;
    const coValueRow = this.dbClient.getCoValue(id);

    // We have no info about coValue header
    const invalidAssumptionOnHeaderPresence = !msg.header && !coValueRow;

    if (invalidAssumptionOnHeaderPresence) {
      const knownState = emptyKnownState(id as RawCoID);
      this.knwonStates.setKnownState(id, knownState);

      return this.handleCorrection(knownState, correctionCallback);
    }

    const storedCoValueRowID: number = coValueRow
      ? coValueRow.rowID
      : this.dbClient.addCoValue(msg);

    const knownState = this.knwonStates.getKnownState(id);
    knownState.header = true;

    let invalidAssumptions = false;

    for (const sessionID of Object.keys(msg.new) as SessionID[]) {
      this.dbClient.transaction(() => {
        const sessionRow = this.dbClient.getSingleCoValueSession(
          storedCoValueRowID,
          sessionID,
        );

        if (sessionRow) {
          knownState.sessions[sessionRow.sessionID] = sessionRow.lastIdx;
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

  private putNewTxs(
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

    const newLastIdx =
      (sessionRow?.lastIdx || 0) + actuallyNewTransactions.length;

    let shouldWriteSignature = false;

    if (exceedsRecommendedSize(bytesSinceLastSignature, newTransactionsSize)) {
      shouldWriteSignature = true;
      bytesSinceLastSignature = 0;
    } else {
      bytesSinceLastSignature += newTransactionsSize;
    }

    const nextIdx = sessionRow?.lastIdx || 0;

    if (!msg.new[sessionID]) throw new Error("Session ID not found");

    const sessionUpdate = {
      coValue: storedCoValueRowID,
      sessionID,
      lastIdx: newLastIdx,
      lastSignature: msg.new[sessionID].lastSignature,
      bytesSinceLastSignature,
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

  waitForSync(id: string, coValue: CoValueCore) {
    return this.knwonStates.waitForSync(id, coValue);
  }

  close() {
    return undefined;
  }
}
