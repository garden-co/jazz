import {
  createContentMessage,
  exceedsRecommendedSize,
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
  NewContentMessage,
  type PeerID,
  type SessionNewContent,
} from "../sync.js";
import {
  CoValueKnownState,
  emptyKnownState,
  setSessionCounter,
} from "../knownState.js";
import { StorageKnownState } from "./knownState.js";
import { DeletedCoValuesEraserScheduler } from "./DeletedCoValuesEraserScheduler.js";
import {
  collectNewTxs,
  getDependedOnCoValues,
  getNewTransactionsSize,
} from "./syncUtils.js";
import type {
  CorrectionCallback,
  DBClientInterfaceAsync,
  DBTransactionInterfaceAsync,
  ReplaceSessionHistoryInput,
  SignatureAfterRow,
  StoredCoValueRow,
  StoredSessionRow,
  StorageReconciliationAcquireResult,
} from "./types.js";
import { isDeleteSessionID } from "../ids.js";

export class StorageApiAsync implements StorageAPI {
  private readonly dbClient: DBClientInterfaceAsync;

  private deletedCoValuesEraserScheduler:
    | DeletedCoValuesEraserScheduler
    | undefined;
  private eraserController: AbortController | undefined;
  /**
   * Keeps track of CoValues that are in memory, to avoid reloading them from storage
   * when it isn't necessary
   */
  private inMemoryCoValues = new Set<RawCoID>();

  // Track pending loads to deduplicate concurrent requests
  private pendingKnownStateLoads = new Map<
    string,
    Promise<CoValueKnownState | undefined>
  >();

  constructor(dbClient: DBClientInterfaceAsync) {
    this.dbClient = dbClient;
  }

  knownStates = new StorageKnownState();

  getKnownState(id: string): CoValueKnownState {
    return this.knownStates.getKnownState(id);
  }

  loadKnownState(
    id: string,
    callback: (knownState: CoValueKnownState | undefined) => void,
  ): void {
    // Check in-memory cache first
    const cached = this.knownStates.getCachedKnownState(id);
    if (cached) {
      callback(cached);
      return;
    }

    // Check if there's already a pending load for this ID (deduplication)
    const pending = this.pendingKnownStateLoads.get(id);
    if (pending) {
      // Ensure callback is always called, even if pending fails unexpectedly
      pending.then(callback, () => callback(undefined));
      return;
    }

    // Start new load and track it for deduplication
    const loadPromise = this.dbClient
      .getCoValueKnownState(id)
      .then((knownState) => {
        if (knownState) {
          // Cache for future use
          this.knownStates.setKnownState(id, knownState);
        }
        return knownState;
      })
      .catch((err) => {
        // Error handling contract:
        // - Log warning
        // - Behave like "not found" so callers can fall back (full load / load from peers)
        logger.warn("Failed to load knownState from storage", { id, err });
        return undefined;
      })
      .finally(() => {
        // Remove from pending map after completion (success or failure)
        this.pendingKnownStateLoads.delete(id);
      });

    this.pendingKnownStateLoads.set(id, loadPromise);
    loadPromise.then(callback);
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
    this.interruptEraser("load");
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

    const knownState = this.knownStates.getKnownState(coValueRow.id);
    knownState.header = true;

    for (const sessionRow of allCoValueSessions) {
      setSessionCounter(
        knownState.sessions,
        sessionRow.sessionID,
        sessionRow.lastIdx,
      );
    }

    this.inMemoryCoValues.add(coValueRow.id);

    let contentMessage = createContentMessage(coValueRow.id, coValueRow.header);

    if (contentStreaming) {
      contentMessage.expectContentUntil = knownState.sessions;
    }

    for (const sessionRow of allCoValueSessions) {
      const signatures = signaturesBySession.get(sessionRow.sessionID) || [];

      let idx = 0;

      const lastSignature = signatures[signatures.length - 1];

      if (lastSignature?.signature !== sessionRow.lastSignature) {
        signatures.push({
          idx: sessionRow.lastIdx,
          signature: sessionRow.lastSignature,
        });
      }

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

    this.knownStates.handleUpdate(coValueRow.id, knownState);
    done?.(true);
  }

  private async pushContentWithDependencies(
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
      if (this.inMemoryCoValues.has(dependedOnCoValue)) {
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

  async store(
    msg: NewContentMessage | ReplaceSessionHistoryInput,
    correctionCallback: CorrectionCallback,
  ) {
    /**
     * The store operations must be done one by one, because we can't start a new transaction when there
     * is already a transaction open.
     */
    this.storeQueue.push(msg, correctionCallback);

    this.storeQueue.processQueue(async (data, correctionCallback) => {
      this.interruptEraser("store");
      if (data.action === "replaceSessionHistory") {
        return this.storeSingleSessionReplacement(data);
      }

      return this.storeSingle(data, correctionCallback);
    });
  }

  private interruptEraser(reason: string) {
    // Cooperative cancellation: a DB transaction already in progress will complete,
    // but the eraser loop will stop starting further work at its next abort check.
    if (this.eraserController) {
      this.eraserController.abort(reason);
      this.eraserController = undefined;
    }
  }

  async eraseAllDeletedCoValues() {
    const ids = await this.dbClient.getAllCoValuesWaitingForDelete();

    this.eraserController = new AbortController();
    const signal = this.eraserController.signal;

    for (const id of ids) {
      if (signal.aborted) {
        return;
      }

      await this.dbClient.eraseCoValueButKeepTombstone(id);
    }
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
    this.interruptEraser("store");
    if (this.storeQueue.closed) {
      return false;
    }

    let invalidAssumptions = false;
    const id = msg.id;
    const knownState = this.knownStates.getKnownState(id);
    const storedCoValueRowID = await this.dbClient.upsertCoValue(
      id,
      msg.header,
    );

    if (!storedCoValueRowID) {
      const emptyState = emptyKnownState(id as RawCoID);
      this.knownStates.setKnownState(id, emptyState);
      return this.handleCorrection(emptyState, correctionCallback);
    }

    await this.dbClient.transaction(async (tx) => {
      knownState.header = true;

      for (const sessionID of Object.keys(msg.new) as SessionID[]) {
        const sessionRow = await tx.getSingleCoValueSession(
          storedCoValueRowID,
          sessionID,
        );

        if (this.deletedValues.has(id) && isDeleteSessionID(sessionID)) {
          await tx.markCoValueAsDeleted(id);
        }

        if (sessionRow) {
          setSessionCounter(
            knownState.sessions,
            sessionRow.sessionID,
            sessionRow.lastIdx,
          );
        }

        const lastIdx = sessionRow?.lastIdx || 0;
        const after = msg.new[sessionID]?.after || 0;

        if (lastIdx < after) {
          invalidAssumptions = true;
        } else {
          const newLastIdx = await this.putNewTxs(
            tx,
            sessionID,
            msg.new[sessionID],
            sessionRow,
            storedCoValueRowID,
          );
          setSessionCounter(knownState.sessions, sessionID, newLastIdx);
        }
      }
    });

    this.markCoValueUpdated(id, knownState);

    if (invalidAssumptions) {
      return this.handleCorrection(knownState, correctionCallback);
    }

    return true;
  }

  private async storeSingleSessionReplacement(
    input: ReplaceSessionHistoryInput,
  ): Promise<boolean> {
    const { coValueId, sessionID, content } = input;

    const coValueRowID = await this.dbClient.upsertCoValue(coValueId);

    if (!coValueRowID) {
      throw new Error(
        `Cannot replace session history for unknown CoValue ${coValueId}`,
      );
    }

    await this.dbClient.transaction(async (tx) => {
      const existing = await tx.getSingleCoValueSession(
        coValueRowID,
        sessionID,
      );

      if (existing) {
        await tx.deleteTransactionsForSession(existing.rowID);
        await tx.deleteSignaturesForSession(existing.rowID);
        await tx.deleteSession(existing.rowID);
      }

      if (content.length === 0) {
        return;
      }

      let nextExpectedAfter = 0;
      for (const piece of content) {
        if (piece.after !== nextExpectedAfter) {
          throw new Error(
            `Invalid replacement content continuity for ${coValueId}/${sessionID}: expected after=${nextExpectedAfter}, got after=${piece.after}`,
          );
        }
        nextExpectedAfter += piece.newTransactions.length;
      }

      const lastPiece = content[content.length - 1];

      if (!lastPiece) {
        return;
      }

      for (let i = 0; i < content.length; i++) {
        const piece = content[i]!;
        const isLastPiece = i === content.length - 1;

        const currentSessionRow = await tx.getSingleCoValueSession(
          coValueRowID,
          sessionID,
        );
        await this.putNewTxs(
          tx,
          sessionID,
          piece,
          currentSessionRow,
          coValueRowID,
          {
            forceSignatureAfter: !isLastPiece,
            disableThresholdSignature: true,
          },
        );
      }
    });

    await this.refreshKnownStateFromStorage(coValueId);

    return true;
  }

  private markCoValueUpdated(id: RawCoID, knownState: CoValueKnownState) {
    this.inMemoryCoValues.add(id);
    this.knownStates.handleUpdate(id, knownState);
  }

  private async refreshKnownStateFromStorage(id: RawCoID) {
    const knownState =
      (await this.dbClient.getCoValueKnownState(id)) ?? emptyKnownState(id);
    this.knownStates.setKnownState(id, knownState);
    this.markCoValueUpdated(id, knownState);
  }

  private async putNewTxs(
    tx: DBTransactionInterfaceAsync,
    sessionID: SessionID,
    sessionContent: SessionNewContent | undefined,
    sessionRow: StoredSessionRow | undefined,
    storedCoValueRowID: number,
    options?: {
      forceSignatureAfter?: boolean;
      disableThresholdSignature?: boolean;
    },
  ) {
    if (!sessionContent) {
      throw new Error("Session content not found");
    }

    const newTransactions = sessionContent.newTransactions;
    const lastIdx = sessionRow?.lastIdx || 0;

    const actuallyNewOffset = lastIdx - sessionContent.after;

    const actuallyNewTransactions = newTransactions.slice(actuallyNewOffset);

    if (actuallyNewTransactions.length === 0) {
      return lastIdx;
    }

    let bytesSinceLastSignature = sessionRow?.bytesSinceLastSignature || 0;
    const newTransactionsSize = getNewTransactionsSize(actuallyNewTransactions);

    const newLastIdx = lastIdx + actuallyNewTransactions.length;

    let shouldWriteSignature = false;

    if (
      !options?.disableThresholdSignature &&
      exceedsRecommendedSize(bytesSinceLastSignature, newTransactionsSize)
    ) {
      shouldWriteSignature = true;
      bytesSinceLastSignature = 0;
    } else {
      bytesSinceLastSignature += newTransactionsSize;
    }

    if (options?.forceSignatureAfter) {
      shouldWriteSignature = true;
      bytesSinceLastSignature = 0;
    }

    const nextIdx = lastIdx;

    const sessionUpdate = {
      coValue: storedCoValueRowID,
      sessionID,
      lastIdx: newLastIdx,
      lastSignature: sessionContent.lastSignature,
      bytesSinceLastSignature,
    };

    const sessionRowID: number = await tx.addSessionUpdate({
      sessionUpdate,
      sessionRow,
    });

    if (shouldWriteSignature) {
      await tx.addSignatureAfter({
        sessionRowID,
        idx: newLastIdx - 1,
        signature: sessionContent.lastSignature,
      });
    }

    await Promise.all(
      actuallyNewTransactions.map((newTransaction, i) =>
        tx.addTransaction(sessionRowID, nextIdx + i, newTransaction),
      ),
    );

    return newLastIdx;
  }

  deletedValues = new Set<RawCoID>();

  markDeleteAsValid(id: RawCoID) {
    this.deletedValues.add(id);

    if (this.deletedCoValuesEraserScheduler) {
      this.deletedCoValuesEraserScheduler.onEnqueueDeletedCoValue();
    }
  }

  enableDeletedCoValuesErasure() {
    if (this.deletedCoValuesEraserScheduler) return;

    this.deletedCoValuesEraserScheduler = new DeletedCoValuesEraserScheduler({
      run: async () => {
        // Async storage: no max-time budgeting; drain to completion when scheduled.
        await this.eraseAllDeletedCoValues();
        const remaining = await this.dbClient.getAllCoValuesWaitingForDelete();
        return { hasMore: remaining.length > 0 };
      },
    });
    this.deletedCoValuesEraserScheduler.scheduleStartupDrain();
  }

  waitForSync(id: string, coValue: CoValueCore) {
    return this.knownStates.waitForSync(id, coValue);
  }

  trackCoValuesSyncState(
    updates: { id: RawCoID; peerId: PeerID; synced: boolean }[],
    done?: () => void,
  ): void {
    this.dbClient.trackCoValuesSyncState(updates).then(() => done?.());
  }

  getCoValueIDs(
    limit: number,
    offset: number,
    callback: (batch: { id: RawCoID }[]) => void,
  ): void {
    this.dbClient.getCoValueIDs(limit, offset).then(callback);
  }

  getCoValueCount(callback: (count: number) => void): void {
    this.dbClient.getCoValueCount().then(callback);
  }

  tryAcquireStorageReconciliationLock(
    sessionId: SessionID,
    peerId: PeerID,
    callback: (result: StorageReconciliationAcquireResult) => void,
  ): void {
    this.dbClient
      .tryAcquireStorageReconciliationLock(sessionId, peerId)
      .then(callback);
  }

  renewStorageReconciliationLock(
    sessionId: SessionID,
    peerId: PeerID,
    offset: number,
  ): void {
    this.dbClient.renewStorageReconciliationLock(sessionId, peerId, offset);
  }

  releaseStorageReconciliationLock(sessionId: SessionID, peerId: PeerID): void {
    this.dbClient.releaseStorageReconciliationLock(sessionId, peerId);
  }

  getUnsyncedCoValueIDs(
    callback: (unsyncedCoValueIDs: RawCoID[]) => void,
  ): void {
    this.dbClient.getUnsyncedCoValueIDs().then(callback);
  }

  stopTrackingSyncState(id: RawCoID): void {
    this.dbClient.stopTrackingSyncState(id);
  }

  onCoValueUnmounted(id: RawCoID): void {
    this.inMemoryCoValues.delete(id);
    this.knownStates.deleteKnownState(id);
  }

  close() {
    this.deletedCoValuesEraserScheduler?.dispose();
    this.inMemoryCoValues.clear();
    this.knownStates.clear();
    return this.storeQueue.close();
  }
}
