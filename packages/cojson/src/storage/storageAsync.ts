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
import { NewContentMessage, SessionNewContent, type PeerID } from "../sync.js";
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
  CoValueUpdate,
  DBClientInterfaceAsync,
  NewCoValueRow,
  NewSessionRow,
  StoredCoValueRow,
  StoredNewCoValueRow,
} from "./types.js";
import { isDeleteSessionID } from "../ids.js";
import { Transaction } from "../coValueCore/verifiedState.js";

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
    id: RawCoID,
    callback: (data: NewContentMessage) => void,
    done: (found: boolean) => void,
  ) {
    await this.loadCoValue(id, callback, done);
  }

  private async loadCoValue(
    id: RawCoID,
    callback: (data: NewContentMessage) => void,
    done: (found: boolean) => void,
  ) {
    this.interruptEraser("load");
    const coValueRow = await this.dbClient.getCoValueRow(id);

    if (!coValueRow) {
      done?.(false);
      return;
    }

    const contentStreaming = Object.values(coValueRow.sessions).some(
      (sessionRow) => Object.keys(sessionRow.signatures).length > 0,
    );

    const knownState = this.knownStates.getKnownState(coValueRow.id);
    knownState.header = true;

    for (const sessionRow of Object.values(coValueRow.sessions)) {
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

    for (const sessionRow of Object.values(coValueRow.sessions)) {
      const signatures = Object.entries(sessionRow.signatures).map(
        ([idx, signature]) => {
          return { idx: Number(idx), signature };
        },
      );

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

    const id = msg.id;
    const storedCoValueRow = await this.dbClient.getCoValueRow(id);
    const coValueRow = getUpdatedCoValueRow(storedCoValueRow, msg);

    if (!coValueRow) {
      const knownState = emptyKnownState(id as RawCoID);
      this.knownStates.setKnownState(id, knownState);

      return this.handleCorrection(knownState, correctionCallback);
    }

    const knownState = this.knownStates.getKnownState(id);
    knownState.header = true;

    await this.dbClient.transaction(async (tx) => {
      const storedCoValueRow = await tx.upsertCoValueRow(coValueRow);

      for (const sessionID of Object.keys(
        coValueRow.newTransactions,
      ) as SessionID[]) {
        const { transactions, afterIdx } =
          coValueRow.newTransactions[sessionID]!;
        const sessionRow = storedCoValueRow.sessions[sessionID]!;

        if (this.deletedValues.has(id) && isDeleteSessionID(sessionID)) {
          await tx.markCoValueAsDeleted(id);
        }

        setSessionCounter(knownState.sessions, sessionID, sessionRow.lastIdx);

        const nextIdx = afterIdx;
        await Promise.all(
          transactions.map((newTransaction, i) =>
            tx.addTransaction(sessionRow.rowID, nextIdx + i, newTransaction),
          ),
        );
      }
    });

    this.inMemoryCoValues.add(id);

    this.knownStates.handleUpdate(id, knownState);

    if (coValueRow.hasInvalidAssumptions) {
      return this.handleCorrection(knownState, correctionCallback);
    }

    return true;
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
  }

  close() {
    this.deletedCoValuesEraserScheduler?.dispose();
    this.inMemoryCoValues.clear();
    return this.storeQueue.close();
  }
}

function getUpdatedCoValueRow(
  storedCoValueRow: StoredNewCoValueRow | undefined,
  msg: NewContentMessage,
): CoValueUpdate | undefined {
  const header = msg.header ?? storedCoValueRow?.header;
  if (!header) {
    return undefined;
  }

  let invalidAssumptions = false;
  const sessions: Record<SessionID, NewSessionRow> =
    storedCoValueRow?.sessions ?? {};
  const newTransactions: Record<
    SessionID,
    { transactions: Transaction[]; afterIdx: number }
  > = {};
  for (const [_sessionID, sessionNewContent] of Object.entries(msg.new)) {
    const sessionID = _sessionID as SessionID;
    const sessionRow = sessions[sessionID];
    const lastIdx = sessionRow?.lastIdx || 0;
    const after = sessionNewContent.after;

    if (lastIdx < after) {
      invalidAssumptions = true;
    } else {
      const actuallyNewOffset = lastIdx - after;
      const actuallyNewTransactions =
        sessionNewContent.newTransactions.slice(actuallyNewOffset);
      const newLastIdx = lastIdx + actuallyNewTransactions.length;

      newTransactions[sessionID] = {
        transactions: actuallyNewTransactions,
        afterIdx: lastIdx,
      };

      const signatures = sessionRow?.signatures ?? {};
      let bytesSinceLastSignature = sessionRow?.bytesSinceLastSignature || 0;
      const newTransactionsSize = getNewTransactionsSize(
        actuallyNewTransactions,
      );
      if (
        exceedsRecommendedSize(bytesSinceLastSignature, newTransactionsSize)
      ) {
        signatures[newLastIdx - 1] = sessionNewContent.lastSignature;
        bytesSinceLastSignature = 0;
      } else {
        bytesSinceLastSignature += newTransactionsSize;
      }

      const updatedSessionRow = {
        sessionID,
        lastIdx: newLastIdx,
        lastSignature: sessionNewContent.lastSignature,
        bytesSinceLastSignature,
        signatures,
        // TODO remove the `coValue` field from the type
        coValue: storedCoValueRow?.rowID ?? Infinity,
      };
      sessions[sessionID] = updatedSessionRow;
    }
  }
  const updatedCoValueRow = {
    id: msg.id,
    header,
    sessions,
  };
  return {
    updatedCoValueRow,
    newTransactions,
    hasInvalidAssumptions: invalidAssumptions,
  };
}
