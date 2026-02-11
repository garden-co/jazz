import type {
  CojsonInternalTypes,
  CoValueCore,
  CoValueKnownState,
  CorrectionCallback,
  NewContentMessage,
  PeerID,
  RawCoID,
  SessionID,
  StorageAPI,
} from "cojson";
import { StorageKnownState, StoreQueue, getDependedOnCoValues } from "cojson";
import type { WorkerRequest, WorkerResponse } from "./protocol.js";

/**
 * Main-thread proxy implementing StorageAPI.
 *
 * Forwards operations to the BfTree Web Worker via postMessage.
 * Each StorageAPI method is at most 1-2 round-trips, compared to
 * 5-10+ round-trips in the v1 DBClientInterfaceAsync design.
 */
export class BfTreeStorageProxy implements StorageAPI {
  private worker: Worker;
  private nextReqId = 0;
  private pending = new Map<
    number,
    {
      resolve: (value: unknown) => void;
      reject: (error: Error) => void;
      /** For load(): accumulates data callbacks before done */
      onData?: (data: CojsonInternalTypes.NewContentMessage) => void;
      onDone?: (found: boolean) => void;
    }
  >();

  // Main-thread state (mirrors what StorageApiAsync managed)
  private knownStates = new StorageKnownState();
  private storeQueue = new StoreQueue();
  private inMemoryCoValues = new Set<RawCoID>();
  private deletedValues = new Set<RawCoID>();

  private pendingKnownStateLoads = new Map<
    string,
    Promise<CoValueKnownState | undefined>
  >();

  constructor(worker: Worker) {
    this.worker = worker;
    this.worker.onmessage = (event: MessageEvent<WorkerResponse>) => {
      this.handleResponse(event.data);
    };
    this.worker.onerror = (event) => {
      this.rejectAllPending(
        new Error(`Worker error: ${event.message || "unknown"}`),
      );
    };
    this.worker.addEventListener("messageerror", () => {
      this.rejectAllPending(
        new Error("Worker messageerror: failed to deserialize response"),
      );
    });
  }

  /**
   * Reject every in-flight request. Called when the worker crashes or
   * produces an unrecoverable error so callers don't hang forever.
   */
  private rejectAllPending(error: Error): void {
    for (const [reqId, handler] of this.pending) {
      handler.reject(error);
    }
    this.pending.clear();
    // pendingKnownStateLoads chain off `call()` which just rejected,
    // so their .catch() path will fire and clean them up automatically.
  }

  private handleResponse(resp: WorkerResponse) {
    const handler = this.pending.get(resp.reqId);
    if (!handler) return;

    switch (resp.type) {
      case "load:data":
        handler.onData?.(resp.data);
        break;

      case "load:done":
        this.pending.delete(resp.reqId);
        handler.onDone?.(resp.found);
        break;

      case "store:result":
        this.pending.delete(resp.reqId);
        handler.resolve(resp);
        break;

      case "result":
        this.pending.delete(resp.reqId);
        handler.resolve(resp.value);
        break;

      case "error":
        this.pending.delete(resp.reqId);
        handler.reject(new Error(resp.message));
        break;
    }
  }

  /** Send a request and return a Promise for the response */
  private call<T>(msg: WorkerRequest): Promise<T> {
    const reqId = this.nextReqId++;
    const msgWithId = { ...msg, reqId } as WorkerRequest & { reqId: number };

    return new Promise<T>((resolve, reject) => {
      this.pending.set(reqId, {
        resolve: resolve as (v: unknown) => void,
        reject,
      });
      this.worker.postMessage(msgWithId);
    });
  }

  /** Send a fire-and-forget message (no response expected) */
  private send(msg: Record<string, unknown>): void {
    this.worker.postMessage(msg);
  }

  // =========================================================================
  // StorageAPI implementation
  // =========================================================================

  getKnownState(id: string): CoValueKnownState {
    return this.knownStates.getKnownState(id);
  }

  loadKnownState(
    id: string,
    callback: (knownState: CoValueKnownState | undefined) => void,
  ): void {
    const cached = this.knownStates.getCachedKnownState(id);
    if (cached) {
      callback(cached);
      return;
    }

    const pending = this.pendingKnownStateLoads.get(id);
    if (pending) {
      pending.then(callback, () => callback(undefined));
      return;
    }

    const loadPromise = this.call<CoValueKnownState | undefined>({
      reqId: 0, // will be overridden by call()
      method: "loadKnownState",
      id,
    })
      .then((knownState) => {
        if (knownState) {
          this.knownStates.setKnownState(id, knownState);
        }
        return knownState;
      })
      .catch(() => undefined)
      .finally(() => {
        this.pendingKnownStateLoads.delete(id);
      });

    this.pendingKnownStateLoads.set(id, loadPromise);
    loadPromise.then(callback);
  }

  load(
    id: string,
    callback: (data: NewContentMessage) => void,
    done: (found: boolean) => void,
  ): void {
    this.loadWithDependencies(id, callback, done);
  }

  /**
   * Load a CoValue and its dependencies recursively.
   * Dependencies (groups, parent groups) are loaded before the CoValue itself.
   */
  private async loadWithDependencies(
    id: string,
    callback: (data: NewContentMessage) => void,
    done: (found: boolean) => void,
  ) {
    try {
      const reqId = this.nextReqId++;
      const messages: CojsonInternalTypes.NewContentMessage[] = [];

      const loadResult = await new Promise<boolean>((resolve, reject) => {
        this.pending.set(reqId, {
          resolve: () => {},
          reject,
          onData: (data) => {
            messages.push(data);
          },
          onDone: (found) => {
            resolve(found);
          },
        });

        this.worker.postMessage({ reqId, method: "load", id });
      });

      if (!loadResult) {
        done(false);
        return;
      }

      // Process each content message, loading dependencies first
      for (const contentMessage of messages) {
        if (contentMessage.header) {
          const deps = getDependedOnCoValues(
            contentMessage.header,
            contentMessage,
          );

          for (const depId of deps) {
            if (this.inMemoryCoValues.has(depId)) continue;

            await new Promise<void>((resolve) => {
              this.loadWithDependencies(depId, callback, () => resolve());
            });
          }
        }

        callback(contentMessage);
      }

      this.inMemoryCoValues.add(id as RawCoID);

      // Update known state from loaded data
      const knownState = this.knownStates.getKnownState(id);
      knownState.header = true;

      for (const msg of messages) {
        for (const [sessionID, sessionContent] of Object.entries(msg.new)) {
          const lastIdx =
            sessionContent.after + sessionContent.newTransactions.length;
          const currentIdx = knownState.sessions[sessionID as SessionID] ?? 0;
          if (lastIdx > currentIdx) {
            knownState.sessions[sessionID as SessionID] = lastIdx;
          }
        }
      }

      this.knownStates.handleUpdate(id, knownState);

      done(true);
    } catch {
      // Worker crashed or was terminated — signal "not found" so the
      // caller doesn't hang. The SyncManager will retry from peers.
      done(false);
    }
  }

  store(msg: NewContentMessage, correctionCallback: CorrectionCallback): void {
    this.storeQueue.push(msg, correctionCallback);

    this.storeQueue.processQueue(async (data, correctionCallback) => {
      return this.storeSingle(data, correctionCallback);
    });
  }

  /**
   * Store a single message by forwarding to the worker.
   * Handles corrections locally since they need main-thread CoValue state.
   */
  private async storeSingle(
    msg: NewContentMessage,
    correctionCallback: CorrectionCallback,
  ): Promise<boolean> {
    if (this.storeQueue.closed) {
      return false;
    }

    let response: {
      knownState: CoValueKnownState;
      storedCoValueRowID: number | undefined;
    };

    try {
      // Send to worker — 1 round-trip
      response = await this.call<{
        knownState: CoValueKnownState;
        storedCoValueRowID: number | undefined;
      }>({
        reqId: 0, // overridden by call()
        method: "store",
        data: msg,
        deletedCoValues: Array.from(this.deletedValues),
      });
    } catch {
      // Worker crashed — data will be re-synced from peers on next session
      return false;
    }

    const { knownState, storedCoValueRowID } = response;

    this.inMemoryCoValues.add(msg.id);
    this.knownStates.setKnownState(msg.id, knownState);
    this.knownStates.handleUpdate(msg.id, knownState);

    // If storedCoValueRowID is undefined, the store needs correction
    if (!storedCoValueRowID) {
      return this.handleCorrection(knownState, correctionCallback);
    }

    return true;
  }

  private async handleCorrection(
    knownState: CoValueKnownState,
    correctionCallback: CorrectionCallback,
  ): Promise<boolean> {
    const correction = correctionCallback(knownState);

    if (!correction) {
      return false;
    }

    for (const msg of correction) {
      const success = await this.storeSingle(msg, (_knownState) => {
        // Double corrections shouldn't happen
        return undefined;
      });

      if (!success) {
        return false;
      }
    }

    return true;
  }

  markDeleteAsValid(id: RawCoID): void {
    this.deletedValues.add(id);
    this.send({ method: "markDeleteAsValid", id });
  }

  enableDeletedCoValuesErasure(): void {
    this.send({ method: "enableDeletedCoValuesErasure" });
  }

  async eraseAllDeletedCoValues(): Promise<void> {
    await this.call<void>({
      reqId: 0,
      method: "eraseAllDeletedCoValues",
    });
  }

  waitForSync(id: string, coValue: CoValueCore): Promise<void> {
    return this.knownStates.waitForSync(id, coValue);
  }

  trackCoValuesSyncState(
    updates: { id: RawCoID; peerId: PeerID; synced: boolean }[],
    done?: () => void,
  ): void {
    this.send({ method: "trackCoValuesSyncState", updates });
    // Fire-and-forget; call done immediately
    done?.();
  }

  getUnsyncedCoValueIDs(
    callback: (unsyncedCoValueIDs: RawCoID[]) => void,
  ): void {
    this.call<RawCoID[]>({
      reqId: 0,
      method: "getUnsyncedCoValueIDs",
    }).then(callback);
  }

  stopTrackingSyncState(id: RawCoID): void {
    this.send({ method: "stopTrackingSyncState", id });
  }

  onCoValueUnmounted(id: RawCoID): void {
    this.inMemoryCoValues.delete(id);
    this.send({ method: "onCoValueUnmounted", id });
  }

  close(): Promise<unknown> | undefined {
    const queuePromise = this.storeQueue.close();

    const closePromise = this.call<void>({
      reqId: 0,
      method: "close",
    }).then(() => {
      this.worker.terminate();
    });

    return Promise.all([queuePromise, closePromise].filter(Boolean));
  }
}
