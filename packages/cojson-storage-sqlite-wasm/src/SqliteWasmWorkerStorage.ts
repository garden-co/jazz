/**
 * Main-thread `StorageAPI` proxy backed by a Web Worker.
 *
 * The worker runs the full SQLite WASM storage stack (driver + client +
 * `StorageApiAsync`). This proxy only communicates at the `StorageAPI` level,
 * dramatically reducing the number of postMessage round-trips compared to
 * proxying individual SQL queries.
 *
 * Synchronous methods (`getKnownState`, `waitForSync`) are served from a
 * local known-state mirror that the worker keeps up-to-date via
 * `knownStateUpdate` messages.
 */

import type {
  CoValueCore,
  CojsonInternalTypes,
  RawCoID,
  StorageAPI,
  CorrectionCallback,
} from "cojson";
import { emptyKnownState } from "cojson";

type CoValueKnownState = CojsonInternalTypes.CoValueKnownState;
type NewContentMessage = CojsonInternalTypes.NewContentMessage;
import type {
  WorkerToMainMessage,
  MainToWorkerMessage,
} from "./storage-worker.js";

// ---------- known-state mirror -----------------------------------------------

/**
 * Lightweight mirror of the worker's `StorageKnownState`.
 * Keeps track of what the worker has stored so that `getKnownState` and
 * `waitForSync` can be answered synchronously on the main thread.
 */
class KnownStateMirror {
  private states = new Map<string, CoValueKnownState>();
  private waitRequests = new Map<
    string,
    Set<{ knownState: CoValueKnownState; resolve: () => void }>
  >();

  get(id: string): CoValueKnownState {
    const existing = this.states.get(id);
    if (existing) return existing;
    const empty = emptyKnownState(id as RawCoID);
    this.states.set(id, empty);
    return empty;
  }

  getCached(id: string): CoValueKnownState | undefined {
    const ks = this.states.get(id);
    return ks?.header ? ks : undefined;
  }

  set(id: string, knownState: CoValueKnownState) {
    this.states.set(id, knownState);
    this.notifyWaiters(id, knownState);
  }

  delete(id: string) {
    this.states.delete(id);
  }

  waitForSync(id: string, coValue: CoValueCore): Promise<void> {
    const initialKnownState = coValue.knownState();
    if (isInSync(initialKnownState, this.get(id))) {
      return Promise.resolve();
    }

    const waiters = this.waitRequests.get(id) ?? new Set();
    this.waitRequests.set(id, waiters);

    return new Promise<void>((resolve) => {
      const unsubscribe = coValue.subscribe((cv) => {
        req.knownState = cv.knownState();
        this.notifyWaiters(id, this.get(id));
      }, false);

      const handleResolve = () => {
        resolve();
        unsubscribe();
      };

      const req = { knownState: initialKnownState, resolve: handleResolve };
      waiters.add(req);
    });
  }

  private notifyWaiters(id: string, storageKnown: CoValueKnownState) {
    const waiters = this.waitRequests.get(id);
    if (!waiters) return;

    for (const req of waiters) {
      if (isInSync(req.knownState, storageKnown)) {
        req.resolve();
        waiters.delete(req);
      }
    }
  }
}

function isInSync(
  coValueKnown: CoValueKnownState,
  storageKnown: CoValueKnownState,
): boolean {
  if (!storageKnown.header && coValueKnown.header) return false;

  const storageSessions = storageKnown.sessions as Record<string, number>;
  for (const [sessionId, count] of Object.entries(coValueKnown.sessions)) {
    if ((storageSessions[sessionId] ?? 0) !== count) return false;
  }
  return true;
}

// ---------- proxy class ------------------------------------------------------

export class SqliteWasmWorkerStorage implements StorageAPI {
  private worker!: Worker;
  private nextId = 0;
  private readonly knownStates = new KnownStateMirror();

  /** Pending load callbacks keyed by request id. */
  private readonly loadCallbacks = new Map<
    number,
    {
      onContent: (data: NewContentMessage) => void;
      onDone?: (found: boolean) => void;
    }
  >();

  /** Correction callbacks keyed by store request id. */
  private readonly correctionCallbacks = new Map<number, CorrectionCallback>();

  /** Generic promise-based pending requests. */
  private readonly pending = new Map<
    number,
    { resolve: (value: unknown) => void; reject: (error: Error) => void }
  >();

  private readonly filename: string;

  constructor(filename = "jazz-cojson.sqlite3") {
    this.filename = filename;
  }

  // -- lifecycle --------------------------------------------------------------

  /**
   * Spawn the Web Worker and initialise the full storage stack inside it.
   * Must be called before any other method.
   */
  async initialize(): Promise<void> {
    this.worker = new Worker(new URL("./storage-worker.js", import.meta.url), {
      type: "module",
    });
    this.worker.onmessage = this.handleMessage;
    this.worker.onerror = (e) => console.error("Storage worker error:", e);

    const id = this.nextId++;
    await new Promise<void>((resolve, reject) => {
      this.pending.set(id, {
        resolve: () => resolve(),
        reject,
      });
      this.post({ type: "initialize", id, filename: this.filename });
    });
  }

  close(): Promise<unknown> {
    const id = this.nextId++;
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      this.post({ type: "close", id });
    }).finally(() => this.worker.terminate());
  }

  // -- StorageAPI (synchronous, served locally) -------------------------------

  getKnownState(id: string): CoValueKnownState {
    return this.knownStates.get(id);
  }

  waitForSync(id: string, coValue: CoValueCore): Promise<void> {
    return this.knownStates.waitForSync(id, coValue);
  }

  // -- StorageAPI (fire-and-forget) -------------------------------------------

  load(
    id: string,
    callback: (data: NewContentMessage) => void,
    done?: (found: boolean) => void,
  ): void {
    const reqId = this.nextId++;
    this.loadCallbacks.set(reqId, { onContent: callback, onDone: done });
    this.post({ type: "load", id: reqId, coValueId: id });
  }

  store(data: NewContentMessage, handleCorrection: CorrectionCallback): void {
    const reqId = this.nextId++;
    this.correctionCallbacks.set(reqId, handleCorrection);
    this.post({ type: "store", id: reqId, msg: data });
  }

  markDeleteAsValid(id: RawCoID): void {
    this.post({ type: "markDeleteAsValid", coValueId: id });
  }

  enableDeletedCoValuesErasure(): void {
    this.post({ type: "enableDeletedCoValuesErasure" });
  }

  async eraseAllDeletedCoValues(): Promise<void> {
    const id = this.nextId++;
    await new Promise<void>((resolve, reject) => {
      this.pending.set(id, { resolve: () => resolve(), reject });
      this.post({ type: "eraseAllDeletedCoValues", id });
    });
  }

  loadKnownState(
    id: string,
    callback: (knownState: CoValueKnownState | undefined) => void,
  ): void {
    // Fast path: check local cache first
    const cached = this.knownStates.getCached(id);
    if (cached) {
      callback(cached);
      return;
    }

    const reqId = this.nextId++;
    this.pending.set(reqId, {
      resolve: (ks) => callback(ks as CoValueKnownState | undefined),
      reject: () => callback(undefined),
    });
    this.post({ type: "loadKnownState", id: reqId, coValueId: id });
  }

  trackCoValuesSyncState(
    updates: { id: RawCoID; peerId: string; synced: boolean }[],
    done?: () => void,
  ): void {
    const reqId = this.nextId++;
    if (done) {
      this.pending.set(reqId, { resolve: () => done(), reject: () => done() });
    }
    this.post({ type: "trackCoValuesSyncState", id: reqId, updates });
  }

  getUnsyncedCoValueIDs(callback: (ids: RawCoID[]) => void): void {
    const reqId = this.nextId++;
    this.pending.set(reqId, {
      resolve: (ids) => callback(ids as RawCoID[]),
      reject: () => callback([]),
    });
    this.post({ type: "getUnsyncedCoValueIDs", id: reqId });
  }

  stopTrackingSyncState(id: RawCoID): void {
    this.post({ type: "stopTrackingSyncState", coValueId: id });
  }

  onCoValueUnmounted(id: RawCoID): void {
    this.knownStates.delete(id);
    this.post({ type: "onCoValueUnmounted", coValueId: id });
  }

  // -- internal ---------------------------------------------------------------

  private post(msg: MainToWorkerMessage) {
    this.worker.postMessage(msg);
  }

  private handleMessage = (event: MessageEvent<WorkerToMainMessage>) => {
    const msg = event.data;

    switch (msg.type) {
      // -- lifecycle responses ------------------------------------------------

      case "initialized": {
        const p = this.pending.get(msg.id);
        this.pending.delete(msg.id);
        if (msg.success) {
          p?.resolve(undefined);
        } else {
          p?.reject(new Error(msg.error ?? "Worker initialisation failed"));
        }
        break;
      }

      case "closeComplete": {
        const p = this.pending.get(msg.id);
        this.pending.delete(msg.id);
        p?.resolve(undefined);
        break;
      }

      // -- load responses -----------------------------------------------------

      case "loadContent": {
        this.loadCallbacks.get(msg.id)?.onContent(msg.data);
        break;
      }

      case "loadDone": {
        const cbs = this.loadCallbacks.get(msg.id);
        this.loadCallbacks.delete(msg.id);
        cbs?.onDone?.(msg.found);
        break;
      }

      // -- store responses ----------------------------------------------------

      case "correctionNeeded": {
        const cb = this.correctionCallbacks.get(msg.id);
        this.correctionCallbacks.delete(msg.id);
        if (cb) {
          const correctionMsgs = cb(msg.knownState);
          if (correctionMsgs) {
            for (const correctionMsg of correctionMsgs) {
              // Send corrections as new stores (no further correction callback)
              this.store(correctionMsg, () => []);
            }
          }
        }
        break;
      }

      // -- known-state updates ------------------------------------------------

      case "knownStateUpdate": {
        this.knownStates.set(msg.coValueId, msg.knownState);
        break;
      }

      case "loadKnownStateResult": {
        const p = this.pending.get(msg.id);
        this.pending.delete(msg.id);
        if (msg.knownState) {
          this.knownStates.set(msg.knownState.id, msg.knownState);
        }
        p?.resolve(msg.knownState);
        break;
      }

      // -- sync tracking responses --------------------------------------------

      case "trackCoValuesSyncStateDone": {
        const p = this.pending.get(msg.id);
        this.pending.delete(msg.id);
        p?.resolve(undefined);
        break;
      }

      case "getUnsyncedCoValueIDsResult": {
        const p = this.pending.get(msg.id);
        this.pending.delete(msg.id);
        p?.resolve(msg.ids);
        break;
      }

      // -- deletion responses -------------------------------------------------

      case "eraseComplete": {
        const p = this.pending.get(msg.id);
        this.pending.delete(msg.id);
        p?.resolve(undefined);
        break;
      }

      // -- errors -------------------------------------------------------------

      case "error": {
        const p = this.pending.get(msg.id);
        this.pending.delete(msg.id);
        p?.reject(new Error(msg.message));

        // Also clean up load / correction maps
        this.loadCallbacks.delete(msg.id);
        this.correctionCallbacks.delete(msg.id);
        break;
      }
    }
  };
}
