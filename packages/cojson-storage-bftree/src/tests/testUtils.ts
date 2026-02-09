import type {
  AgentSecret,
  CoValueCore,
  CoValueKnownState,
  CorrectionCallback,
  NewContentMessage,
  PeerID,
  RawCoID,
  RawCoMap,
  SessionID,
  StorageAPI,
  SyncMessage,
} from "cojson";
import {
  StorageKnownState,
  StoreQueue,
  cojsonInternals,
  ControlledAgent,
  LocalNode,
  getDependedOnCoValues,
} from "cojson";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { initializeSync, create_bftree_memory } from "cojson-core-wasm";
import { BfTreeWorkerBackend } from "../workerBackend.js";
import { onTestFinished } from "vitest";

const { knownStateFromContent } = cojsonInternals;

// ============================================================================
// WASM + BfTree initialisation (runs once per test file)
// ============================================================================

let wasmReady = false;

export function ensureWasm() {
  if (!wasmReady) {
    initializeSync();
    wasmReady = true;
  }
}

// ============================================================================
// Direct StorageAPI proxy: wraps BfTreeWorkerBackend without postMessage.
// Replaces the old DirectBfTreeClient + StorageApiAsync pattern.
// ============================================================================

/**
 * A test-only StorageAPI that calls BfTreeWorkerBackend directly
 * (no Worker, no postMessage). Replicates BfTreeStorageProxy logic
 * but with synchronous backend calls.
 */
export class DirectBfTreeStorageProxy implements StorageAPI {
  backend: BfTreeWorkerBackend;
  private knownStates = new StorageKnownState();
  private storeQueue = new StoreQueue();
  private inMemoryCoValues = new Set<RawCoID>();
  private deletedValues = new Set<RawCoID>();
  private pendingKnownStateLoads = new Map<
    string,
    Promise<CoValueKnownState | undefined>
  >();

  constructor(backend: BfTreeWorkerBackend) {
    this.backend = backend;
  }

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

    const loadPromise = Promise.resolve(this.backend.getCoValueKnownState(id))
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

  private async loadWithDependencies(
    id: string,
    callback: (data: NewContentMessage) => void,
    done: (found: boolean) => void,
  ) {
    const { messages, knownState, found } = this.backend.loadContent(id);

    if (!found) {
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

    if (knownState) {
      this.knownStates.setKnownState(id, knownState);
      this.knownStates.handleUpdate(id, knownState);
    }

    done(true);
  }

  store(msg: NewContentMessage, correctionCallback: CorrectionCallback): void {
    this.storeQueue.push(msg, correctionCallback);

    this.storeQueue.processQueue(async (data, correctionCallback) => {
      return this.storeSingle(data, correctionCallback);
    });
  }

  private async storeSingle(
    msg: NewContentMessage,
    correctionCallback: CorrectionCallback,
  ): Promise<boolean> {
    if (this.storeQueue.closed) {
      return false;
    }

    const { knownState, storedCoValueRowID } = this.backend.storeContent(
      msg,
      this.deletedValues,
    );

    this.inMemoryCoValues.add(msg.id);
    this.knownStates.setKnownState(msg.id, knownState);
    this.knownStates.handleUpdate(msg.id, knownState);

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
  }

  enableDeletedCoValuesErasure(): void {
    // No-op in tests — tests call eraseAllDeletedCoValues directly
  }

  async eraseAllDeletedCoValues(): Promise<void> {
    this.backend.eraseAllDeletedCoValues();
  }

  waitForSync(id: string, coValue: CoValueCore): Promise<void> {
    return this.knownStates.waitForSync(id, coValue);
  }

  trackCoValuesSyncState(
    updates: { id: RawCoID; peerId: PeerID; synced: boolean }[],
    done?: () => void,
  ): void {
    this.backend.trackCoValuesSyncState(updates);
    done?.();
  }

  getUnsyncedCoValueIDs(
    callback: (unsyncedCoValueIDs: RawCoID[]) => void,
  ): void {
    callback(this.backend.getUnsyncedCoValueIDs() as RawCoID[]);
  }

  stopTrackingSyncState(id: RawCoID): void {
    this.backend.stopTrackingSyncState(id);
  }

  onCoValueUnmounted(id: RawCoID): void {
    this.inMemoryCoValues.delete(id);
  }

  close(): Promise<unknown> | undefined {
    return this.storeQueue.close();
  }
}

// ============================================================================
// Factory: in-memory BfTree backed StorageAPI (no Worker, no OPFS)
// ============================================================================

/**
 * Create a fresh in-memory BfTree backend.
 * Multiple StorageAPI instances can share the same backend
 * (like multiple IndexedDB connections to the same database).
 */
export function createBfTreeBackend(): BfTreeWorkerBackend {
  ensureWasm();
  const tree = create_bftree_memory(4 * 1024 * 1024); // 4 MB cache for tests
  return new BfTreeWorkerBackend(tree);
}

/**
 * Wrap an existing backend in a fresh DirectBfTreeStorageProxy.
 * Each call returns a new proxy with clean internal state
 * (knownStates, storeQueue, etc.), mirroring how IndexedDB tests
 * create a new StorageApiAsync per `getIndexedDBStorage()` call.
 */
export function createStorageFromBackend(
  backend: BfTreeWorkerBackend,
): StorageAPI {
  return new DirectBfTreeStorageProxy(backend);
}

/**
 * Convenience: create a standalone in-memory storage (backend + wrapper).
 * Use when the test only uses one node with one storage instance.
 */
export function createInMemoryBfTreeStorage(): StorageAPI {
  return createStorageFromBackend(createBfTreeBackend());
}

// ============================================================================
// Shared test helpers (same patterns as cojson-storage-indexeddb)
// ============================================================================

const Crypto = await WasmCrypto.create();

export function getAgentAndSessionID(
  secret: AgentSecret = Crypto.newRandomAgentSecret(),
): [ControlledAgent, SessionID] {
  const sessionID = Crypto.newRandomSessionID(Crypto.getAgentID(secret));
  return [new ControlledAgent(secret, Crypto), sessionID];
}

export function createTestNode(opts?: { secret?: AgentSecret }) {
  const [admin, session] = getAgentAndSessionID(opts?.secret);
  return new LocalNode(admin.agentSecret, session, Crypto);
}

export function connectToSyncServer(
  client: LocalNode,
  syncServer: LocalNode,
): void {
  const [clientPeer, serverPeer] = cojsonInternals.connectedPeers(
    client.currentSessionID,
    syncServer.currentSessionID,
    {
      peer1role: "client",
      peer2role: "server",
      persistent: true,
    },
  );

  client.syncManager.addPeer(serverPeer);
  syncServer.syncManager.addPeer(clientPeer);
}

export function getAllCoValuesWaitingForDelete(
  storage: StorageAPI,
): Promise<RawCoID[]> {
  // Access the backend directly on our test proxy
  const proxy = storage as DirectBfTreeStorageProxy;
  return Promise.resolve(
    proxy.backend.getAllCoValuesWaitingForDelete() as RawCoID[],
  );
}

export async function getCoValueStoredSessions(
  storage: StorageAPI,
  id: RawCoID,
): Promise<SessionID[]> {
  return new Promise<SessionID[]>((resolve) => {
    storage.load(
      id,
      (content) => {
        if (content.id === id) {
          resolve(
            Object.keys(knownStateFromContent(content).sessions) as SessionID[],
          );
        }
      },
      () => {},
    );
  });
}

export function trackMessages() {
  const messages: {
    from: "client" | "server" | "storage";
    msg: SyncMessage;
  }[] = [];

  const originalLoad = DirectBfTreeStorageProxy.prototype.load;
  const originalStore = DirectBfTreeStorageProxy.prototype.store;

  DirectBfTreeStorageProxy.prototype.load = function (id, callback, done) {
    messages.push({
      from: "client",
      msg: {
        action: "load",
        id: id as RawCoID,
        header: false,
        sessions: {},
      },
    });
    return originalLoad.call(
      this,
      id,
      (msg: NewContentMessage) => {
        messages.push({ from: "storage", msg });
        callback(msg);
      },
      done,
    );
  };

  DirectBfTreeStorageProxy.prototype.store = function (
    data,
    correctionCallback,
  ) {
    messages.push({ from: "client", msg: data });
    return originalStore.call(this, data, (msg) => {
      messages.push({
        from: "storage",
        msg: { action: "known", isCorrection: true, ...msg },
      });
      const correctionMessages = correctionCallback(msg);
      if (correctionMessages) {
        for (const m of correctionMessages) {
          messages.push({ from: "client", msg: m });
        }
      }
      return correctionMessages;
    });
  };

  const restore = () => {
    DirectBfTreeStorageProxy.prototype.load = originalLoad;
    DirectBfTreeStorageProxy.prototype.store = originalStore;
    messages.length = 0;
  };

  const clear = () => {
    messages.length = 0;
  };

  onTestFinished(() => {
    restore();
  });

  return { messages, restore, clear };
}

export function waitFor(
  callback: () => boolean | undefined | Promise<boolean | undefined>,
) {
  return new Promise<void>((resolve, reject) => {
    const checkPassed = async () => {
      try {
        return { ok: await callback(), error: null };
      } catch (error) {
        return { ok: false, error };
      }
    };

    let retries = 0;
    const interval = setInterval(async () => {
      const { ok, error } = await checkPassed();
      if (ok !== false) {
        clearInterval(interval);
        resolve();
      }
      if (++retries > 10) {
        clearInterval(interval);
        reject(error);
      }
    }, 100);
  });
}

export function fillCoMapWithLargeData(map: RawCoMap) {
  const dataSize = 1 * 1024 * 200;
  const chunkSize = 1024;
  const chunks = dataSize / chunkSize;

  const value = btoa(
    new Array(chunkSize).fill("value$").join("").slice(0, chunkSize),
  );

  for (let i = 0; i < chunks; i++) {
    map.set(`key${i}`, value, "trusting");
  }

  return map;
}

export { Crypto };
