import type {
  AgentSecret,
  DBClientInterfaceAsync,
  DBTransactionInterfaceAsync,
  RawCoID,
  RawCoMap,
  SessionID,
  SignatureAfterRow,
  StoredCoValueRow,
  StoredSessionRow,
  TransactionRow,
  StorageAPI,
  SyncMessage,
} from "cojson";
import {
  cojsonInternals,
  ControlledAgent,
  LocalNode,
  StorageApiAsync,
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
// Direct adapter: wraps BfTreeWorkerBackend as DBClientInterfaceAsync
// without going through postMessage (for testing).
// ============================================================================

class DirectBfTreeClient implements DBClientInterfaceAsync {
  constructor(private backend: BfTreeWorkerBackend) {}

  getCoValue(coValueId: string) {
    return Promise.resolve(
      this.backend.dispatch("getCoValue", [coValueId]) as
        | StoredCoValueRow
        | undefined,
    );
  }

  upsertCoValue(id: string, header?: unknown) {
    return Promise.resolve(
      this.backend.dispatch("upsertCoValue", [id, header]) as
        | number
        | undefined,
    );
  }

  getCoValueSessions(coValueRowId: number) {
    return Promise.resolve(
      this.backend.dispatch("getCoValueSessions", [
        coValueRowId,
      ]) as StoredSessionRow[],
    );
  }

  getNewTransactionInSession(
    sessionRowId: number,
    fromIdx: number,
    toIdx: number,
  ) {
    return Promise.resolve(
      this.backend.dispatch("getNewTransactionInSession", [
        sessionRowId,
        fromIdx,
        toIdx,
      ]) as TransactionRow[],
    );
  }

  getSignatures(sessionRowId: number, firstNewTxIdx: number) {
    return Promise.resolve(
      this.backend.dispatch("getSignatures", [
        sessionRowId,
        firstNewTxIdx,
      ]) as SignatureAfterRow[],
    );
  }

  getAllCoValuesWaitingForDelete() {
    return Promise.resolve(
      this.backend.dispatch("getAllCoValuesWaitingForDelete", []) as RawCoID[],
    );
  }

  async transaction(
    callback: (tx: DBTransactionInterfaceAsync) => Promise<unknown>,
  ) {
    const txProxy: DBTransactionInterfaceAsync = {
      getSingleCoValueSession: (coValueRowId, sessionID) =>
        Promise.resolve(
          this.backend.dispatch("tx.getSingleCoValueSession", [
            coValueRowId,
            sessionID,
          ]) as StoredSessionRow | undefined,
        ),
      markCoValueAsDeleted: (id) =>
        Promise.resolve(this.backend.dispatch("tx.markCoValueAsDeleted", [id])),
      addSessionUpdate: ({ sessionUpdate, sessionRow }) =>
        Promise.resolve(
          this.backend.dispatch("tx.addSessionUpdate", [
            { sessionUpdate, sessionRow },
          ]) as number,
        ),
      addTransaction: (sessionRowID, idx, newTransaction) =>
        Promise.resolve(
          this.backend.dispatch("tx.addTransaction", [
            sessionRowID,
            idx,
            newTransaction,
          ]),
        ),
      addSignatureAfter: ({ sessionRowID, idx, signature }) =>
        Promise.resolve(
          this.backend.dispatch("tx.addSignatureAfter", [
            { sessionRowID, idx, signature },
          ]),
        ),
      deleteCoValueContent: (coValueRow) =>
        Promise.resolve(
          this.backend.dispatch("tx.deleteCoValueContent", [coValueRow]),
        ),
    };
    return callback(txProxy);
  }

  trackCoValuesSyncState(
    updates: { id: RawCoID; peerId: string; synced: boolean }[],
  ) {
    return Promise.resolve(
      this.backend.dispatch("trackCoValuesSyncState", [
        updates,
      ]) as undefined as void,
    );
  }

  getUnsyncedCoValueIDs() {
    return Promise.resolve(
      this.backend.dispatch("getUnsyncedCoValueIDs", []) as RawCoID[],
    );
  }

  stopTrackingSyncState(id: RawCoID) {
    return Promise.resolve(
      this.backend.dispatch("stopTrackingSyncState", [id]) as undefined as void,
    );
  }

  eraseCoValueButKeepTombstone(coValueID: RawCoID) {
    return Promise.resolve(
      this.backend.dispatch("eraseCoValueButKeepTombstone", [coValueID]),
    );
  }

  getCoValueKnownState(coValueId: string) {
    return Promise.resolve(
      this.backend.dispatch("getCoValueKnownState", [coValueId]) as
        | { id: RawCoID; header: boolean; sessions: Record<string, number> }
        | undefined,
    );
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
 * Wrap an existing backend in a fresh StorageApiAsync.
 * Each call returns a new StorageApiAsync with clean internal state
 * (inMemoryCoValues, storeQueue, etc.), mirroring how IndexedDB tests
 * create a new StorageApiAsync per `getIndexedDBStorage()` call.
 */
export function createStorageFromBackend(
  backend: BfTreeWorkerBackend,
): StorageAPI {
  const client = new DirectBfTreeClient(backend);
  return new StorageApiAsync(client);
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
  // @ts-expect-error - dbClient is private
  return storage.dbClient.getAllCoValuesWaitingForDelete();
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

  const originalLoad = StorageApiAsync.prototype.load;
  const originalStore = StorageApiAsync.prototype.store;

  StorageApiAsync.prototype.load = async function (id, callback, done) {
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
      (msg) => {
        messages.push({ from: "storage", msg });
        callback(msg);
      },
      done,
    );
  };

  StorageApiAsync.prototype.store = async function (data, correctionCallback) {
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
    StorageApiAsync.prototype.load = originalLoad;
    StorageApiAsync.prototype.store = originalStore;
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
