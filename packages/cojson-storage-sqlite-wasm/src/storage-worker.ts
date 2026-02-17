/**
 * Web Worker that runs the complete SQLite WASM storage stack.
 *
 * Initialises `SqliteWasmDriver` → `getSqliteStorageAsync` → `StorageApiAsync`.
 * All storage logic (load, store, migrations, queues, erasure) runs inside
 * this worker so the main thread only communicates at the `StorageAPI` level.
 *
 * The OPFS SyncAccessHandle Pool VFS requires a dedicated Web Worker context;
 * this is why the entire storage layer lives here.
 */

import {
  getSqliteStorageAsync,
  type StorageApiAsync,
  type CojsonInternalTypes,
  type RawCoID,
} from "cojson";

type CoValueKnownState = CojsonInternalTypes.CoValueKnownState;
type NewContentMessage = CojsonInternalTypes.NewContentMessage;
import { SqliteWasmDriver } from "./SqliteWasmDriver.js";

// ---------- message types (shared vocabulary with the main-thread proxy) ------

export type MainToWorkerMessage =
  | { type: "initialize"; id: number; filename: string }
  | { type: "load"; id: number; coValueId: string }
  | { type: "store"; id: number; msg: NewContentMessage }
  | { type: "markDeleteAsValid"; coValueId: string }
  | { type: "enableDeletedCoValuesErasure" }
  | { type: "eraseAllDeletedCoValues"; id: number }
  | { type: "loadKnownState"; id: number; coValueId: string }
  | {
      type: "trackCoValuesSyncState";
      id: number;
      updates: { id: RawCoID; peerId: string; synced: boolean }[];
    }
  | { type: "getUnsyncedCoValueIDs"; id: number }
  | { type: "stopTrackingSyncState"; coValueId: string }
  | { type: "onCoValueUnmounted"; coValueId: string }
  | { type: "close"; id: number };

export type WorkerToMainMessage =
  | { type: "initialized"; id: number; success: boolean; error?: string }
  | { type: "loadContent"; id: number; data: NewContentMessage }
  | { type: "loadDone"; id: number; found: boolean }
  | { type: "storeComplete"; id: number }
  | {
      type: "correctionNeeded";
      id: number;
      knownState: CoValueKnownState;
    }
  | {
      type: "knownStateUpdate";
      coValueId: string;
      knownState: CoValueKnownState;
    }
  | {
      type: "loadKnownStateResult";
      id: number;
      knownState: CoValueKnownState | undefined;
    }
  | { type: "trackCoValuesSyncStateDone"; id: number }
  | { type: "getUnsyncedCoValueIDsResult"; id: number; ids: RawCoID[] }
  | { type: "eraseComplete"; id: number }
  | { type: "closeComplete"; id: number }
  | { type: "error"; id: number; message: string };

// ---------- worker-safe `self` reference -------------------------------------

const workerScope: {
  onmessage: ((event: MessageEvent) => void) | null;
  postMessage(message: WorkerToMainMessage): void;
} = self as never;

// ---------- state ------------------------------------------------------------

let storage: StorageApiAsync;

// ---------- helpers ----------------------------------------------------------

function post(msg: WorkerToMainMessage) {
  workerScope.postMessage(msg);
}

/**
 * Patch `StorageApiAsync.knownStates` so that every update is relayed to the
 * main thread, keeping its mirror in sync.
 */
function patchKnownStateNotifications(storageApi: StorageApiAsync) {
  const ks = storageApi.knownStates;

  const origHandleUpdate = ks.handleUpdate.bind(ks);
  ks.handleUpdate = (id: string, knownState: CoValueKnownState) => {
    origHandleUpdate(id, knownState);
    post({
      type: "knownStateUpdate",
      coValueId: id,
      knownState: {
        id: knownState.id,
        header: knownState.header,
        sessions: { ...knownState.sessions },
      },
    });
  };

  const origSetKnownState = ks.setKnownState.bind(ks);
  ks.setKnownState = (id: string, knownState: CoValueKnownState) => {
    origSetKnownState(id, knownState);
    post({
      type: "knownStateUpdate",
      coValueId: id,
      knownState: {
        id: knownState.id,
        header: knownState.header,
        sessions: { ...knownState.sessions },
      },
    });
  };
}

// ---------- message handler --------------------------------------------------

workerScope.onmessage = async (event: MessageEvent<MainToWorkerMessage>) => {
  const msg = event.data;

  try {
    switch (msg.type) {
      // -- lifecycle ----------------------------------------------------------

      case "initialize": {
        const driver = new SqliteWasmDriver(msg.filename, true);
        const storageApi = (await getSqliteStorageAsync(
          driver,
        )) as StorageApiAsync;
        patchKnownStateNotifications(storageApi);
        storage = storageApi;
        post({ type: "initialized", id: msg.id, success: true });
        break;
      }

      case "close": {
        await storage.close();
        post({ type: "closeComplete", id: msg.id });
        break;
      }

      // -- load / store -------------------------------------------------------

      case "load": {
        storage.load(
          msg.coValueId,
          (data) => post({ type: "loadContent", id: msg.id, data }),
          (found) => post({ type: "loadDone", id: msg.id, found }),
        );
        break;
      }

      case "store": {
        storage.store(msg.msg, (knownState) => {
          // We cannot call back to the main thread synchronously, so we
          // notify it and return an empty correction.  The main thread
          // will compute the real correction and re-send as new stores.
          post({
            type: "correctionNeeded",
            id: msg.id,
            knownState: {
              id: knownState.id,
              header: knownState.header,
              sessions: { ...knownState.sessions },
            },
          });
          return [];
        });
        break;
      }

      // -- known-state --------------------------------------------------------

      case "loadKnownState": {
        storage.loadKnownState(msg.coValueId, (knownState) => {
          post({
            type: "loadKnownStateResult",
            id: msg.id,
            knownState: knownState
              ? {
                  id: knownState.id,
                  header: knownState.header,
                  sessions: { ...knownState.sessions },
                }
              : undefined,
          });
        });
        break;
      }

      // -- deletion -----------------------------------------------------------

      case "markDeleteAsValid": {
        storage.markDeleteAsValid(msg.coValueId as RawCoID);
        break;
      }

      case "enableDeletedCoValuesErasure": {
        storage.enableDeletedCoValuesErasure();
        break;
      }

      case "eraseAllDeletedCoValues": {
        await storage.eraseAllDeletedCoValues();
        post({ type: "eraseComplete", id: msg.id });
        break;
      }

      // -- sync tracking ------------------------------------------------------

      case "trackCoValuesSyncState": {
        storage.trackCoValuesSyncState(msg.updates, () => {
          post({ type: "trackCoValuesSyncStateDone", id: msg.id });
        });
        break;
      }

      case "getUnsyncedCoValueIDs": {
        storage.getUnsyncedCoValueIDs((ids) => {
          post({ type: "getUnsyncedCoValueIDsResult", id: msg.id, ids });
        });
        break;
      }

      case "stopTrackingSyncState": {
        storage.stopTrackingSyncState(msg.coValueId as RawCoID);
        break;
      }

      // -- misc ---------------------------------------------------------------

      case "onCoValueUnmounted": {
        storage.onCoValueUnmounted(msg.coValueId as RawCoID);
        break;
      }

      default: {
        const _exhaustive: never = msg;
        throw new Error(
          `Unknown message type: ${(_exhaustive as { type: string }).type}`,
        );
      }
    }
  } catch (error) {
    const id = "id" in msg ? (msg as { id: number }).id : -1;
    post({
      type: "error",
      id,
      message: error instanceof Error ? error.message : String(error),
    });
  }
};
