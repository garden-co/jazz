import type { CojsonInternalTypes } from "cojson";

// ===================================================================
// Initialization messages
// ===================================================================

/** Initialization message sent once on startup */
export type WorkerInitRequest = {
  type: "init";
  dbName: string;
  cacheSizeBytes: number;
};

/** Initialization response */
export type WorkerInitResponse =
  | { type: "ready" }
  | { type: "error"; message: string };

// ===================================================================
// StorageAPI-level request messages (main → worker)
// ===================================================================

/** Request/response messages — each maps to a StorageAPI-level operation. */
export type WorkerRequest =
  | { reqId: number; method: "load"; id: string }
  | {
      reqId: number;
      method: "store";
      data: CojsonInternalTypes.NewContentMessage;
      deletedCoValues: string[];
    }
  | { reqId: number; method: "loadKnownState"; id: string }
  | { reqId: number; method: "eraseAllDeletedCoValues" }
  | { reqId: number; method: "getUnsyncedCoValueIDs" }
  | { reqId: number; method: "close" };

/** Fire-and-forget messages (no response expected) */
export type WorkerFireAndForget =
  | { method: "markDeleteAsValid"; id: string }
  | { method: "enableDeletedCoValuesErasure" }
  | {
      method: "trackCoValuesSyncState";
      updates: { id: string; peerId: string; synced: boolean }[];
    }
  | { method: "stopTrackingSyncState"; id: string }
  | { method: "onCoValueUnmounted"; id: string };

// ===================================================================
// Response messages (worker → main)
// ===================================================================

/**
 * Worker responses.
 *
 * For load(): the worker sends one or more "load:data" messages followed by a "load:done".
 * For store(): the worker sends a single "store:result" with the resulting knownState.
 * For other request/response methods: a single "result" message.
 */
export type WorkerResponse =
  | {
      reqId: number;
      type: "load:data";
      data: CojsonInternalTypes.NewContentMessage;
    }
  | { reqId: number; type: "load:done"; found: boolean }
  | {
      reqId: number;
      type: "store:result";
      knownState: CojsonInternalTypes.CoValueKnownState;
      storedCoValueRowID: number | undefined;
    }
  | { reqId: number; type: "result"; value: unknown }
  | { reqId: number; type: "error"; message: string };

/** All message types the worker can receive */
export type WorkerIncoming =
  | WorkerInitRequest
  | WorkerRequest
  | WorkerFireAndForget;
