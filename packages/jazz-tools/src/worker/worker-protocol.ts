/**
 * Worker protocol types for main thread ↔ dedicated worker communication.
 *
 * Pure type definitions — no runtime code.
 */

// ============================================================================
// Main Thread → Worker Messages
// ============================================================================

/** Initialize the worker runtime with schema, OPFS, and optional server. */
export interface InitMessage {
  type: "init";
  schemaJson: string;
  appId: string;
  env: string;
  userBranch: string;
  dbName: string;
  clientId: string;
  serverUrl?: string;
  serverPathPrefix?: string;
  jwtToken?: string;
  localAuthMode?: "anonymous" | "demo";
  localAuthToken?: string;
  adminSecret?: string;
}

/** Forward a sync payload from main thread to worker. */
export interface SyncToWorkerMessage {
  type: "sync";
  payload: string[]; // JSON-encoded SyncPayloads
}

export type WorkerLifecycleEvent =
  | "visibility-hidden"
  | "visibility-visible"
  | "pagehide"
  | "freeze"
  | "resume";

/** Forward a best-effort page lifecycle hint to the worker runtime. */
export interface LifecycleHintMessage {
  type: "lifecycle-hint";
  event: WorkerLifecycleEvent;
  sentAtMs: number;
}

/** Open/update a follower peer mapping in the worker runtime. */
export interface PeerOpenMessage {
  type: "peer-open";
  peerId: string;
}

/** Forward sync payload(s) for a follower peer through leader worker runtime. */
export interface PeerSyncToWorkerMessage {
  type: "peer-sync";
  peerId: string;
  term: number;
  payload: string[]; // JSON-encoded SyncPayloads
}

/**
 * Signal peer disconnection.
 *
 * Note: WASM runtime currently has no removeClient binding, so this is best-effort
 * metadata cleanup in JS for now.
 */
export interface PeerCloseMessage {
  type: "peer-close";
  peerId: string;
}

/** Update auth credentials (e.g., token refresh). */
export interface UpdateAuthMessage {
  type: "update-auth";
  jwtToken?: string;
  localAuthMode?: "anonymous" | "demo";
  localAuthToken?: string;
}

/** Request graceful shutdown. */
export interface ShutdownMessage {
  type: "shutdown";
}

/**
 * Simulate a crash: release OPFS handles without flushing snapshot.
 * Used for testing WAL recovery. Worker closes OPFS locks and confirms
 * but does NOT write a clean snapshot — recovery must replay the WAL.
 */
export interface SimulateCrashMessage {
  type: "simulate-crash";
}

/** Request worker-side schema/lens debug state for tests. */
export interface DebugSchemaStateMessage {
  type: "debug-schema-state";
}

/** Seed a historical schema and persist its schema/lens catalogue objects. */
export interface DebugSeedLiveSchemaMessage {
  type: "debug-seed-live-schema";
  schemaJson: string;
}

export type MainToWorkerMessage =
  | InitMessage
  | SyncToWorkerMessage
  | LifecycleHintMessage
  | PeerOpenMessage
  | PeerSyncToWorkerMessage
  | PeerCloseMessage
  | UpdateAuthMessage
  | ShutdownMessage
  | SimulateCrashMessage
  | DebugSchemaStateMessage
  | DebugSeedLiveSchemaMessage;

// ============================================================================
// Worker → Main Thread Messages
// ============================================================================

/** Worker has loaded WASM and is ready to receive init. */
export interface ReadyMessage {
  type: "ready";
}

/** Worker has initialized runtime and is ready for sync. */
export interface InitOkMessage {
  type: "init-ok";
  clientId: string;
}

/** Forward a sync payload from worker to main thread. */
export interface SyncToMainMessage {
  type: "sync";
  payload: string[]; // JSON-encoded SyncPayloads
}

/** Forward sync payload(s) to a specific follower peer through leader main thread. */
export interface PeerSyncToMainMessage {
  type: "peer-sync";
  peerId: string;
  term: number;
  payload: string[]; // JSON-encoded SyncPayloads
}

/** Worker encountered an error. */
export interface ErrorMessage {
  type: "error";
  message: string;
}

/** Worker has completed shutdown (OPFS handles released). */
export interface ShutdownOkMessage {
  type: "shutdown-ok";
}

export interface DebugLensEdgeState {
  sourceHash: string;
  targetHash: string;
}

export interface DebugSchemaState {
  currentSchemaHash: string;
  liveSchemaHashes: string[];
  knownSchemaHashes: string[];
  pendingSchemaHashes: string[];
  lensEdges: DebugLensEdgeState[];
}

/** Worker responds with runtime schema/lens debug state. */
export interface DebugSchemaStateOkMessage {
  type: "debug-schema-state-ok";
  state: DebugSchemaState;
}

/** Worker confirms debug schema seeding completed. */
export interface DebugSeedLiveSchemaOkMessage {
  type: "debug-seed-live-schema-ok";
}

export type WorkerToMainMessage =
  | ReadyMessage
  | InitOkMessage
  | SyncToMainMessage
  | PeerSyncToMainMessage
  | ErrorMessage
  | ShutdownOkMessage
  | DebugSchemaStateOkMessage
  | DebugSeedLiveSchemaOkMessage;
