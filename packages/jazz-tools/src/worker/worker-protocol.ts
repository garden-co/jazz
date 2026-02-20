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
  payload: string; // JSON-encoded SyncPayload
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

export type MainToWorkerMessage =
  | InitMessage
  | SyncToWorkerMessage
  | UpdateAuthMessage
  | ShutdownMessage
  | SimulateCrashMessage;

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

/** Worker encountered an error. */
export interface ErrorMessage {
  type: "error";
  message: string;
}

/** Worker has completed shutdown (OPFS handles released). */
export interface ShutdownOkMessage {
  type: "shutdown-ok";
}

export type WorkerToMainMessage =
  | ReadyMessage
  | InitOkMessage
  | SyncToMainMessage
  | ErrorMessage
  | ShutdownOkMessage;
