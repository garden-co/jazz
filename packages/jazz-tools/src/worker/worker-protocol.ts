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
  /** Optional WASM tracing log level for this worker runtime (default: "warn"). */
  logLevel?: "error" | "warn" | "info" | "debug" | "trace";
}

/** Forward a sync payload from main thread to worker. */
export interface SyncToWorkerMessage {
  type: "sync";
  payload: Uint8Array[];
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
  payload: Uint8Array[];
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

/** Execute a worker-local query and return rows to the main thread. */
export interface QueryMessage {
  type: "query";
  requestId: number;
  queryJson: string;
  sessionJson?: string;
  tier?: "worker" | "edge" | "global";
  optionsJson?: string;
}

/** Start a worker-local subscription and stream deltas back to the main thread. */
export interface SubscribeMessage {
  type: "subscribe";
  subscriptionId: number;
  queryJson: string;
  sessionJson?: string;
  tier?: "worker" | "edge" | "global";
  optionsJson?: string;
}

/** Stop a worker-local subscription. */
export interface UnsubscribeMessage {
  type: "unsubscribe";
  subscriptionId: number;
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
  | DebugSeedLiveSchemaMessage
  | QueryMessage
  | SubscribeMessage
  | UnsubscribeMessage;

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
  payload: (Uint8Array | string)[];
}

/** Forward sync payload(s) to a specific follower peer through leader main thread. */
export interface PeerSyncToMainMessage {
  type: "peer-sync";
  peerId: string;
  term: number;
  payload: Uint8Array[];
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

/** Worker responds with query rows. */
export interface QueryOkMessage {
  type: "query-ok";
  requestId: number;
  rows: unknown[];
}

/** Worker reports a query execution failure. */
export interface QueryErrorMessage {
  type: "query-error";
  requestId: number;
  message: string;
}

/** Worker confirms a subscription is active. */
export interface SubscriptionReadyMessage {
  type: "subscription-ready";
  subscriptionId: number;
}

/** Worker streams a subscription delta. */
export interface SubscriptionDeltaMessage {
  type: "subscription-delta";
  subscriptionId: number;
  delta: unknown;
}

/** Worker reports a subscription setup failure. */
export interface SubscriptionErrorMessage {
  type: "subscription-error";
  subscriptionId: number;
  message: string;
}

export type WorkerToMainMessage =
  | ReadyMessage
  | InitOkMessage
  | SyncToMainMessage
  | PeerSyncToMainMessage
  | ErrorMessage
  | ShutdownOkMessage
  | DebugSchemaStateOkMessage
  | DebugSeedLiveSchemaOkMessage
  | QueryOkMessage
  | QueryErrorMessage
  | SubscriptionReadyMessage
  | SubscriptionDeltaMessage
  | SubscriptionErrorMessage;
