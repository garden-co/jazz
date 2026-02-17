/**
 * TypeScript interfaces for the native Rust storage bindings.
 *
 * These types mirror the Rust structures exported via NAPI/WASM bindings.
 * They are used by the NativeClient to interact with the native storage backend.
 */

/**
 * Native CoValue header structure (JavaScript representation).
 */
export interface JsCoValueHeader {
  type: string;
  ruleset: JsRuleset;
  meta?: Record<string, unknown>;
  uniqueness?: null | boolean | string | Record<string, string>;
  createdAt?: string;
}

/**
 * Ruleset type.
 */
export interface JsRuleset {
  type: "unsafeAllowAll" | "ownedByGroup" | "group" | "account";
  group?: string;
}

/**
 * Stored CoValue row from native storage.
 */
export interface JsStoredCoValueRow {
  rowId: number;
  id: string;
  headerJson: string;
}

/**
 * Stored session row from native storage.
 */
export interface JsStoredSessionRow {
  rowId: number;
  covalue: number;
  sessionId: string;
  lastIdx: number;
  lastSignature: string;
  bytesSinceLastSignature?: number;
}

/**
 * Transaction row from native storage.
 */
export interface JsTransactionRow {
  ses: number;
  idx: number;
  txJson: string;
}

/**
 * Signature row from native storage.
 */
export interface JsSignatureRow {
  ses: number;
  idx: number;
  signature: string;
}

/**
 * Sync state update.
 */
export interface JsSyncStateUpdate {
  id: string;
  peerId: string;
  synced: boolean;
}

/**
 * Known state for a CoValue.
 */
export interface JsCoValueKnownState {
  id: string;
  header: boolean;
  sessions: Record<string, number>;
}

/**
 * Native storage interface (NAPI).
 *
 * This matches the Rust `NativeStorage` class exported via NAPI bindings.
 */
export interface NativeStorageNapi {
  // CoValue operations
  getCovalue(coValueId: string): JsStoredCoValueRow | null;
  upsertCovalue(id: string, headerJson: string | null): number | null;
  getCovalueSessions(coValueRowId: number): JsStoredSessionRow[];
  getNewTransactionInSession(
    sessionRowId: number,
    fromIdx: number,
    toIdx: number,
  ): JsTransactionRow[];
  getSignatures(sessionRowId: number, firstNewTxIdx: number): JsSignatureRow[];
  getCovalueKnownState(coValueId: string): JsCoValueKnownState | null;

  // Sync state
  trackCovaluesSyncState(updates: JsSyncStateUpdate[]): void;
  getUnsyncedCovalueIds(): string[];
  stopTrackingSyncState(id: string): void;

  // Deletion
  getAllCovaluesWaitingForDelete(): string[];
  eraseCovalueButKeepTombstone(coValueId: string): void;

  // Transaction operations
  addSession(
    covalueRowId: number,
    sessionId: string,
    lastIdx: number,
    lastSignature: string,
    bytesSinceLastSignature: number | null,
    existingRowId: number | null,
  ): number;
  addTransaction(sessionRowId: number, idx: number, txJson: string): number;
  addSignatureAfter(sessionRowId: number, idx: number, signature: string): void;
  markCovalueAsDeleted(id: string): void;
  getSingleCovalueSession(
    coValueRowId: number,
    sessionId: string,
  ): JsStoredSessionRow | null;

  // Lifecycle
  clear(): void;
}

/**
 * Native storage interface (WASM).
 *
 * Uses JSON strings for all data exchange.
 */
export interface NativeStorageWasm {
  // CoValue operations
  getCovalue(coValueId: string): string | undefined;
  upsertCovalue(id: string, headerJson: string | undefined): string | undefined;
  getCovalueSessions(coValueRowId: number): string;
  getNewTransactionInSession(
    sessionRowId: number,
    fromIdx: number,
    toIdx: number,
  ): string;
  getSignatures(sessionRowId: number, firstNewTxIdx: number): string;
  getCovalueKnownState(coValueId: string): string | undefined;

  // Sync state
  trackCovaluesSyncState(updatesJson: string): void;
  getUnsyncedCovalueIds(): string;
  stopTrackingSyncState(id: string): void;

  // Deletion
  getAllCovaluesWaitingForDelete(): string;
  eraseCovalueButKeepTombstone(coValueId: string): void;

  // Transaction operations
  addSession(
    covalueRowId: number,
    sessionId: string,
    lastIdx: number,
    lastSignature: string,
    bytesSinceLastSignature: number | undefined,
    existingRowId: number | undefined,
  ): number;
  addTransaction(sessionRowId: number, idx: number, txJson: string): number;
  addSignatureAfter(sessionRowId: number, idx: number, signature: string): void;
  markCovalueAsDeleted(id: string): void;
  getSingleCovalueSession(
    coValueRowId: number,
    sessionId: string,
  ): string | undefined;

  // Platform detection
  supportsOpfs(): boolean;
  isInWorker(): boolean;

  // Lifecycle
  clear(): void;
}

/**
 * Union type for both NAPI and WASM storage.
 */
export type NativeStorageDriver = NativeStorageNapi | NativeStorageWasm;
