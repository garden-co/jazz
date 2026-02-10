/**
 * Typed interface for FjallStorageNapi.
 *
 * napi-rs AsyncTask generates `Promise<unknown>` return types,
 * so we define proper types here matching the Rust NAPI structs.
 */

export interface NapiCoValueResult {
  rowId: number;
  headerJson: string;
}

export interface NapiSessionResult {
  rowId: number;
  coValue: number;
  sessionId: string;
  lastIdx: number;
  lastSignature: string;
  bytesSinceLastSignature: number;
}

export interface NapiTransactionResult {
  ses: number;
  idx: number;
  tx: string;
}

export interface NapiSignatureResult {
  idx: number;
  signature: string;
}

export interface NapiKnownStateResult {
  id: string;
  sessions: Record<string, number>;
}

export interface NapiSyncUpdate {
  id: string;
  peerId: string;
  synced: boolean;
}

/**
 * Properly typed version of FjallStorageNapi.
 * The actual NAPI class returns `Promise<unknown>` for AsyncTask methods.
 */
export interface FjallStorageNapiTyped {
  getCoValue(coValueId: string): Promise<NapiCoValueResult | null>;
  upsertCoValue(
    id: string,
    headerJson?: string | undefined | null,
  ): Promise<number | null>;
  getCoValueSessions(coValueRowId: number): Promise<NapiSessionResult[]>;
  getSingleCoValueSession(
    coValueRowId: number,
    sessionId: string,
  ): Promise<NapiSessionResult | null>;
  addSessionUpdate(
    coValueRowId: number,
    sessionId: string,
    lastIdx: number,
    lastSignature: string,
    bytesSinceLastSignature: number,
  ): Promise<number>;
  getNewTransactionInSession(
    sessionRowId: number,
    fromIdx: number,
    toIdx: number,
  ): Promise<NapiTransactionResult[]>;
  addTransaction(
    sessionRowId: number,
    idx: number,
    txJson: string,
  ): Promise<void>;
  getSignatures(
    sessionRowId: number,
    firstNewTxIdx: number,
  ): Promise<NapiSignatureResult[]>;
  addSignatureAfter(
    sessionRowId: number,
    idx: number,
    signature: string,
  ): Promise<void>;
  markCoValueAsDeleted(coValueId: string): Promise<void>;
  eraseCoValueButKeepTombstone(coValueId: string): Promise<void>;
  getAllCoValuesWaitingForDelete(): Promise<string[]>;
  trackCoValuesSyncState(updates: NapiSyncUpdate[]): Promise<void>;
  getUnsyncedCoValueIds(): Promise<string[]>;
  stopTrackingSyncState(coValueId: string): Promise<void>;
  getCoValueKnownState(coValueId: string): Promise<NapiKnownStateResult | null>;
  close(): void;
}
