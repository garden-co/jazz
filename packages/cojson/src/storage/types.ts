import type {
  CoValueHeader,
  Transaction,
} from "../coValueCore/verifiedState.js";
import { Signature } from "../crypto/crypto.js";
import type { CoValueCore, RawCoID, SessionID } from "../exports.js";
import { NewContentMessage } from "../sync.js";
import type { PeerID } from "../sync.js";
import { CoValueKnownState } from "../knownState.js";
import { StorageStreamingQueue } from "../queue/StorageStreamingQueue.js";

export type CorrectionCallback = (
  correction: CoValueKnownState,
) => NewContentMessage[] | undefined;

/**
 * Deletion work queue status for `deletedCoValues` (SQLite).
 *
 * Stored as an INTEGER in SQLite:
 * - 0 = pending
 * - 1 = done
 */
export enum DeletedCoValueDeletionStatus {
  Pending = 0,
  Done = 1,
}

/**
 * The StorageAPI is the interface that the StorageSync and StorageAsync classes implement.
 *
 * It uses callbacks instead of promises to have no overhead when using the StorageSync and less overhead when using the StorageAsync.
 */
export interface StorageAPI {
  /**
   * Flags that the coValue delete is valid.
   *
   * When the delete tx is stored, the storage will mark the coValue as deleted.
   */
  markDeleteAsValid(id: RawCoID): void;

  /**
   * Enable the background erasure scheduler that drains the `deletedCoValues` work queue.
   * This is intentionally opt-in and should be activated by `LocalNode`.
   */
  enableDeletedCoValuesErasure(): void;

  /**
   * Batch physical deletion for coValues queued in `deletedCoValues` with status `Pending`.
   * Must preserve tombstones (header + delete session(s) + their tx/signatures).
   */
  eraseAllDeletedCoValues(): Promise<void>;

  load(
    id: string,
    // This callback is fired when data is found, might be called multiple times if the content requires streaming (e.g when loading files)
    callback: (data: NewContentMessage) => void,
    done?: (found: boolean) => void,
  ): void;
  store(data: NewContentMessage, handleCorrection: CorrectionCallback): void;

  streamingQueue?: StorageStreamingQueue;

  getKnownState(id: string): CoValueKnownState;

  waitForSync(id: string, coValue: CoValueCore): Promise<void>;

  /**
   * Track multiple sync status updates.
   * Does not guarantee the updates will be applied in order, so only one
   * update per CoValue ID + Peer ID combination should be tracked at a time.
   */
  trackCoValuesSyncState(
    updates: { id: RawCoID; peerId: PeerID; synced: boolean }[],
    done?: () => void,
  ): void;

  /**
   * Get all CoValue IDs that have at least one unsynced peer.
   */
  getUnsyncedCoValueIDs(
    callback: (unsyncedCoValueIDs: RawCoID[]) => void,
  ): void;

  /**
   * Stop tracking sync status for a CoValue (remove all peer entries).
   */
  stopTrackingSyncState(id: RawCoID): void;

  /**
   * Load only the knownState (header presence + session counters) for a CoValue.
   * This is more efficient than load() when we only need to check if a peer needs new content.
   *
   * @param id - The CoValue ID
   * @param callback - Called with the knownState, or undefined if CoValue not found
   */
  loadKnownState(
    id: string,
    callback: (knownState: CoValueKnownState | undefined) => void,
  ): void;

  /**
   * Called when a CoValue is unmounted from memory.
   * Used to clean up the metadata associated with that CoValue.
   */
  onCoValueUnmounted(id: RawCoID): void;

  close(): Promise<unknown> | undefined;
}

export type CoValueRow = {
  id: RawCoID;
  header: CoValueHeader;
};

export type StoredCoValueRow = CoValueRow & { rowID: number };

export type SessionRow = {
  coValue: number;
  sessionID: SessionID;
  lastIdx: number;
  lastSignature: Signature;
  bytesSinceLastSignature?: number;
};

export type StoredSessionRow = SessionRow & { rowID: number };

export type NewSessionRow = SessionRow & {
  signatures: Record<number, Signature>;
};

export type StoredNewSessionRow = StoredSessionRow & {
  signatures: Record<number, Signature>;
};

export type NewCoValueRow = CoValueRow & {
  sessions: Record<SessionID, NewSessionRow>;
};

export type StoredNewCoValueRow = StoredCoValueRow & {
  sessions: Record<SessionID, StoredNewSessionRow>;
};

export type TransactionRow = {
  ses: number;
  idx: number;
  tx: Transaction;
};

export type SignatureAfterRow = {
  ses: number;
  idx: number;
  signature: Signature;
};

export interface DBTransactionInterfaceAsync {
  getSingleCoValueSession(
    coValueRowId: number,
    sessionID: SessionID,
  ): Promise<StoredSessionRow | undefined>;

  /**
   * Persist a "deleted coValue" marker in storage (work queue entry).
   * This is an enqueue signal: implementations should set status to `Pending`.
   * This is expected to be idempotent (safe to call repeatedly).
   */
  markCoValueAsDeleted(id: RawCoID): Promise<unknown>;

  addSessionUpdate({
    sessionUpdate,
    sessionRow,
  }: {
    sessionUpdate: SessionRow;
    sessionRow?: StoredSessionRow;
  }): Promise<number>;

  addTransaction(
    sessionRowID: number,
    idx: number,
    newTransaction: Transaction,
  ): Promise<number> | undefined | unknown;

  addSignatureAfter({
    sessionRowID,
    idx,
    signature,
  }: {
    sessionRowID: number;
    idx: number;
    signature: Signature;
  }): Promise<unknown>;
}

export interface DBClientInterfaceAsync {
  getCoValue(
    coValueId: string,
  ): Promise<StoredCoValueRow | undefined> | undefined;

  getCoValueRow(
    coValueId: string,
  ): Promise<StoredNewCoValueRow | undefined> | undefined;

  upsertCoValue(
    id: string,
    header?: CoValueHeader,
  ): Promise<number | undefined>;

  /**
   * Enumerate all coValue IDs currently pending in the "deleted coValues" work queue.
   */
  getAllCoValuesWaitingForDelete(): Promise<RawCoID[]>;

  getCoValueSessions(coValueRowId: number): Promise<StoredSessionRow[]>;

  getNewTransactionInSession(
    sessionRowId: number,
    fromIdx: number,
    toIdx: number,
  ): Promise<TransactionRow[]>;

  getSignatures(
    sessionRowId: number,
    firstNewTxIdx: number,
  ): Promise<SignatureAfterRow[]>;

  transaction(
    callback: (tx: DBTransactionInterfaceAsync) => Promise<unknown>,
  ): Promise<unknown>;

  trackCoValuesSyncState(
    updates: { id: RawCoID; peerId: PeerID; synced: boolean }[],
  ): Promise<void>;

  getUnsyncedCoValueIDs(): Promise<RawCoID[]>;

  stopTrackingSyncState(id: RawCoID): Promise<void>;

  /**
   * Physical deletion primitive: erase all persisted history for a deleted coValue,
   * while preserving the tombstone (header + delete session(s)).
   * Must run inside a single storage transaction.
   */
  eraseCoValueButKeepTombstone(coValueID: RawCoID): Promise<unknown>;

  /**
   * Get the knownState for a CoValue without loading transactions.
   * Returns undefined if the CoValue doesn't exist.
   */
  getCoValueKnownState(
    coValueId: string,
  ): Promise<CoValueKnownState | undefined>;
}

export interface DBTransactionInterfaceSync {
  getSingleCoValueSession(
    coValueRowId: number,
    sessionID: SessionID,
  ): StoredSessionRow | undefined;

  /**
   * Persist a "deleted coValue" marker in storage (work queue entry).
   * This is an enqueue signal: implementations should set status to `"pending"`.
   * This is expected to be idempotent (safe to call repeatedly).
   */
  markCoValueAsDeleted(id: RawCoID): unknown;

  addSessionUpdate({
    sessionUpdate,
    sessionRow,
  }: {
    sessionUpdate: SessionRow;
    sessionRow?: StoredSessionRow;
  }): number;

  addTransaction(
    sessionRowID: number,
    idx: number,
    newTransaction: Transaction,
  ): number | undefined | unknown;

  addSignatureAfter({
    sessionRowID,
    idx,
    signature,
  }: {
    sessionRowID: number;
    idx: number;
    signature: Signature;
  }): number | undefined | unknown;
}

export interface DBClientInterfaceSync {
  getCoValue(coValueId: string): StoredCoValueRow | undefined;

  upsertCoValue(id: string, header?: CoValueHeader): number | undefined;

  /**
   * Enumerate all coValue IDs currently pending in the "deleted coValues" work queue.
   */
  getAllCoValuesWaitingForDelete(): RawCoID[];

  getCoValueSessions(coValueRowId: number): StoredSessionRow[];

  getNewTransactionInSession(
    sessionRowId: number,
    fromIdx: number,
    toIdx: number,
  ): TransactionRow[];

  getSignatures(
    sessionRowId: number,
    firstNewTxIdx: number,
  ): Pick<SignatureAfterRow, "idx" | "signature">[];

  transaction(callback: (tx: DBTransactionInterfaceSync) => unknown): unknown;

  trackCoValuesSyncState(
    updates: { id: RawCoID; peerId: PeerID; synced: boolean }[],
  ): void;

  getUnsyncedCoValueIDs(): RawCoID[];

  stopTrackingSyncState(id: RawCoID): void;

  /**
   * Physical deletion primitive: erase all persisted history for a deleted coValue,
   * while preserving the tombstone (header + delete session(s)).
   * Must run inside a single storage transaction.
   */
  eraseCoValueButKeepTombstone(coValueID: RawCoID): unknown;

  /**
   * Get the knownState for a CoValue without loading transactions.
   * Returns undefined if the CoValue doesn't exist.
   */
  getCoValueKnownState(coValueId: string): CoValueKnownState | undefined;
}
