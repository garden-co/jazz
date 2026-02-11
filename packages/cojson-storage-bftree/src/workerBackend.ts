import {
  type CojsonInternalTypes,
  type RawCoID,
  type SessionID,
  cojsonInternals,
  collectNewTxs,
  getNewTransactionsSize,
} from "cojson";
import type { BfTreeStore } from "cojson-core-wasm";
import { Keys } from "./keys.js";

const {
  createContentMessage,
  exceedsRecommendedSize,
  isDeleteSessionID,
  setSessionCounter,
} = cojsonInternals;

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/**
 * Maximum number of entries to return from a prefix scan.
 * This is a generous upper bound; most scans will return far fewer.
 */
const SCAN_LIMIT = 100_000;

// Row types used internally by the backend
type SessionRowData = {
  lastIdx: number;
  lastSignature: string;
  bytesSinceLastSignature?: number;
};

type InternalSessionRow = SessionRowData & {
  rowID: number;
  coValue: number;
  sessionID: string;
};

/**
 * Worker-side implementation of the storage backend.
 *
 * Runs entirely inside a Web Worker. It wraps the WASM BfTreeStore
 * with key encoding, JSON serialisation, and row-ID mapping.
 *
 * Exposes high-level `storeContent` and `loadContent` methods that
 * perform all DB operations in a single synchronous call, avoiding
 * the per-operation postMessage overhead of the v1 DBClientInterfaceAsync design.
 */
export class BfTreeWorkerBackend {
  private tree: BfTreeStore;

  // Row-ID counters and bidirectional maps
  private rowIdCounter = 0;
  private coValueIdToRowId = new Map<string, number>();
  private rowIdToCoValueId = new Map<number, string>();
  private sessionKeyToRowId = new Map<string, number>();
  private rowIdToSessionKey = new Map<number, string>();

  constructor(tree: BfTreeStore) {
    this.tree = tree;
    this.rebuildRowIdMaps();
  }

  // ===================================================================
  // High-level operations (StorageAPI-level)
  // ===================================================================

  /**
   * Store a NewContentMessage in a single synchronous call.
   *
   * Performs: upsert CoValue + for each session: get/create session,
   * write transactions, write signatures, update session metadata.
   *
   * Returns the resulting CoValueKnownState after the store, and whether
   * the CoValue row was successfully stored (undefined if correction needed).
   */
  storeContent(
    msg: CojsonInternalTypes.NewContentMessage,
    deletedCoValues: Set<string>,
  ): {
    knownState: CojsonInternalTypes.CoValueKnownState;
    storedCoValueRowID: number | undefined;
  } {
    const id = msg.id;

    // 1. Upsert the CoValue
    const storedCoValueRowID = this.upsertCoValue(id, msg.header);

    if (!storedCoValueRowID) {
      // No header and CoValue doesn't exist yet — return empty known state
      return {
        knownState: { id: id as RawCoID, header: false, sessions: {} },
        storedCoValueRowID: undefined,
      };
    }

    const knownState: CojsonInternalTypes.CoValueKnownState = {
      id: id as RawCoID,
      header: true,
      sessions: {},
    };

    // Pre-populate known state with ALL existing sessions in storage
    // (not just the ones in the current message).
    // This is critical for multi-session CoValues where the incoming
    // message only contains a subset of sessions.
    const existingSessions = this.getCoValueSessions(storedCoValueRowID);
    for (const session of existingSessions) {
      knownState.sessions[session.sessionID as SessionID] = session.lastIdx;
    }

    let invalidAssumptions = false;

    // 2. Process each session
    for (const sessionID of Object.keys(msg.new) as SessionID[]) {
      const sessionRow = this.getSingleCoValueSession(
        storedCoValueRowID,
        sessionID,
      );

      // Handle delete markers
      if (
        deletedCoValues.has(id) &&
        isDeleteSessionID(sessionID as SessionID)
      ) {
        this.markCoValueAsDeleted(id);
      }

      if (sessionRow) {
        setSessionCounter(
          knownState.sessions,
          sessionRow.sessionID as SessionID,
          sessionRow.lastIdx,
        );
      }

      const lastIdx = sessionRow?.lastIdx || 0;
      const after = msg.new[sessionID]?.after || 0;

      if (lastIdx < after) {
        // Storage has less data than message assumes — need correction
        invalidAssumptions = true;
      } else {
        // 3. Write new transactions + signatures
        const newLastIdx = this.putNewTxs(
          msg,
          sessionID,
          sessionRow,
          storedCoValueRowID,
        );
        setSessionCounter(knownState.sessions, sessionID, newLastIdx);
      }
    }

    return {
      knownState,
      storedCoValueRowID: invalidAssumptions ? undefined : storedCoValueRowID,
    };
  }

  /**
   * Write new transactions and signatures for a session.
   * Synchronous — all bf-tree operations happen in the worker.
   */
  private putNewTxs(
    msg: CojsonInternalTypes.NewContentMessage,
    sessionID: SessionID,
    sessionRow: InternalSessionRow | undefined,
    storedCoValueRowID: number,
  ): number {
    const sessionEntry = msg.new[sessionID];
    if (!sessionEntry) throw new Error("Session ID not found");

    const newTransactions = sessionEntry.newTransactions || [];
    const lastIdx = sessionRow?.lastIdx || 0;
    const actuallyNewOffset = lastIdx - (sessionEntry.after || 0);
    const actuallyNewTransactions = newTransactions.slice(actuallyNewOffset);

    if (actuallyNewTransactions.length === 0) {
      return lastIdx;
    }

    let bytesSinceLastSignature = sessionRow?.bytesSinceLastSignature || 0;
    const newTransactionsSize = getNewTransactionsSize(actuallyNewTransactions);
    const newLastIdx = lastIdx + actuallyNewTransactions.length;

    let shouldWriteSignature = false;
    if (exceedsRecommendedSize(bytesSinceLastSignature, newTransactionsSize)) {
      shouldWriteSignature = true;
      bytesSinceLastSignature = 0;
    } else {
      bytesSinceLastSignature += newTransactionsSize;
    }

    const sessionUpdate = {
      coValue: storedCoValueRowID,
      sessionID,
      lastIdx: newLastIdx,
      lastSignature: sessionEntry.lastSignature,
      bytesSinceLastSignature,
    };

    const sessionRowID = this.addSessionUpdate({
      sessionUpdate,
      sessionRow,
    });

    if (shouldWriteSignature) {
      this.addSignatureAfter({
        sessionRowID,
        idx: newLastIdx - 1,
        signature: sessionEntry.lastSignature,
      });
    }

    for (let i = 0; i < actuallyNewTransactions.length; i++) {
      this.addTransaction(
        sessionRowID,
        lastIdx + i,
        actuallyNewTransactions[i]!,
      );
    }

    return newLastIdx;
  }

  /**
   * Load all content for a CoValue in a single synchronous call.
   *
   * Assembles complete NewContentMessage objects (including streamed chunks
   * for large CoValues with multiple signatures).
   *
   * Returns the messages, the resulting known state, and whether the CoValue was found.
   */
  loadContent(id: string): {
    messages: CojsonInternalTypes.NewContentMessage[];
    knownState: CojsonInternalTypes.CoValueKnownState | undefined;
    found: boolean;
  } {
    const coValueRow = this.getCoValue(id);

    if (!coValueRow) {
      return { messages: [], knownState: undefined, found: false };
    }

    const allSessions = this.getCoValueSessions(coValueRow.rowID);
    const messages: CojsonInternalTypes.NewContentMessage[] = [];

    // Collect signatures per session
    const signaturesBySession = new Map<
      string,
      { idx: number; signature: string }[]
    >();
    let needsStreaming = false;

    for (const sessionRow of allSessions) {
      const signatures = this.getSignatures(sessionRow.rowID, 0);
      if (signatures.length > 0) {
        needsStreaming = true;
        signaturesBySession.set(sessionRow.sessionID, signatures);
      }
    }

    // Build known state
    const knownState: CojsonInternalTypes.CoValueKnownState = {
      id: coValueRow.id as RawCoID,
      header: true,
      sessions: {},
    };

    for (const sessionRow of allSessions) {
      knownState.sessions[sessionRow.sessionID as SessionID] =
        sessionRow.lastIdx;
    }

    let contentMessage = createContentMessage(
      coValueRow.id as RawCoID,
      coValueRow.header as CojsonInternalTypes.CoValueHeader,
    );

    if (needsStreaming) {
      contentMessage.expectContentUntil = knownState.sessions;
    }

    for (const sessionRow of allSessions) {
      const signatures = [
        ...(signaturesBySession.get(sessionRow.sessionID) || []),
      ];

      let idx = 0;

      const lastSignature = signatures[signatures.length - 1];

      if (lastSignature?.signature !== sessionRow.lastSignature) {
        signatures.push({
          idx: sessionRow.lastIdx,
          signature: sessionRow.lastSignature,
        });
      }

      for (const signature of signatures) {
        const txRows = this.getNewTransactionInSession(
          sessionRow.rowID,
          idx,
          signature.idx,
        );

        collectNewTxs({
          newTxsInSession: txRows,
          contentMessage,
          sessionRow: sessionRow as {
            rowID: number;
            coValue: number;
            sessionID: SessionID;
            lastIdx: number;
            lastSignature: CojsonInternalTypes.Signature;
            bytesSinceLastSignature?: number;
          },
          firstNewTxIdx: idx,
          signature: signature.signature as CojsonInternalTypes.Signature,
        });

        idx = signature.idx + 1;

        if (signatures.length > 1) {
          // Stream: push current chunk, start new message
          messages.push(contentMessage);
          contentMessage = createContentMessage(
            coValueRow.id as RawCoID,
            coValueRow.header as CojsonInternalTypes.CoValueHeader,
          );
        }
      }
    }

    const hasNewContent = Object.keys(contentMessage.new).length > 0;
    if (hasNewContent || !needsStreaming) {
      messages.push(contentMessage);
    }

    return { messages, knownState, found: true };
  }

  /**
   * Erase all CoValues that are waiting for deletion.
   * Cooperative: checks an abort signal between iterations.
   */
  eraseAllDeletedCoValues(signal?: AbortSignal): void {
    const ids = this.getAllCoValuesWaitingForDelete();

    for (const id of ids) {
      if (signal?.aborted) return;
      this.eraseCoValueButKeepTombstone(id);
    }
  }

  // ===================================================================
  // Low-level bf-tree helpers
  // ===================================================================

  private put(key: string, value: unknown): void {
    this.tree.insert(
      encoder.encode(key),
      encoder.encode(JSON.stringify(value)),
    );
  }

  private get<T>(key: string): T | undefined {
    const raw = this.tree.read(encoder.encode(key));
    if (!raw) return undefined;
    return JSON.parse(decoder.decode(raw)) as T;
  }

  private del(key: string): void {
    this.tree.delete(encoder.encode(key));
  }

  /**
   * Prefix scan returning decoded `[key, value]` string pairs.
   */
  private scanByPrefix(prefix: string): [string, string][] {
    const pairs = this.tree.scan(encoder.encode(prefix), SCAN_LIMIT) as [
      Uint8Array,
      Uint8Array,
    ][];
    return pairs.map(([k, v]: [Uint8Array, Uint8Array]) => [
      decoder.decode(k),
      decoder.decode(v),
    ]);
  }

  // ===================================================================
  // Row-ID management
  // ===================================================================

  private assignCoValueRowId(id: string): number {
    let rowId = this.coValueIdToRowId.get(id);
    if (rowId !== undefined) return rowId;
    rowId = ++this.rowIdCounter;
    this.coValueIdToRowId.set(id, rowId);
    this.rowIdToCoValueId.set(rowId, id);
    return rowId;
  }

  private assignSessionRowId(coValueId: string, sessionID: string): number {
    const key = `${coValueId}|${sessionID}`;
    let rowId = this.sessionKeyToRowId.get(key);
    if (rowId !== undefined) return rowId;
    rowId = ++this.rowIdCounter;
    this.sessionKeyToRowId.set(key, rowId);
    this.rowIdToSessionKey.set(rowId, key);
    return rowId;
  }

  private splitSessionKey(key: string): [string, string] {
    const idx = key.indexOf("|");
    return [key.slice(0, idx), key.slice(idx + 1)];
  }

  /**
   * Rebuild row-ID maps from persisted data on startup.
   * Scans `cv|` and `se|` prefixes so that numeric IDs assigned
   * after a page reload are consistent within a session.
   */
  private rebuildRowIdMaps(): void {
    for (const [key] of this.scanByPrefix(Keys.coValuePrefix())) {
      const coValueId = key.slice(Keys.coValuePrefix().length);
      this.assignCoValueRowId(coValueId);
    }

    for (const [key] of this.scanByPrefix("se|")) {
      const rest = key.slice(3);
      const sepIdx = rest.indexOf("|");
      if (sepIdx === -1) continue;
      const coValueId = rest.slice(0, sepIdx);
      const sessionID = rest.slice(sepIdx + 1);
      this.assignSessionRowId(coValueId, sessionID);
    }
  }

  // ===================================================================
  // CoValue CRUD operations
  // ===================================================================

  getCoValue(coValueId: string) {
    const data = this.get<{ id: string; header: unknown }>(
      Keys.coValue(coValueId),
    );
    if (!data) return undefined;
    return { ...data, rowID: this.assignCoValueRowId(coValueId) };
  }

  upsertCoValue(id: string, header?: unknown) {
    const existing = this.get(Keys.coValue(id));
    if (existing) return this.assignCoValueRowId(id);
    if (!header) return undefined;
    this.put(Keys.coValue(id), { id, header });
    return this.assignCoValueRowId(id);
  }

  getCoValueSessions(coValueRowId: number) {
    const coValueId = this.rowIdToCoValueId.get(coValueRowId);
    if (!coValueId) return [];

    const prefix = Keys.sessionPrefix(coValueId);
    const results: InternalSessionRow[] = [];

    for (const [key, value] of this.scanByPrefix(prefix)) {
      const sessionID = key.slice(prefix.length);
      const data = JSON.parse(value) as SessionRowData;
      results.push({
        rowID: this.assignSessionRowId(coValueId, sessionID),
        coValue: coValueRowId,
        sessionID,
        ...data,
      });
    }

    return results;
  }

  getNewTransactionInSession(
    sessionRowId: number,
    fromIdx: number,
    toIdx: number,
  ) {
    const sessionKey = this.rowIdToSessionKey.get(sessionRowId);
    if (!sessionKey) return [];

    const [coValueId, sessionID] = this.splitSessionKey(sessionKey);
    const results: {
      ses: number;
      idx: number;
      tx: CojsonInternalTypes.Transaction;
    }[] = [];

    for (let idx = fromIdx; idx <= toIdx; idx++) {
      const tx = this.get<CojsonInternalTypes.Transaction>(
        Keys.transaction(coValueId, sessionID, idx),
      );
      if (tx !== undefined) {
        results.push({ ses: sessionRowId, idx, tx });
      }
    }

    return results;
  }

  getSignatures(sessionRowId: number, firstNewTxIdx: number) {
    const sessionKey = this.rowIdToSessionKey.get(sessionRowId);
    if (!sessionKey) return [];

    const [coValueId, sessionID] = this.splitSessionKey(sessionKey);
    const prefix = Keys.signaturePrefix(coValueId, sessionID);
    const results: { ses: number; idx: number; signature: string }[] = [];

    for (const [key, value] of this.scanByPrefix(prefix)) {
      const idxStr = key.slice(prefix.length);
      const idx = parseInt(idxStr, 10);
      if (idx >= firstNewTxIdx) {
        results.push({
          ses: sessionRowId,
          idx,
          signature: JSON.parse(value) as string,
        });
      }
    }

    return results;
  }

  getAllCoValuesWaitingForDelete() {
    const prefix = Keys.deletedPrefix();
    const result: string[] = [];

    for (const [key, value] of this.scanByPrefix(prefix)) {
      const status = JSON.parse(value) as number;
      if (status === 0) {
        result.push(key.slice(prefix.length));
      }
    }

    return result;
  }

  trackCoValuesSyncState(
    updates: { id: string; peerId: string; synced: boolean }[],
  ) {
    for (const update of updates) {
      const key = Keys.unsynced(update.id, update.peerId);
      if (update.synced) {
        this.del(key);
      } else {
        this.put(key, "");
      }
    }
  }

  getUnsyncedCoValueIDs() {
    const prefix = Keys.allUnsyncedPrefix();
    const ids = new Set<string>();

    for (const [key] of this.scanByPrefix(prefix)) {
      const rest = key.slice(prefix.length);
      const sepIdx = rest.indexOf("|");
      if (sepIdx !== -1) {
        ids.add(rest.slice(0, sepIdx));
      }
    }

    return Array.from(ids);
  }

  stopTrackingSyncState(id: string) {
    const prefix = Keys.unsyncedPrefix(id);
    for (const [key] of this.scanByPrefix(prefix)) {
      this.del(key);
    }
  }

  eraseCoValueButKeepTombstone(coValueID: string) {
    const coValueRow = this.getCoValue(coValueID);
    if (!coValueRow) return;
    this.deleteCoValueContent({ rowID: coValueRow.rowID, id: coValueID });
  }

  getCoValueKnownState(coValueId: string) {
    const coValueRow = this.getCoValue(coValueId);
    if (!coValueRow) return undefined;

    const sessions = this.getCoValueSessions(coValueRow.rowID);

    const knownState: CojsonInternalTypes.CoValueKnownState = {
      id: coValueId as RawCoID,
      header: true,
      sessions: {},
    };

    for (const session of sessions) {
      knownState.sessions[session.sessionID as SessionID] = session.lastIdx;
    }

    return knownState;
  }

  // ===================================================================
  // Session / Transaction / Signature operations
  // ===================================================================

  getSingleCoValueSession(
    coValueRowId: number,
    sessionID: string,
  ): InternalSessionRow | undefined {
    const coValueId = this.rowIdToCoValueId.get(coValueRowId);
    if (!coValueId) return undefined;

    const key = Keys.session(coValueId, sessionID);
    const data = this.get<SessionRowData>(key);
    if (!data) return undefined;

    return {
      rowID: this.assignSessionRowId(coValueId, sessionID),
      coValue: coValueRowId,
      sessionID,
      ...data,
    };
  }

  markCoValueAsDeleted(id: string) {
    this.put(Keys.deleted(id), 0);
  }

  addSessionUpdate({
    sessionUpdate,
    sessionRow,
  }: {
    sessionUpdate: {
      coValue: number;
      sessionID: string;
      lastIdx: number;
      lastSignature: string;
      bytesSinceLastSignature?: number;
    };
    sessionRow?: { rowID: number };
  }) {
    const coValueId = this.rowIdToCoValueId.get(sessionUpdate.coValue);
    if (!coValueId) {
      throw new Error(
        `addSessionUpdate: no coValue for rowId ${sessionUpdate.coValue}`,
      );
    }

    const key = Keys.session(coValueId, sessionUpdate.sessionID);
    this.put(key, {
      lastIdx: sessionUpdate.lastIdx,
      lastSignature: sessionUpdate.lastSignature,
      bytesSinceLastSignature: sessionUpdate.bytesSinceLastSignature,
    });

    if (sessionRow?.rowID) {
      return sessionRow.rowID;
    }
    return this.assignSessionRowId(coValueId, sessionUpdate.sessionID);
  }

  addTransaction(sessionRowID: number, idx: number, newTransaction: unknown) {
    const sessionKey = this.rowIdToSessionKey.get(sessionRowID);
    if (!sessionKey) {
      throw new Error(`addTransaction: no session for rowId ${sessionRowID}`);
    }

    const [coValueId, sessionID] = this.splitSessionKey(sessionKey);
    this.put(Keys.transaction(coValueId, sessionID, idx), newTransaction);
  }

  addSignatureAfter({
    sessionRowID,
    idx,
    signature,
  }: {
    sessionRowID: number;
    idx: number;
    signature: string;
  }) {
    const sessionKey = this.rowIdToSessionKey.get(sessionRowID);
    if (!sessionKey) {
      throw new Error(
        `addSignatureAfter: no session for rowId ${sessionRowID}`,
      );
    }

    const [coValueId, sessionID] = this.splitSessionKey(sessionKey);
    this.put(Keys.signature(coValueId, sessionID, idx), signature);
  }

  deleteCoValueContent(coValueRow: { rowID: number; id: string }) {
    const coValueId = coValueRow.id;

    const prefix = Keys.sessionPrefix(coValueId);
    const sessionKeys: string[] = [];

    for (const [key, , sessionID] of this.scanByPrefix(prefix).map(
      ([k, v]) => [k, v, k.slice(prefix.length)] as const,
    )) {
      // Keep delete sessions (ending with "$")
      if (sessionID.endsWith("$")) continue;

      const txPrefix = Keys.transactionPrefix(coValueId, sessionID);
      for (const [txKey] of this.scanByPrefix(txPrefix)) {
        this.del(txKey);
      }

      const sigPrefix = Keys.signaturePrefix(coValueId, sessionID);
      for (const [sigKey] of this.scanByPrefix(sigPrefix)) {
        this.del(sigKey);
      }

      sessionKeys.push(key);
    }

    for (const key of sessionKeys) {
      this.del(key);
    }

    this.put(Keys.deleted(coValueId), 1);
  }
}
