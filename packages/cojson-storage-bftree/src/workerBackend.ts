import { Keys } from "./keys.js";
import type { BfTreeStore } from "cojson-core-wasm";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/**
 * Maximum number of entries to return from a prefix scan.
 * This is a generous upper bound; most scans will return far fewer.
 */
const SCAN_LIMIT = 100_000;

/**
 * Worker-side implementation of the storage backend.
 *
 * Runs entirely inside a Web Worker. It wraps the WASM BfTreeStore
 * with key encoding, JSON serialisation, and row-ID mapping so that
 * the main-thread `BfTreeClient` proxy can talk in the same terms as
 * `DBClientInterfaceAsync`.
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
  // Dispatch — maps RPC method names to concrete implementations
  // ===================================================================

  dispatch(method: string, args: unknown[]): unknown {
    switch (method) {
      // DBClientInterfaceAsync
      case "getCoValue":
        return this.getCoValue(args[0] as string);
      case "upsertCoValue":
        return this.upsertCoValue(args[0] as string, args[1]);
      case "getCoValueSessions":
        return this.getCoValueSessions(args[0] as number);
      case "getNewTransactionInSession":
        return this.getNewTransactionInSession(
          args[0] as number,
          args[1] as number,
          args[2] as number,
        );
      case "getSignatures":
        return this.getSignatures(args[0] as number, args[1] as number);
      case "getAllCoValuesWaitingForDelete":
        return this.getAllCoValuesWaitingForDelete();
      case "trackCoValuesSyncState":
        return this.trackCoValuesSyncState(
          args[0] as { id: string; peerId: string; synced: boolean }[],
        );
      case "getUnsyncedCoValueIDs":
        return this.getUnsyncedCoValueIDs();
      case "stopTrackingSyncState":
        return this.stopTrackingSyncState(args[0] as string);
      case "eraseCoValueButKeepTombstone":
        return this.eraseCoValueButKeepTombstone(args[0] as string);
      case "getCoValueKnownState":
        return this.getCoValueKnownState(args[0] as string);

      // DBTransactionInterfaceAsync (tx.* prefix)
      case "tx.getSingleCoValueSession":
        return this.getSingleCoValueSession(
          args[0] as number,
          args[1] as string,
        );
      case "tx.markCoValueAsDeleted":
        return this.markCoValueAsDeleted(args[0] as string);
      case "tx.addSessionUpdate":
        return this.addSessionUpdate(
          args[0] as {
            sessionUpdate: {
              coValue: number;
              sessionID: string;
              lastIdx: number;
              lastSignature: string;
              bytesSinceLastSignature?: number;
            };
            sessionRow?: { rowID: number };
          },
        );
      case "tx.addTransaction":
        return this.addTransaction(
          args[0] as number,
          args[1] as number,
          args[2],
        );
      case "tx.addSignatureAfter":
        return this.addSignatureAfter(
          args[0] as { sessionRowID: number; idx: number; signature: string },
        );
      case "tx.deleteCoValueContent":
        return this.deleteCoValueContent(
          args[0] as { rowID: number; id: string },
        );

      default:
        throw new Error(`BfTreeWorkerBackend: unknown method "${method}"`);
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
    // Scan all coValues
    for (const [key] of this.scanByPrefix(Keys.coValuePrefix())) {
      // key = "cv|{coValueId}"
      const coValueId = key.slice(Keys.coValuePrefix().length);
      this.assignCoValueRowId(coValueId);
    }

    // Scan all sessions
    for (const [key] of this.scanByPrefix("se|")) {
      // key = "se|{coValueId}|{sessionID}"
      const rest = key.slice(3); // skip "se|"
      const sepIdx = rest.indexOf("|");
      if (sepIdx === -1) continue;
      const coValueId = rest.slice(0, sepIdx);
      const sessionID = rest.slice(sepIdx + 1);
      this.assignSessionRowId(coValueId, sessionID);
    }
  }

  // ===================================================================
  // DBClientInterfaceAsync — method implementations
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
    const results: {
      rowID: number;
      coValue: number;
      sessionID: string;
      lastIdx: number;
      lastSignature: string;
      bytesSinceLastSignature?: number;
    }[] = [];

    for (const [key, value] of this.scanByPrefix(prefix)) {
      const sessionID = key.slice(prefix.length);
      const data = JSON.parse(value) as {
        lastIdx: number;
        lastSignature: string;
        bytesSinceLastSignature?: number;
      };
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
    const results: { ses: number; idx: number; tx: unknown }[] = [];

    for (let idx = fromIdx; idx <= toIdx; idx++) {
      const tx = this.get(Keys.transaction(coValueId, sessionID, idx));
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
      // 0 = Pending
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
      // key = "us|{coValueId}|{peerId}"
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

    const knownState: {
      id: string;
      header: boolean;
      sessions: Record<string, number>;
    } = {
      id: coValueId,
      header: true,
      sessions: {},
    };

    for (const session of sessions) {
      knownState.sessions[session.sessionID] = session.lastIdx;
    }

    return knownState;
  }

  // ===================================================================
  // DBTransactionInterfaceAsync — method implementations
  // ===================================================================

  getSingleCoValueSession(coValueRowId: number, sessionID: string) {
    const coValueId = this.rowIdToCoValueId.get(coValueRowId);
    if (!coValueId) return undefined;

    const key = Keys.session(coValueId, sessionID);
    const data = this.get<{
      lastIdx: number;
      lastSignature: string;
      bytesSinceLastSignature?: number;
    }>(key);
    if (!data) return undefined;

    return {
      rowID: this.assignSessionRowId(coValueId, sessionID),
      coValue: coValueRowId,
      sessionID,
      ...data,
    };
  }

  markCoValueAsDeleted(id: string) {
    // 0 = DeletedCoValueDeletionStatus.Pending
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

    // Return the session row ID (reuse if updating)
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

    // Get all sessions for this coValue
    const prefix = Keys.sessionPrefix(coValueId);
    const sessionKeys: string[] = [];

    for (const [key, , sessionID] of this.scanByPrefix(prefix).map(
      ([k, v]) => [k, v, k.slice(prefix.length)] as const,
    )) {
      // Keep delete sessions (ending with "$")
      if (sessionID.endsWith("$")) continue;

      // Delete all transactions for this session
      const txPrefix = Keys.transactionPrefix(coValueId, sessionID);
      for (const [txKey] of this.scanByPrefix(txPrefix)) {
        this.del(txKey);
      }

      // Delete all signatures for this session
      const sigPrefix = Keys.signaturePrefix(coValueId, sessionID);
      for (const [sigKey] of this.scanByPrefix(sigPrefix)) {
        this.del(sigKey);
      }

      sessionKeys.push(key);
    }

    // Delete the session entries themselves (non-delete sessions)
    for (const key of sessionKeys) {
      this.del(key);
    }

    // Mark deletion as done (status = 1)
    this.put(Keys.deleted(coValueId), 1);
  }
}
