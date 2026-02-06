import { describe, expect, test, vi, beforeEach } from "vitest";
import { NativeClient } from "../storage/native/client.js";
import type {
  NativeStorageNapi,
  JsStoredCoValueRow,
  JsStoredSessionRow,
  JsTransactionRow,
  JsSignatureRow,
  JsCoValueKnownState,
  JsSyncStateUpdate,
} from "../storage/native/types.js";
import type { RawCoID, SessionID } from "../exports.js";
import type { Signature } from "../crypto/crypto.js";

/**
 * Mock native storage driver for testing.
 *
 * This mock implements the NAPI interface and stores data in memory.
 * It's used to test the NativeClient without requiring actual Rust bindings.
 */
function createMockNativeStorage(): NativeStorageNapi {
  const covalues = new Map<
    string,
    { rowId: number; id: string; headerJson: string }
  >();
  const sessions = new Map<number, Map<string, JsStoredSessionRow>>();
  const transactions = new Map<number, Map<number, JsTransactionRow>>();
  const signatures = new Map<number, Map<number, JsSignatureRow>>();
  const unsyncedCovalues = new Map<string, Set<string>>();
  const deletedCovalues = new Set<string>();
  let rowIdCounter = 0;
  let sessionRowIdCounter = 0;

  return {
    getCovalue(coValueId: string): JsStoredCoValueRow | null {
      return covalues.get(coValueId) || null;
    },

    upsertCovalue(id: string, headerJson: string | null): number | null {
      const existing = covalues.get(id);
      if (existing) {
        return existing.rowId;
      }

      if (!headerJson) {
        return null;
      }

      const rowId = ++rowIdCounter;
      covalues.set(id, { rowId, id, headerJson });
      sessions.set(rowId, new Map());
      return rowId;
    },

    getCovalueSessions(coValueRowId: number): JsStoredSessionRow[] {
      const sessionMap = sessions.get(coValueRowId);
      return sessionMap ? Array.from(sessionMap.values()) : [];
    },

    getNewTransactionInSession(
      sessionRowId: number,
      fromIdx: number,
      toIdx: number,
    ): JsTransactionRow[] {
      const txMap = transactions.get(sessionRowId);
      if (!txMap) return [];

      return Array.from(txMap.values()).filter(
        (tx) => tx.idx >= fromIdx && tx.idx <= toIdx,
      );
    },

    getSignatures(
      sessionRowId: number,
      firstNewTxIdx: number,
    ): JsSignatureRow[] {
      const sigMap = signatures.get(sessionRowId);
      if (!sigMap) return [];

      return Array.from(sigMap.values()).filter(
        (sig) => sig.idx >= firstNewTxIdx,
      );
    },

    getCovalueKnownState(coValueId: string): JsCoValueKnownState | null {
      const cv = covalues.get(coValueId);
      if (!cv) return null;

      const sessionMap = sessions.get(cv.rowId);
      const sessionsObj: Record<string, number> = {};

      if (sessionMap) {
        for (const sess of sessionMap.values()) {
          sessionsObj[sess.sessionId] = sess.lastIdx;
        }
      }

      return {
        id: coValueId,
        header: true,
        sessions: sessionsObj,
      };
    },

    trackCovaluesSyncState(updates: JsSyncStateUpdate[]): void {
      for (const update of updates) {
        if (!unsyncedCovalues.has(update.id)) {
          unsyncedCovalues.set(update.id, new Set());
        }
        const peers = unsyncedCovalues.get(update.id)!;
        if (update.synced) {
          peers.delete(update.peerId);
        } else {
          peers.add(update.peerId);
        }
      }
    },

    getUnsyncedCovalueIds(): string[] {
      return Array.from(unsyncedCovalues.entries())
        .filter(([, peers]) => peers.size > 0)
        .map(([id]) => id);
    },

    stopTrackingSyncState(id: string): void {
      unsyncedCovalues.delete(id);
    },

    getAllCovaluesWaitingForDelete(): string[] {
      return Array.from(deletedCovalues);
    },

    eraseCovalueButKeepTombstone(coValueId: string): void {
      deletedCovalues.delete(coValueId);
      // In real implementation, would also delete transactions but keep header
    },

    addSession(
      covalueRowId: number,
      sessionId: string,
      lastIdx: number,
      lastSignature: string,
      bytesSinceLastSignature: number | null,
      existingRowId: number | null,
    ): number {
      const sessionMap = sessions.get(covalueRowId);
      if (!sessionMap) {
        throw new Error(`CoValue ${covalueRowId} not found`);
      }

      let rowId: number;
      if (existingRowId !== null) {
        rowId = existingRowId;
      } else {
        rowId = ++sessionRowIdCounter;
      }

      sessionMap.set(sessionId, {
        rowId,
        covalue: covalueRowId,
        sessionId,
        lastIdx,
        lastSignature,
        bytesSinceLastSignature: bytesSinceLastSignature ?? undefined,
      });

      if (!transactions.has(rowId)) {
        transactions.set(rowId, new Map());
      }
      if (!signatures.has(rowId)) {
        signatures.set(rowId, new Map());
      }

      return rowId;
    },

    addTransaction(sessionRowId: number, idx: number, txJson: string): number {
      const txMap = transactions.get(sessionRowId);
      if (!txMap) {
        throw new Error(`Session ${sessionRowId} not found`);
      }

      txMap.set(idx, { ses: sessionRowId, idx, txJson });
      return idx;
    },

    addSignatureAfter(
      sessionRowId: number,
      idx: number,
      signature: string,
    ): void {
      const sigMap = signatures.get(sessionRowId);
      if (!sigMap) {
        throw new Error(`Session ${sessionRowId} not found`);
      }

      sigMap.set(idx, { ses: sessionRowId, idx, signature });
    },

    markCovalueAsDeleted(id: string): void {
      deletedCovalues.add(id);
    },

    getSingleCovalueSession(
      coValueRowId: number,
      sessionId: string,
    ): JsStoredSessionRow | null {
      const sessionMap = sessions.get(coValueRowId);
      return sessionMap?.get(sessionId) || null;
    },

    clear(): void {
      covalues.clear();
      sessions.clear();
      transactions.clear();
      signatures.clear();
      unsyncedCovalues.clear();
      deletedCovalues.clear();
      rowIdCounter = 0;
      sessionRowIdCounter = 0;
    },
  };
}

describe("NativeClient", () => {
  let mockStorage: NativeStorageNapi;
  let client: NativeClient;

  beforeEach(() => {
    mockStorage = createMockNativeStorage();
    client = new NativeClient(mockStorage);
  });

  describe("CoValue operations", () => {
    test("should upsert and get a CoValue", () => {
      const id = "co_test123" as RawCoID;
      const header = {
        type: "comap",
        ruleset: { type: "unsafeAllowAll" },
      };

      const rowId = client.upsertCoValue(id, header);
      expect(rowId).toBe(1);

      const retrieved = client.getCoValue(id);
      expect(retrieved).toBeDefined();
      expect(retrieved?.id).toBe(id);
      expect(retrieved?.header.type).toBe("comap");
    });

    test("should return undefined for non-existent CoValue", () => {
      const result = client.getCoValue("co_nonexistent" as RawCoID);
      expect(result).toBeUndefined();
    });

    test("should return existing rowId when upserting without header", () => {
      const id = "co_test123" as RawCoID;
      const header = {
        type: "comap",
        ruleset: { type: "unsafeAllowAll" },
      };

      // First insert
      const rowId1 = client.upsertCoValue(id, header);
      expect(rowId1).toBe(1);

      // Upsert without header returns existing rowId
      const rowId2 = client.upsertCoValue(id, undefined);
      expect(rowId2).toBe(1);
    });
  });

  describe("Session operations", () => {
    test("should add and retrieve sessions", () => {
      const id = "co_test123" as RawCoID;
      const header = {
        type: "comap",
        ruleset: { type: "unsafeAllowAll" },
      };

      const coValueRowId = client.upsertCoValue(id, header)!;

      // Add a session
      const sessionRowId = client.addSessionUpdate({
        sessionUpdate: {
          coValue: coValueRowId,
          sessionID: "session_abc" as SessionID,
          lastIdx: 5,
          lastSignature: "sig_xyz" as Signature,
        },
      });

      expect(sessionRowId).toBeDefined();

      // Get sessions
      const sessions = client.getCoValueSessions(coValueRowId);
      expect(sessions).toHaveLength(1);
      expect(sessions[0]?.sessionID).toBe("session_abc");
      expect(sessions[0]?.lastIdx).toBe(5);
    });

    test("should get single session", () => {
      const id = "co_test123" as RawCoID;
      const header = {
        type: "comap",
        ruleset: { type: "unsafeAllowAll" },
      };

      const coValueRowId = client.upsertCoValue(id, header)!;
      const sessionId = "session_abc" as SessionID;

      client.addSessionUpdate({
        sessionUpdate: {
          coValue: coValueRowId,
          sessionID: sessionId,
          lastIdx: 5,
          lastSignature: "sig_xyz" as Signature,
        },
      });

      const session = client.getSingleCoValueSession(coValueRowId, sessionId);
      expect(session).toBeDefined();
      expect(session?.sessionID).toBe(sessionId);
    });
  });

  describe("Transaction operations", () => {
    test("should add and retrieve transactions", () => {
      const id = "co_test123" as RawCoID;
      const header = {
        type: "comap",
        ruleset: { type: "unsafeAllowAll" },
      };

      const coValueRowId = client.upsertCoValue(id, header)!;
      const sessionRowId = client.addSessionUpdate({
        sessionUpdate: {
          coValue: coValueRowId,
          sessionID: "session_abc" as SessionID,
          lastIdx: 2,
          lastSignature: "sig_xyz" as Signature,
        },
      });

      // Add transactions
      const tx1 = { privacy: "trusting", changes: {}, madeAt: 123 };
      const tx2 = { privacy: "trusting", changes: {}, madeAt: 124 };

      client.addTransaction(sessionRowId, 0, tx1);
      client.addTransaction(sessionRowId, 1, tx2);

      // Get transactions
      const transactions = client.getNewTransactionInSession(
        sessionRowId,
        0,
        1,
      );
      expect(transactions).toHaveLength(2);
    });
  });

  describe("Signature operations", () => {
    test("should add and retrieve signatures", () => {
      const id = "co_test123" as RawCoID;
      const header = {
        type: "comap",
        ruleset: { type: "unsafeAllowAll" },
      };

      const coValueRowId = client.upsertCoValue(id, header)!;
      const sessionRowId = client.addSessionUpdate({
        sessionUpdate: {
          coValue: coValueRowId,
          sessionID: "session_abc" as SessionID,
          lastIdx: 10,
          lastSignature: "sig_xyz" as Signature,
        },
      });

      // Add signature checkpoint
      client.addSignatureAfter({
        sessionRowID: sessionRowId,
        idx: 5,
        signature: "sig_checkpoint" as Signature,
      });

      // Get signatures
      const signatures = client.getSignatures(sessionRowId, 0);
      expect(signatures).toHaveLength(1);
      expect(signatures[0]?.signature).toBe("sig_checkpoint");
    });
  });

  describe("Sync state tracking", () => {
    test("should track sync state", () => {
      const id = "co_test123" as RawCoID;

      client.trackCoValuesSyncState([
        { id, peerId: "peer1" as any, synced: false },
        { id, peerId: "peer2" as any, synced: false },
      ]);

      const unsynced = client.getUnsyncedCoValueIDs();
      expect(unsynced).toContain(id);

      // Mark one as synced
      client.trackCoValuesSyncState([
        { id, peerId: "peer1" as any, synced: true },
      ]);

      // Still unsynced because peer2 is not synced
      expect(client.getUnsyncedCoValueIDs()).toContain(id);

      // Mark remaining as synced
      client.trackCoValuesSyncState([
        { id, peerId: "peer2" as any, synced: true },
      ]);

      // Now should be synced
      expect(client.getUnsyncedCoValueIDs()).not.toContain(id);
    });

    test("should stop tracking sync state", () => {
      const id = "co_test123" as RawCoID;

      client.trackCoValuesSyncState([
        { id, peerId: "peer1" as any, synced: false },
      ]);
      expect(client.getUnsyncedCoValueIDs()).toContain(id);

      client.stopTrackingSyncState(id);
      expect(client.getUnsyncedCoValueIDs()).not.toContain(id);
    });
  });

  describe("Known state", () => {
    test("should get known state for CoValue", () => {
      const id = "co_test123" as RawCoID;
      const header = {
        type: "comap",
        ruleset: { type: "unsafeAllowAll" },
      };

      const coValueRowId = client.upsertCoValue(id, header)!;

      // Add sessions
      client.addSessionUpdate({
        sessionUpdate: {
          coValue: coValueRowId,
          sessionID: "session_abc" as SessionID,
          lastIdx: 5,
          lastSignature: "sig_xyz" as Signature,
        },
      });

      const knownState = client.getCoValueKnownState(id);
      expect(knownState).toBeDefined();
      expect(knownState?.id).toBe(id);
      expect(knownState?.header).toBe(true);
      expect(knownState?.sessions["session_abc"]).toBe(5);
    });

    test("should return undefined for non-existent CoValue", () => {
      const knownState = client.getCoValueKnownState("co_nonexistent");
      expect(knownState).toBeUndefined();
    });
  });

  describe("Deletion operations", () => {
    test("should mark and track deleted CoValues", () => {
      const id = "co_test123" as RawCoID;

      client.markCoValueAsDeleted(id);

      const waiting = client.getAllCoValuesWaitingForDelete();
      expect(waiting).toContain(id);
    });

    test("should erase CoValue but keep tombstone", () => {
      const id = "co_test123" as RawCoID;

      client.markCoValueAsDeleted(id);
      expect(client.getAllCoValuesWaitingForDelete()).toContain(id);

      client.eraseCoValueButKeepTombstone(id);
      expect(client.getAllCoValuesWaitingForDelete()).not.toContain(id);
    });
  });

  describe("Transactions", () => {
    test("should execute transaction callback", () => {
      const callback = vi.fn();
      client.transaction(callback);
      expect(callback).toHaveBeenCalledWith(client);
    });
  });
});
