import { afterEach, beforeEach, describe, expect, test } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { FjallStorageNapi } from "cojson-core-napi";
import { FjallClient } from "../client.js";
import type { FjallStorageNapiTyped } from "../types.js";
import type { SessionID, RawCoID, StoredSessionRow } from "cojson";
import type { Signature } from "cojson/dist/crypto/crypto.js";
import type { CoValueHeader } from "cojson/dist/coValueCore/verifiedState.js";

let tmpDir: string;
let client: FjallClient;

beforeEach(() => {
  tmpDir = mkdtempSync(join(tmpdir(), "fjall-test-"));
  const napi = new FjallStorageNapi(tmpDir) as unknown as FjallStorageNapiTyped;
  client = new FjallClient(napi);
});

afterEach(() => {
  try {
    rmSync(tmpDir, { recursive: true, force: true });
  } catch {
    // ignore cleanup errors on Windows
  }
});

const testHeader: CoValueHeader = {
  type: "comap",
  ruleset: { type: "unsafeAllowAll" },
  meta: null,
  createdAt: null,
  uniqueness: null,
};

describe("FjallClient", () => {
  // ─── CoValue operations ────────────────────────────────────────

  describe("getCoValue / upsertCoValue", () => {
    test("returns undefined for non-existent CoValue", async () => {
      const result = await client.getCoValue("co_zNonExistent");
      expect(result).toBeUndefined();
    });

    test("upsertCoValue without header returns undefined for missing CoValue", async () => {
      const result = await client.upsertCoValue("co_zMissing");
      expect(result).toBeUndefined();
    });

    test("upserts and retrieves a CoValue with header", async () => {
      const rowID = await client.upsertCoValue("co_zTest1", testHeader);
      expect(rowID).toBeDefined();
      expect(typeof rowID).toBe("number");

      const stored = await client.getCoValue("co_zTest1");
      expect(stored).toBeDefined();
      expect(stored!.id).toBe("co_zTest1");
      expect(stored!.rowID).toBe(rowID);
      expect(stored!.header).toEqual(testHeader);
    });

    test("upsert is idempotent — same rowID on repeat", async () => {
      const rowID1 = await client.upsertCoValue("co_zIdem", testHeader);
      const rowID2 = await client.upsertCoValue("co_zIdem", testHeader);
      expect(rowID1).toBe(rowID2);
    });

    test("upsert without header after creation returns existing rowID", async () => {
      const rowID1 = await client.upsertCoValue("co_zLookup", testHeader);
      const rowID2 = await client.upsertCoValue("co_zLookup");
      expect(rowID2).toBe(rowID1);
    });
  });

  // ─── Session operations ────────────────────────────────────────

  describe("sessions", () => {
    test("returns empty array for CoValue with no sessions", async () => {
      const rowID = await client.upsertCoValue("co_zNoSess", testHeader);
      const sessions = await client.getCoValueSessions(rowID!);
      expect(sessions).toEqual([]);
    });

    test("addSessionUpdate creates and updates a session", async () => {
      const cvRow = await client.upsertCoValue("co_zSess1", testHeader);

      const sesRow = await client.transaction(async (tx) => {
        return tx.addSessionUpdate({
          sessionUpdate: {
            coValue: cvRow!,
            sessionID: "session_z1" as SessionID,
            lastIdx: 5,
            lastSignature: "sig_z1" as Signature,
            bytesSinceLastSignature: 100,
          },
        });
      });

      expect(typeof sesRow).toBe("number");

      const sessions = await client.getCoValueSessions(cvRow!);
      expect(sessions).toHaveLength(1);
      expect(sessions[0]!.sessionID).toBe("session_z1");
      expect(sessions[0]!.lastIdx).toBe(5);
      expect(sessions[0]!.lastSignature).toBe("sig_z1");
      expect(sessions[0]!.bytesSinceLastSignature).toBe(100);
    });

    test("getSingleCoValueSession finds the right session", async () => {
      const cvRow = await client.upsertCoValue("co_zSingle", testHeader);

      await client.transaction(async (tx) => {
        await tx.addSessionUpdate({
          sessionUpdate: {
            coValue: cvRow!,
            sessionID: "session_a" as SessionID,
            lastIdx: 1,
            lastSignature: "sig_a" as Signature,
          },
        });
        await tx.addSessionUpdate({
          sessionUpdate: {
            coValue: cvRow!,
            sessionID: "session_b" as SessionID,
            lastIdx: 2,
            lastSignature: "sig_b" as Signature,
          },
        });
      });

      const result = (await client.transaction(async (tx) => {
        return tx.getSingleCoValueSession(cvRow!, "session_b" as SessionID);
      })) as StoredSessionRow | undefined;
      expect(result).toBeDefined();
      expect(result!.sessionID).toBe("session_b");
      expect(result!.lastIdx).toBe(2);
    });
  });

  // ─── Transaction operations ────────────────────────────────────

  describe("transactions", () => {
    test("stores and retrieves transactions by range", async () => {
      const cvRow = await client.upsertCoValue("co_zTx", testHeader);

      const sesRow = (await client.transaction(async (tx) => {
        return tx.addSessionUpdate({
          sessionUpdate: {
            coValue: cvRow!,
            sessionID: "s1" as SessionID,
            lastIdx: 0,
            lastSignature: "" as Signature,
          },
        });
      })) as number;

      // Insert 5 transactions
      await client.transaction(async (tx) => {
        for (let i = 0; i < 5; i++) {
          await tx.addTransaction(sesRow, i, { changes: { idx: i } } as any);
        }
      });

      const txs = await client.getNewTransactionInSession(sesRow, 1, 3);
      expect(txs).toHaveLength(3);
      expect(txs[0]!.idx).toBe(1);
      expect(txs[2]!.idx).toBe(3);
      expect(txs[0]!.tx).toEqual({ changes: { idx: 1 } });
    });
  });

  // ─── Signature operations ──────────────────────────────────────

  describe("signatures", () => {
    test("stores and retrieves signatures", async () => {
      const cvRow = await client.upsertCoValue("co_zSig", testHeader);

      const sesRow = (await client.transaction(async (tx) => {
        return tx.addSessionUpdate({
          sessionUpdate: {
            coValue: cvRow!,
            sessionID: "s1" as SessionID,
            lastIdx: 0,
            lastSignature: "" as Signature,
          },
        });
      })) as number;

      await client.transaction(async (tx) => {
        await tx.addSignatureAfter({
          sessionRowID: sesRow,
          idx: 5,
          signature: "sig_z5" as Signature,
        });
        await tx.addSignatureAfter({
          sessionRowID: sesRow,
          idx: 10,
          signature: "sig_z10" as Signature,
        });
      });

      const sigs = await client.getSignatures(sesRow, 5);
      expect(sigs).toHaveLength(2);
      expect(sigs[0]!.idx).toBe(5);
      expect(sigs[0]!.signature).toBe("sig_z5");
      expect(sigs[1]!.idx).toBe(10);

      const sigsFrom8 = await client.getSignatures(sesRow, 8);
      expect(sigsFrom8).toHaveLength(1);
      expect(sigsFrom8[0]!.idx).toBe(10);
    });
  });

  // ─── Deletion operations ───────────────────────────────────────

  describe("deletion", () => {
    test("marks a CoValue as deleted and lists it", async () => {
      await client.transaction(async (tx) => {
        await tx.markCoValueAsDeleted("co_zDel1" as RawCoID);
      });

      const waiting = await client.getAllCoValuesWaitingForDelete();
      expect(waiting).toContain("co_zDel1");
    });

    test("eraseCoValueButKeepTombstone preserves delete sessions", async () => {
      const cvRow = await client.upsertCoValue("co_zErase", testHeader);

      // Add a normal session and a delete session
      await client.transaction(async (tx) => {
        await tx.addSessionUpdate({
          sessionUpdate: {
            coValue: cvRow!,
            sessionID: "normal_session" as SessionID,
            lastIdx: 3,
            lastSignature: "sig_n" as Signature,
          },
        });
        await tx.addSessionUpdate({
          sessionUpdate: {
            coValue: cvRow!,
            sessionID: "delete_session$" as SessionID,
            lastIdx: 1,
            lastSignature: "sig_d" as Signature,
          },
        });
        await tx.markCoValueAsDeleted("co_zErase" as RawCoID);
      });

      await client.eraseCoValueButKeepTombstone("co_zErase" as RawCoID);

      // Delete session should be preserved
      const sessions = await client.getCoValueSessions(cvRow!);
      expect(sessions).toHaveLength(1);
      expect(sessions[0]!.sessionID).toMatch(/\$$/);

      // Should no longer be in pending queue
      const waiting = await client.getAllCoValuesWaitingForDelete();
      expect(waiting).not.toContain("co_zErase");
    });
  });

  // ─── Sync tracking ────────────────────────────────────────────

  describe("sync tracking", () => {
    test("tracks unsynced CoValues and resolves them", async () => {
      await client.trackCoValuesSyncState([
        { id: "co_z1" as RawCoID, peerId: "peer_a", synced: false },
        { id: "co_z1" as RawCoID, peerId: "peer_b", synced: false },
        { id: "co_z2" as RawCoID, peerId: "peer_a", synced: false },
      ]);

      let ids = await client.getUnsyncedCoValueIDs();
      expect(ids).toContain("co_z1");
      expect(ids).toContain("co_z2");

      // Sync one peer
      await client.trackCoValuesSyncState([
        { id: "co_z1" as RawCoID, peerId: "peer_a", synced: true },
      ]);

      ids = await client.getUnsyncedCoValueIDs();
      expect(ids).toContain("co_z1"); // peer_b still unsynced

      // Sync the other peer
      await client.trackCoValuesSyncState([
        { id: "co_z1" as RawCoID, peerId: "peer_b", synced: true },
      ]);

      ids = await client.getUnsyncedCoValueIDs();
      expect(ids).not.toContain("co_z1");
      expect(ids).toContain("co_z2");
    });

    test("stopTrackingSyncState removes all entries", async () => {
      await client.trackCoValuesSyncState([
        { id: "co_zStop" as RawCoID, peerId: "p1", synced: false },
        { id: "co_zStop" as RawCoID, peerId: "p2", synced: false },
      ]);

      await client.stopTrackingSyncState("co_zStop" as RawCoID);

      const ids = await client.getUnsyncedCoValueIDs();
      expect(ids).not.toContain("co_zStop");
    });
  });

  // ─── Known state ──────────────────────────────────────────────

  describe("getCoValueKnownState", () => {
    test("returns undefined for non-existent CoValue", async () => {
      const result = await client.getCoValueKnownState("co_zMissing");
      expect(result).toBeUndefined();
    });

    test("returns header + session counters", async () => {
      const cvRow = await client.upsertCoValue("co_zKS", testHeader);

      await client.transaction(async (tx) => {
        await tx.addSessionUpdate({
          sessionUpdate: {
            coValue: cvRow!,
            sessionID: "s1" as SessionID,
            lastIdx: 5,
            lastSignature: "sig" as Signature,
          },
        });
        await tx.addSessionUpdate({
          sessionUpdate: {
            coValue: cvRow!,
            sessionID: "s2" as SessionID,
            lastIdx: 10,
            lastSignature: "sig2" as Signature,
          },
        });
      });

      const ks = await client.getCoValueKnownState("co_zKS");
      expect(ks).toBeDefined();
      expect(ks!.id).toBe("co_zKS");
      expect(ks!.header).toBe(true);
      expect(ks!.sessions["s1" as SessionID]).toBe(5);
      expect(ks!.sessions["s2" as SessionID]).toBe(10);
    });
  });
});
