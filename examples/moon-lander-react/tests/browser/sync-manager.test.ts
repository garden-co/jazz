/**
 * Unit tests for Moon Lander — SyncManager.
 *
 * Tests the SyncManager class which owns all DB write state:
 *   - Immediate writes (collectDeposit, refuel, shareFuel, burstDeposit, sendMessage)
 *   - Player state sync (insert on first settle, update on state change)
 *   - Deposit reconciliation (once on settle)
 *   - Release of stale deposits on restart
 *
 * Writes are fire-and-forget. Tests use flushPromises() to let pending
 * async operations settle before asserting.
 */

import { describe, it, expect, vi } from "vitest";
import type { PlayerInit, FuelDeposit } from "../../schema/app.js";
import type { FuelType } from "../../src/game/constants.js";
import { SyncManager, DEPOSITS_PER_TYPE, type SyncInputs } from "../../src/jazz/SyncManager.js";
import { FUEL_TYPES } from "../../src/game/constants.js";

/** Flush all pending microtasks and macrotasks queued before this call. */
function flushPromises(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 0));
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function mockDb() {
  const syncUpdates: Array<{
    table: unknown;
    id: string;
    data: Record<string, unknown>;
  }> = [];
  const inserts: Array<{
    table: unknown;
    data: Record<string, unknown>;
    options: { tier?: string } | undefined;
  }> = [];
  const syncInserts: Array<{
    table: unknown;
    data: Record<string, unknown>;
  }> = [];
  const updates: Array<{
    table: unknown;
    id: string;
    data: Record<string, unknown>;
    options: { tier?: string } | undefined;
  }> = [];
  const deletes: Array<{
    table: unknown;
    id: string;
    options: { tier?: string } | undefined;
  }> = [];
  const durableDeletes: Array<{
    table: unknown;
    id: string;
    options: { tier?: string } | undefined;
  }> = [];

  return {
    db: {
      update: vi.fn((table: unknown, id: string, data: Record<string, unknown>) => {
        syncUpdates.push({ table, id, data });
      }),
      insert: vi.fn((table: unknown, data: Record<string, unknown>) => {
        const id = `sync-${syncInserts.length}`;
        syncInserts.push({ table, data });
        return { id, ...data };
      }),
      insertDurable: vi.fn(
        async (table: unknown, data: Record<string, unknown>, options?: { tier?: string }) => {
          const id = `new-${inserts.length}`;
          inserts.push({ table, data, options });
          return { id, ...data };
        },
      ),
      updateDurable: vi.fn(
        async (
          table: unknown,
          id: string,
          data: Record<string, unknown>,
          options?: { tier?: string },
        ) => {
          updates.push({ table, id, data, options });
        },
      ),
      delete: vi.fn((table: unknown, id: string, options?: { tier?: string }) => {
        deletes.push({ table, id, options });
      }),
      deleteDurable: vi.fn(async (table: unknown, id: string, options?: { tier?: string }) => {
        durableDeletes.push({ table, id, options });
      }),
    } as any,
    inserts,
    syncInserts,
    syncUpdates,
    updates,
    deletes,
    durableDeletes,
  };
}

function makePlayer(overrides: Partial<PlayerInit> = {}): PlayerInit {
  return {
    playerId: "alice-uuid",
    name: "Alice",
    color: "#ff0000",
    mode: "walking",
    online: true,
    lastSeen: 1000,
    positionX: 500,
    positionY: 400,
    velocityX: 0,
    velocityY: 0,
    requiredFuelType: "circle",
    landerFuelLevel: 40,
    landerSpawnX: 480,
    thrusting: false,
    ...overrides,
  };
}

function makeDeposit(overrides: Partial<FuelDeposit> & { fuelType: string }): FuelDeposit {
  return {
    id: `dep-${overrides.fuelType}-${Math.random().toString(36).slice(2, 6)}`,
    positionX: 500,
    createdAt: 1000,
    collected: false,
    collectedBy: "",
    ...overrides,
  };
}

function emptyInputs(): SyncInputs {
  return {
    settled: false,
    localPlayerSettled: false,
    uncollectedDeposits: [],
    myCollectedDeposits: [],
    allDepositsRaw: [],
    localPlayerRows: [],
    perTypeLimits: FUEL_TYPES.map(() => DEPOSITS_PER_TYPE),
    perTypeCounts: FUEL_TYPES.map(() => 0),
    myCollectedCount: 0,
    debugTotalDeposits: 0,
  };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("SyncManager", () => {
  // =========================================================================
  // 1. Lifecycle
  // =========================================================================

  describe("lifecycle", () => {
    it("constructs without errors and destroy is a no-op", () => {
      const { db } = mockDb();
      const sync = new SyncManager(db, "alice");
      sync.destroy();
    });
  });

  // =========================================================================
  // 2. Immediate writes
  //
  //   Each event method fires a DB call immediately (fire-and-forget).
  //   flushPromises() lets the async operations settle.
  // =========================================================================

  describe("immediate writes", () => {
    it("writes deposit collection immediately", async () => {
      const { db, syncUpdates } = mockDb();
      const sync = new SyncManager(db, "alice");

      // collectDeposit looks up the deposit in allDepositsRaw; provide both.
      const dep1 = makeDeposit({ fuelType: "circle", id: "dep-1", positionX: 100 });
      const dep2 = makeDeposit({ fuelType: "triangle", id: "dep-2", positionX: 200 });
      sync.setInputs({
        ...emptyInputs(),
        settled: true,
        allDepositsRaw: [dep1, dep2],
        uncollectedDeposits: [dep1, dep2],
      });

      sync.collectDeposit("dep-1");
      sync.collectDeposit("dep-2");

      await flushPromises();

      // Uses sync db.update so the bridge outbox emits immediately cross-client.
      const collectionUpdates = syncUpdates.filter((u) => u.data.collected === true);
      expect(collectionUpdates).toHaveLength(2);
      expect(collectionUpdates.map((u) => u.id)).toEqual(["dep-1", "dep-2"]);
      expect(collectionUpdates[0].data.collectedBy).toBe("alice");

      sync.destroy();
    });

    it("refuel releases a collected deposit immediately", async () => {
      const { db, syncInserts, deletes } = mockDb();
      const sync = new SyncManager(db, "alice");

      const collectedDep = makeDeposit({
        fuelType: "circle",
        id: "dep-circle-1",
        positionX: 300,
        collected: true,
        collectedBy: "alice",
      });

      sync.setInputs({
        ...emptyInputs(),
        settled: true,
        allDepositsRaw: [collectedDep],
      });

      sync.refuel("circle" as FuelType);

      await flushPromises();

      // Uses sync delete+insert so local subscription updates immediately.
      const deleteCall = deletes.find((d) => d.id === "dep-circle-1");
      expect(deleteCall).toBeTruthy();

      const releaseInsert = syncInserts.find(
        (i) =>
          i.data.fuelType === "circle" && i.data.positionX === 300 && i.data.collected === false,
      );
      expect(releaseInsert).toBeTruthy();
      expect(releaseInsert!.data.collectedBy).toBe("");

      sync.destroy();
    });

    it("burstDeposit releases a collected deposit immediately", async () => {
      const { db, syncInserts, deletes } = mockDb();
      const sync = new SyncManager(db, "alice");

      const collectedDep = makeDeposit({
        fuelType: "triangle",
        id: "dep-tri-1",
        positionX: 400,
        collected: true,
        collectedBy: "alice",
      });

      sync.setInputs({
        ...emptyInputs(),
        settled: true,
        allDepositsRaw: [collectedDep],
      });

      sync.burstDeposit("triangle");

      await flushPromises();

      const deleteCall = deletes.find((d) => d.id === "dep-tri-1");
      expect(deleteCall).toBeTruthy();

      const releaseInsert = syncInserts.find(
        (i) =>
          i.data.fuelType === "triangle" && i.data.positionX === 400 && i.data.collected === false,
      );
      expect(releaseInsert).toBeTruthy();

      sync.destroy();
    });

    it("shareFuel rewrites collectedBy immediately", async () => {
      const { db, syncUpdates } = mockDb();
      const sync = new SyncManager(db, "alice");

      const collectedDep = makeDeposit({
        fuelType: "hexagon",
        id: "dep-hex-1",
        collected: true,
        collectedBy: "alice",
      });

      sync.setInputs({
        ...emptyInputs(),
        settled: true,
        allDepositsRaw: [collectedDep],
      });

      sync.shareFuel("hexagon", "bob-uuid");

      await flushPromises();

      const shareUpdate = syncUpdates.find((u) => u.id === "dep-hex-1");
      expect(shareUpdate).toBeTruthy();
      expect(shareUpdate!.data.collectedBy).toBe("bob-uuid");

      sync.destroy();
    });

    it("sendMessage inserts immediately", async () => {
      const { db, inserts } = mockDb();
      const sync = new SyncManager(db, "alice");
      sync.setInputs({ ...emptyInputs(), settled: true });

      sync.sendMessage("hello moon");
      sync.sendMessage("need hexagon!");

      await flushPromises();

      const chatInserts = inserts.filter((i) => (i.data as any).message !== undefined);
      expect(chatInserts).toHaveLength(2);
      expect(chatInserts[0].data.message).toBe("hello moon");
      expect(chatInserts[0].data.playerId).toBe("alice");
      expect(chatInserts[1].data.message).toBe("need hexagon!");

      sync.destroy();
    });

    it("each event fires exactly one write (no double processing)", async () => {
      const { db, syncUpdates } = mockDb();
      const sync = new SyncManager(db, "alice");

      const dep = makeDeposit({ fuelType: "circle", id: "dep-1" });
      sync.setInputs({
        ...emptyInputs(),
        settled: true,
        allDepositsRaw: [dep],
        uncollectedDeposits: [dep],
      });

      sync.collectDeposit("dep-1");

      await flushPromises();
      expect(syncUpdates.filter((u) => u.id === "dep-1")).toHaveLength(1);

      // No further writes without another event
      await flushPromises();
      expect(syncUpdates.filter((u) => u.id === "dep-1")).toHaveLength(1);

      sync.destroy();
    });
  });

  // =========================================================================
  // 3. Player state sync
  //
  //   setInputs with known row → writes immediately when dbRowId first resolves
  //   settled + no row → insert
  //   No meaningful change → no write
  // =========================================================================

  describe("player state sync", () => {
    it("inserts a new player row when settled with no existing rows", async () => {
      const { db, inserts } = mockDb();
      const sync = new SyncManager(db, "alice");

      const state = makePlayer();
      sync.updateState(state);
      sync.setInputs({
        ...emptyInputs(),
        settled: true,
        localPlayerRows: [],
      });

      await flushPromises();

      const playerInsert = inserts.find((i) => (i.data as any).playerId === "alice-uuid");
      expect(playerInsert).toBeTruthy();

      sync.destroy();
    });

    it("updates an existing player row when dbRowId resolves", async () => {
      const { db, updates } = mockDb();
      const sync = new SyncManager(db, "alice");

      const state = makePlayer();
      sync.updateState(state);
      sync.setInputs({
        ...emptyInputs(),
        settled: true,
        localPlayerRows: [{ id: "existing-row-1" }],
      });

      await flushPromises();

      const playerUpdate = updates.find((u) => u.id === "existing-row-1");
      expect(playerUpdate).toBeTruthy();

      sync.destroy();
    });

    it("skips update when state has not changed meaningfully", async () => {
      const { db, updates } = mockDb();
      const sync = new SyncManager(db, "alice");

      const state = makePlayer();
      sync.updateState(state);
      sync.setInputs({
        ...emptyInputs(),
        settled: true,
        localPlayerRows: [{ id: "existing-row-1" }],
      });

      await flushPromises();
      const firstCount = updates.length;

      // Call setInputs again with same state — no new write
      sync.setInputs({
        ...emptyInputs(),
        settled: true,
        localPlayerRows: [{ id: "existing-row-1" }],
      });
      await flushPromises();
      expect(updates.length).toBe(firstCount);

      sync.destroy();
    });

    it("does not insert before settled", async () => {
      const { db, inserts } = mockDb();
      const sync = new SyncManager(db, "alice");

      const state = makePlayer();
      sync.updateState(state);
      sync.setInputs({
        ...emptyInputs(),
        settled: false,
        localPlayerRows: [],
      });

      await flushPromises();

      const playerInsert = inserts.find((i) => (i.data as any).playerId === "alice-uuid");
      expect(playerInsert).toBeUndefined();

      sync.destroy();
    });
  });

  // =========================================================================
  // 4. Deposit reconciliation
  //
  //   Fires exactly once when settled becomes true.
  // =========================================================================

  describe("deposit reconciliation", () => {
    it("reconciles deposits once when settled", async () => {
      const { db, inserts } = mockDb();
      const sync = new SyncManager(db, "alice");

      sync.setInputs({
        ...emptyInputs(),
        settled: true,
        uncollectedDeposits: [],
        perTypeLimits: FUEL_TYPES.map(() => DEPOSITS_PER_TYPE),
      });

      await flushPromises();

      const expectedInserts = FUEL_TYPES.length * DEPOSITS_PER_TYPE;
      const depositInserts = inserts.filter((i) => (i.data as any).fuelType !== undefined);
      expect(depositInserts.length).toBe(expectedInserts);

      // Second setInputs with settled — should NOT reconcile again
      const countAfterFirst = inserts.length;
      sync.setInputs({ ...emptyInputs(), settled: true, uncollectedDeposits: [] });
      await flushPromises();
      expect(inserts.length).toBe(countAfterFirst);

      sync.destroy();
    });

    it("does not reconcile when not settled", async () => {
      const { db, inserts } = mockDb();
      const sync = new SyncManager(db, "alice");

      sync.setInputs({ ...emptyInputs(), settled: false });

      await flushPromises();

      const depositInserts = inserts.filter((i) => (i.data as any).fuelType !== undefined);
      expect(depositInserts).toHaveLength(0);

      sync.destroy();
    });
  });

  // =========================================================================
  // 5. Stale deposit release on restart
  //
  //   When player mode is "start" or "descending", any deposits
  //   still marked as collected by this player should be released.
  // =========================================================================

  describe("stale deposit release", () => {
    it("releases deposits collected by this player when mode is start", async () => {
      const { db, updates } = mockDb();
      const sync = new SyncManager(db, "alice");

      const staleDep = makeDeposit({
        fuelType: "circle",
        id: "stale-1",
        collected: true,
        collectedBy: "alice",
      });

      sync.updateState(makePlayer({ mode: "start" }));
      sync.setInputs({
        ...emptyInputs(),
        settled: true,
        allDepositsRaw: [staleDep],
        localPlayerRows: [{ id: "row-1" }],
      });

      await flushPromises();

      const releaseUpdate = updates.find((u) => u.id === "stale-1");
      expect(releaseUpdate).toBeTruthy();
      expect(releaseUpdate!.data.collected).toBe(false);
      expect(releaseUpdate!.data.collectedBy).toBe("");

      sync.destroy();
    });

    it("releases deposits when mode is descending", async () => {
      const { db, updates } = mockDb();
      const sync = new SyncManager(db, "alice");

      const staleDep = makeDeposit({
        fuelType: "triangle",
        id: "stale-2",
        collected: true,
        collectedBy: "alice",
      });

      sync.updateState(makePlayer({ mode: "descending" }));
      sync.setInputs({
        ...emptyInputs(),
        settled: true,
        allDepositsRaw: [staleDep],
        localPlayerRows: [{ id: "row-1" }],
      });

      await flushPromises();

      const releaseUpdate = updates.find((u) => u.id === "stale-2");
      expect(releaseUpdate).toBeTruthy();
      expect(releaseUpdate!.data.collected).toBe(false);

      sync.destroy();
    });

    it("does not release deposits when mode is walking", async () => {
      const { db, updates } = mockDb();
      const sync = new SyncManager(db, "alice");

      const myDep = makeDeposit({
        fuelType: "circle",
        id: "kept-1",
        collected: true,
        collectedBy: "alice",
      });

      sync.updateState(makePlayer({ mode: "walking" }));
      sync.setInputs({
        ...emptyInputs(),
        settled: true,
        allDepositsRaw: [myDep],
        localPlayerRows: [{ id: "row-1" }],
      });

      await flushPromises();

      const releaseUpdate = updates.find((u) => u.id === "kept-1" && u.data.collected === false);
      expect(releaseUpdate).toBeUndefined();

      sync.destroy();
    });

    it("does not release deposits collected by other players", async () => {
      const { db, updates } = mockDb();
      const sync = new SyncManager(db, "alice");

      const otherDep = makeDeposit({
        fuelType: "circle",
        id: "other-1",
        collected: true,
        collectedBy: "bob",
      });

      sync.updateState(makePlayer({ mode: "start" }));
      sync.setInputs({
        ...emptyInputs(),
        settled: true,
        allDepositsRaw: [otherDep],
        localPlayerRows: [{ id: "row-1" }],
      });

      await flushPromises();

      const releaseUpdate = updates.find((u) => u.id === "other-1" && u.data.collected === false);
      expect(releaseUpdate).toBeUndefined();

      sync.destroy();
    });
  });

  // =========================================================================
  // 6. State accessors
  // =========================================================================

  describe("state accessors", () => {
    it("setInputs updates the inputs property", () => {
      const { db } = mockDb();
      const sync = new SyncManager(db, "alice");

      const inputs = { ...emptyInputs(), settled: true };
      sync.setInputs(inputs);

      expect(sync.inputs).toBe(inputs);

      sync.destroy();
    });

    it("updateState updates the latestState property", () => {
      const { db } = mockDb();
      const sync = new SyncManager(db, "alice");

      expect(sync.latestState).toBeNull();

      const state = makePlayer();
      sync.updateState(state);

      expect(sync.latestState).toBe(state);

      sync.destroy();
    });
  });
});
