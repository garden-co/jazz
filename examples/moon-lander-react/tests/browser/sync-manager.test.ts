/**
 * Unit tests for Moon Lander — SyncManager.
 *
 * Tests the SyncManager class which owns all DB write state:
 *   - Event queuing (collectDeposit, refuel, shareFuel, burstDeposit, sendMessage)
 *   - Player state sync (insert on first flush, update on subsequent)
 *   - Deposit reconciliation (once on settle)
 *   - Release of stale deposits on restart
 *
 * Uses a thin db mock (same pattern as moon-lander-writes.test.tsx).
 * Timer is advanced manually via vi.advanceTimersByTime().
 */

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import type { PlayerInit, FuelDeposit } from "../../schema/app.js";
import type { FuelType } from "../../src/game/constants.js";
import { SyncManager, DEPOSITS_PER_TYPE, type SyncInputs } from "../../src/jazz/SyncManager.js";
import { DB_SYNC_INTERVAL_MS, FUEL_TYPES } from "../../src/game/constants.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function mockDb() {
  const inserts: Array<{
    table: unknown;
    data: Record<string, unknown>;
    options: { tier?: string } | undefined;
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

  return {
    db: {
      insert: vi.fn(
        async (table: unknown, data: Record<string, unknown>, options?: { tier?: string }) => {
          const id = `new-${inserts.length}`;
          inserts.push({ table, data, options });
          return id;
        },
      ),
      update: vi.fn(
        async (
          table: unknown,
          id: string,
          data: Record<string, unknown>,
          options?: { tier?: string },
        ) => {
          updates.push({ table, id, data, options });
        },
      ),
      deleteFrom: vi.fn(async (table: unknown, id: string, options?: { tier?: string }) => {
        deletes.push({ table, id, options });
      }),
    } as any,
    inserts,
    updates,
    deletes,
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
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  // =========================================================================
  // 1. Construction and destruction
  // =========================================================================

  describe("lifecycle", () => {
    it("starts a flush interval on construction", () => {
      const { db } = mockDb();
      const sync = new SyncManager(db, "alice");

      // Advance past one interval — flush should fire
      vi.advanceTimersByTime(DB_SYNC_INTERVAL_MS + 10);

      // No state to sync, so no DB calls, but no errors either
      sync.destroy();
    });

    it("destroy stops the interval", () => {
      const { db, inserts } = mockDb();
      const sync = new SyncManager(db, "alice");

      // Queue a message
      sync.sendMessage("hello");
      sync.setInputs({ ...emptyInputs(), settled: true });

      // Destroy before the interval fires
      sync.destroy();

      vi.advanceTimersByTime(DB_SYNC_INTERVAL_MS * 5);

      // Nothing should have been flushed
      expect(inserts).toHaveLength(0);
    });
  });

  // =========================================================================
  // 2. Event queuing
  //
  //   Each event method pushes to a queue. The queue is drained on flush.
  //   One interval tick = one flush.
  //
  //   Queue a few events → advance timer → verify DB calls.
  // =========================================================================

  describe("event queuing and flush", () => {
    it("flushes deposit collections", async () => {
      const { db, updates } = mockDb();
      const sync = new SyncManager(db, "alice");
      sync.setInputs({ ...emptyInputs(), settled: true });

      sync.collectDeposit("dep-1");
      sync.collectDeposit("dep-2");

      await vi.advanceTimersByTimeAsync(DB_SYNC_INTERVAL_MS + 10);

      expect(updates).toHaveLength(2);
      expect(updates[0].id).toBe("dep-1");
      expect(updates[0].data.collected).toBe(true);
      expect(updates[0].data.collectedBy).toBe("alice");
      expect(updates[1].id).toBe("dep-2");

      sync.destroy();
    });

    it("flushes refuels by releasing a collected deposit", async () => {
      const { db, inserts, deletes } = mockDb();
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

      await vi.advanceTimersByTimeAsync(DB_SYNC_INTERVAL_MS + 10);

      // Should delete the old deposit and insert a new uncollected one
      const deleteCall = deletes.find((d) => d.id === "dep-circle-1");
      expect(deleteCall).toBeTruthy();

      const releaseInsert = inserts.find(
        (i) =>
          i.data.fuelType === "circle" && i.data.positionX === 300 && i.data.collected === false,
      );
      expect(releaseInsert).toBeTruthy();
      expect(releaseInsert!.data.collectedBy).toBe("");

      sync.destroy();
    });

    it("flushes bursts by releasing a collected deposit", async () => {
      const { db, inserts, deletes } = mockDb();
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

      await vi.advanceTimersByTimeAsync(DB_SYNC_INTERVAL_MS + 10);

      // Should delete the old deposit and insert a new uncollected one
      const deleteCall = deletes.find((d) => d.id === "dep-tri-1");
      expect(deleteCall).toBeTruthy();

      const releaseInsert = inserts.find(
        (i) =>
          i.data.fuelType === "triangle" && i.data.positionX === 400 && i.data.collected === false,
      );
      expect(releaseInsert).toBeTruthy();

      sync.destroy();
    });

    it("flushes fuel shares by rewriting collectedBy", async () => {
      const { db, updates } = mockDb();
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

      await vi.advanceTimersByTimeAsync(DB_SYNC_INTERVAL_MS + 10);

      const shareUpdate = updates.find((u) => u.id === "dep-hex-1");
      expect(shareUpdate).toBeTruthy();
      expect(shareUpdate!.data.collectedBy).toBe("bob-uuid");

      sync.destroy();
    });

    it("flushes chat messages", async () => {
      const { db, inserts } = mockDb();
      const sync = new SyncManager(db, "alice");
      sync.setInputs({ ...emptyInputs(), settled: true });

      sync.sendMessage("hello moon");
      sync.sendMessage("need hexagon!");

      await vi.advanceTimersByTimeAsync(DB_SYNC_INTERVAL_MS + 10);

      // Two chat message inserts
      const chatInserts = inserts.filter((i) => (i.data as any).message !== undefined);
      expect(chatInserts).toHaveLength(2);
      expect(chatInserts[0].data.message).toBe("hello moon");
      expect(chatInserts[0].data.playerId).toBe("alice");
      expect(chatInserts[1].data.message).toBe("need hexagon!");

      sync.destroy();
    });

    it("queues are drained after flush (no double processing)", async () => {
      const { db, updates } = mockDb();
      const sync = new SyncManager(db, "alice");
      sync.setInputs({ ...emptyInputs(), settled: true });

      sync.collectDeposit("dep-1");

      // First flush
      await vi.advanceTimersByTimeAsync(DB_SYNC_INTERVAL_MS + 10);
      expect(updates).toHaveLength(1);

      // Second flush — queue should be empty
      await vi.advanceTimersByTimeAsync(DB_SYNC_INTERVAL_MS + 10);
      expect(updates).toHaveLength(1); // no new updates

      sync.destroy();
    });
  });

  // =========================================================================
  // 3. Player state sync
  //
  //   First flush with state + settled + no existing row → insert
  //   Subsequent flush with changed state → update
  //   No change → no write
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
        localPlayerRows: [], // no existing row
      });

      await vi.advanceTimersByTimeAsync(DB_SYNC_INTERVAL_MS + 10);

      // Should have inserted a player row
      const playerInsert = inserts.find((i) => (i.data as any).playerId === "alice-uuid");
      expect(playerInsert).toBeTruthy();

      sync.destroy();
    });

    it("updates an existing player row when state changes", async () => {
      const { db, updates } = mockDb();
      const sync = new SyncManager(db, "alice");

      const state = makePlayer();
      sync.updateState(state);
      sync.setInputs({
        ...emptyInputs(),
        settled: true,
        localPlayerRows: [{ id: "existing-row-1" }],
      });

      // First flush — should update (no lastSynced yet)
      await vi.advanceTimersByTimeAsync(DB_SYNC_INTERVAL_MS + 10);

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

      // First flush — should update
      await vi.advanceTimersByTimeAsync(DB_SYNC_INTERVAL_MS + 10);
      const firstCount = updates.length;

      // Second flush with same state — should skip
      await vi.advanceTimersByTimeAsync(DB_SYNC_INTERVAL_MS + 10);
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
        settled: false, // not settled yet
        localPlayerRows: [],
      });

      await vi.advanceTimersByTimeAsync(DB_SYNC_INTERVAL_MS + 10);

      // No insert or update should have happened
      const playerInsert = inserts.find((i) => (i.data as any).playerId === "alice-uuid");
      expect(playerInsert).toBeUndefined();

      sync.destroy();
    });
  });

  // =========================================================================
  // 4. Deposit reconciliation
  //
  //   Reconciliation runs exactly once, when settled becomes true.
  //   After that, subsequent flushes do not re-reconcile.
  // =========================================================================

  describe("deposit reconciliation", () => {
    it("reconciles deposits once when settled", async () => {
      const { db, inserts } = mockDb();
      const sync = new SyncManager(db, "alice");

      // No uncollected deposits → should insert DEPOSITS_PER_TYPE per type
      sync.setInputs({
        ...emptyInputs(),
        settled: true,
        uncollectedDeposits: [],
        perTypeLimits: FUEL_TYPES.map(() => DEPOSITS_PER_TYPE),
      });

      await vi.advanceTimersByTimeAsync(DB_SYNC_INTERVAL_MS + 10);

      const expectedInserts = FUEL_TYPES.length * DEPOSITS_PER_TYPE;
      // Filter to deposit inserts (have fuelType)
      const depositInserts = inserts.filter((i) => (i.data as any).fuelType !== undefined);
      expect(depositInserts.length).toBe(expectedInserts);

      // Second flush — should NOT reconcile again
      const countAfterFirst = inserts.length;
      await vi.advanceTimersByTimeAsync(DB_SYNC_INTERVAL_MS + 10);
      expect(inserts.length).toBe(countAfterFirst);

      sync.destroy();
    });

    it("does not reconcile when not settled", async () => {
      const { db, inserts } = mockDb();
      const sync = new SyncManager(db, "alice");

      sync.setInputs({
        ...emptyInputs(),
        settled: false,
      });

      await vi.advanceTimersByTimeAsync(DB_SYNC_INTERVAL_MS + 10);

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

      await vi.advanceTimersByTimeAsync(DB_SYNC_INTERVAL_MS + 10);

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

      await vi.advanceTimersByTimeAsync(DB_SYNC_INTERVAL_MS + 10);

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

      await vi.advanceTimersByTimeAsync(DB_SYNC_INTERVAL_MS + 10);

      // The only updates should be for player state, not deposit release
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
        collectedBy: "bob", // not alice
      });

      sync.updateState(makePlayer({ mode: "start" }));
      sync.setInputs({
        ...emptyInputs(),
        settled: true,
        allDepositsRaw: [otherDep],
        localPlayerRows: [{ id: "row-1" }],
      });

      await vi.advanceTimersByTimeAsync(DB_SYNC_INTERVAL_MS + 10);

      const releaseUpdate = updates.find((u) => u.id === "other-1" && u.data.collected === false);
      expect(releaseUpdate).toBeUndefined();

      sync.destroy();
    });
  });

  // =========================================================================
  // 6. setInputs and latestState
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
