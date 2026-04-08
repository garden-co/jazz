/**
 * Unit tests for Moon Lander — sync write helpers.
 *
 * Tests the pure/near-pure functions in `src/sync/writes.ts`:
 *   - `playerStateChanged` — compares two PlayerInit snapshots with thresholds
 *   - `reconcileDeposits`  — inserts missing deposits and trims excess per type
 *
 * These are isolated function tests (the CLAUDE.md exception for pure functions).
 * `reconcileDeposits` uses a thin db mock to capture insert/update calls.
 */

import { describe, it, expect, vi } from "vitest";
import type { PlayerInit, FuelDeposit } from "../../schema.js";
import {
  playerStateChanged,
  reconcileDeposits,
  DEPOSITS_PER_TYPE,
} from "../../src/jazz/SyncManager.js";
import { FUEL_TYPES, MOON_SURFACE_WIDTH } from "../../src/game/constants.js";

// ---------------------------------------------------------------------------
// playerStateChanged
// ---------------------------------------------------------------------------

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

describe("playerStateChanged", () => {
  it("returns false for identical states", () => {
    const a = makePlayer();
    const b = makePlayer();
    expect(playerStateChanged(a, b)).toBe(false);
  });

  it("detects mode change", () => {
    const a = makePlayer({ mode: "walking" });
    const b = makePlayer({ mode: "descending" });
    expect(playerStateChanged(a, b)).toBe(true);
  });

  it("ignores position changes within threshold (2px)", () => {
    const a = makePlayer({ positionX: 100, positionY: 200 });
    const b = makePlayer({ positionX: 101, positionY: 201 });
    expect(playerStateChanged(a, b)).toBe(false);
  });

  it("detects position changes beyond threshold", () => {
    const a = makePlayer({ positionX: 100 });
    const b = makePlayer({ positionX: 103 });
    expect(playerStateChanged(a, b)).toBe(true);
  });

  it("ignores velocity changes within threshold (0.5 px/tick)", () => {
    const a = makePlayer({ velocityX: 1.0 });
    const b = makePlayer({ velocityX: 1.4 });
    expect(playerStateChanged(a, b)).toBe(false);
  });

  it("detects velocity changes beyond threshold", () => {
    const a = makePlayer({ velocityY: 0 });
    const b = makePlayer({ velocityY: 0.6 });
    expect(playerStateChanged(a, b)).toBe(true);
  });

  it("detects fuel level change", () => {
    const a = makePlayer({ landerFuelLevel: 40 });
    const b = makePlayer({ landerFuelLevel: 100 });
    expect(playerStateChanged(a, b)).toBe(true);
  });

  it("detects name change", () => {
    const a = makePlayer({ name: "Alice" });
    const b = makePlayer({ name: "Bob" });
    expect(playerStateChanged(a, b)).toBe(true);
  });

  it("detects colour change", () => {
    const a = makePlayer({ color: "#ff0000" });
    const b = makePlayer({ color: "#00ff00" });
    expect(playerStateChanged(a, b)).toBe(true);
  });

  it("detects requiredFuelType change", () => {
    const a = makePlayer({ requiredFuelType: "circle" });
    const b = makePlayer({ requiredFuelType: "triangle" });
    expect(playerStateChanged(a, b)).toBe(true);
  });

  it("detects thrusting change", () => {
    const a = makePlayer({ thrusting: false });
    const b = makePlayer({ thrusting: true });
    expect(playerStateChanged(a, b)).toBe(true);
  });

  it("detects landerSpawnX change", () => {
    const a = makePlayer({ landerSpawnX: 480 });
    const b = makePlayer({ landerSpawnX: 600 });
    expect(playerStateChanged(a, b)).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// reconcileDeposits
// ---------------------------------------------------------------------------

/**
 * Build a fake FuelDeposit with the given overrides.
 * IDs use the format "dep-{fuelType}-{index}" for readability.
 */
function makeDeposit(overrides: Partial<FuelDeposit> & { fuelType: string }): FuelDeposit {
  return {
    id: `dep-${overrides.fuelType}-${Math.random().toString(36).slice(2, 6)}`,
    positionX: Math.floor(Math.random() * MOON_SURFACE_WIDTH),
    createdAt: 1000,
    collected: false,
    collectedBy: "",
    ...overrides,
  };
}

/**
 * Minimal db mock that captures insert and update calls.
 *
 * reconcileDeposits only uses these two methods, both returning a promise.
 */
function mockDb() {
  const inserts: Array<{ table: unknown; data: Record<string, unknown>; tier: string }> = [];
  const updates: Array<{
    table: unknown;
    id: string;
    data: Record<string, unknown>;
    tier: string;
  }> = [];

  return {
    db: {
      insertDurable: vi.fn(
        async (table: unknown, data: Record<string, unknown>, options?: { tier?: string }) => {
          const id = `new-${inserts.length}`;
          inserts.push({ table, data, tier: options?.tier ?? "edge" });
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
          updates.push({ table, id, data, tier: options?.tier ?? "edge" });
        },
      ),
    } as any,
    inserts,
    updates,
  };
}

describe("reconcileDeposits", () => {
  // =========================================================================
  // 1. Inserts missing deposits when count is below target
  //
  //   uncollected: [circle x1]     target: [circle x3, triangle x3, ...]
  //                                → inserts 2 circles, 3 triangles, ...
  // =========================================================================

  it("inserts missing deposits to reach per-type target", async () => {
    const { db, inserts, updates } = mockDb();

    // Only 1 circle deposit exists, all other types have 0
    const existing = [makeDeposit({ fuelType: "circle", createdAt: 1000 })];

    // Targets: DEPOSITS_PER_TYPE (3) for each type
    const limits = FUEL_TYPES.map(() => DEPOSITS_PER_TYPE);

    await reconcileDeposits(db, existing, limits);

    // Should insert 2 circles + 3 of each remaining type
    const expectedInserts = 2 + (FUEL_TYPES.length - 1) * DEPOSITS_PER_TYPE;
    expect(inserts.length).toBe(expectedInserts);

    // No trims needed
    expect(updates.length).toBe(0);

    // Each inserted deposit should have collected: false
    for (const ins of inserts) {
      expect(ins.data.collected).toBe(false);
      expect(ins.data.collectedBy).toBe("");
      expect(ins.tier).toBe("edge");
    }
  });

  // =========================================================================
  // 2. No-op when counts match targets
  //
  //   uncollected: [3 of each type]   target: [3, 3, 3, ...]
  //                                    → 0 inserts, 0 updates
  // =========================================================================

  it("does nothing when all per-type counts match targets", async () => {
    const { db, inserts, updates } = mockDb();

    // Build exactly DEPOSITS_PER_TYPE deposits for each fuel type
    const existing: FuelDeposit[] = [];
    for (const ft of FUEL_TYPES) {
      for (let i = 0; i < DEPOSITS_PER_TYPE; i++) {
        existing.push(makeDeposit({ fuelType: ft, createdAt: 1000 + i }));
      }
    }

    const limits = FUEL_TYPES.map(() => DEPOSITS_PER_TYPE);

    await reconcileDeposits(db, existing, limits);

    expect(inserts.length).toBe(0);
    expect(updates.length).toBe(0);
  });

  // =========================================================================
  // 3. Trims excess deposits (newest first)
  //
  //   uncollected: [circle x5]   target: [circle x3]
  //                               → marks 2 newest circles as collected
  //
  //   createdAt:  10  20  30  40  50
  //                              ^^  ^^  ← trimmed (newest first)
  // =========================================================================

  it("trims excess deposits by marking newest as collected", async () => {
    const { db, inserts, updates } = mockDb();

    // 5 circle deposits with ascending createdAt
    const circles: FuelDeposit[] = [];
    for (let i = 0; i < 5; i++) {
      circles.push(
        makeDeposit({
          fuelType: "circle",
          id: `circle-${i}`,
          createdAt: 100 + i * 10,
        }),
      );
    }

    // Build full set with other types at target
    const existing: FuelDeposit[] = [...circles];
    for (const ft of FUEL_TYPES) {
      if (ft === "circle") continue;
      for (let i = 0; i < DEPOSITS_PER_TYPE; i++) {
        existing.push(makeDeposit({ fuelType: ft, createdAt: 1000 + i }));
      }
    }

    const limits = FUEL_TYPES.map(() => DEPOSITS_PER_TYPE);

    await reconcileDeposits(db, existing, limits);

    // Should trim 2 excess circles (5 - 3 = 2), no inserts
    expect(inserts.length).toBe(0);
    expect(updates.length).toBe(2);

    // Trimmed deposits should be the two newest (createdAt 140, 130)
    const trimmedIds = updates.map((u) => u.id);
    expect(trimmedIds).toContain("circle-4"); // createdAt 140
    expect(trimmedIds).toContain("circle-3"); // createdAt 130

    // Each trimmed deposit gets marked as collected with sentinel
    for (const upd of updates) {
      expect(upd.data.collected).toBe(true);
      expect(upd.data.collectedBy).toBe("__trimmed__");
      expect(upd.tier).toBe("edge");
    }
  });

  // =========================================================================
  // 4. Handles mixed insert + trim across different types
  //
  //   circle: 5 (excess by 2) → trim 2
  //   triangle: 1 (short by 2) → insert 2
  //   others: at target → no-op
  // =========================================================================

  it("handles mixed insert and trim across types", async () => {
    const { db, inserts, updates } = mockDb();

    const existing: FuelDeposit[] = [];

    for (const ft of FUEL_TYPES) {
      if (ft === "circle") {
        // 5 circles — 2 excess
        for (let i = 0; i < 5; i++) {
          existing.push(makeDeposit({ fuelType: ft, id: `c-${i}`, createdAt: 100 + i }));
        }
      } else if (ft === "triangle") {
        // 1 triangle — 2 short
        existing.push(makeDeposit({ fuelType: ft, createdAt: 500 }));
      } else {
        // Exactly at target
        for (let i = 0; i < DEPOSITS_PER_TYPE; i++) {
          existing.push(makeDeposit({ fuelType: ft, createdAt: 1000 + i }));
        }
      }
    }

    const limits = FUEL_TYPES.map(() => DEPOSITS_PER_TYPE);

    await reconcileDeposits(db, existing, limits);

    // 2 inserts (for triangle shortfall) + 2 trims (for circle excess)
    expect(inserts.length).toBe(2);
    expect(updates.length).toBe(2);

    // Inserted deposits should be triangles
    for (const ins of inserts) {
      expect(ins.data.fuelType).toBe("triangle");
    }

    // Trimmed deposits should be circles
    for (const upd of updates) {
      expect(upd.id).toMatch(/^c-/);
    }
  });

  // =========================================================================
  // 5. Inserted deposits have valid positionX within world bounds
  // =========================================================================

  it("inserts deposits with positionX within world bounds", async () => {
    const { db, inserts } = mockDb();

    await reconcileDeposits(
      db,
      [],
      FUEL_TYPES.map(() => DEPOSITS_PER_TYPE),
    );

    expect(inserts.length).toBeGreaterThan(0);
    for (const ins of inserts) {
      const x = ins.data.positionX as number;
      expect(x).toBeGreaterThanOrEqual(0);
      expect(x).toBeLessThan(MOON_SURFACE_WIDTH);
    }
  });

  // =========================================================================
  // 6. Empty input with zero targets produces no operations
  // =========================================================================

  it("does nothing when all targets are zero", async () => {
    const { db, inserts, updates } = mockDb();

    await reconcileDeposits(
      db,
      [],
      FUEL_TYPES.map(() => 0),
    );

    expect(inserts.length).toBe(0);
    expect(updates.length).toBe(0);
  });
});
