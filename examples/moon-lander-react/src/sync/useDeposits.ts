import { useAll } from "jazz-tools/react";
import { useMemo, useRef } from "react";
import type { FuelDeposit, Player } from "../../schema/app";
import { app } from "../../schema/app";
import type { FuelType } from "../game/constants";
import { FUEL_TYPES } from "../game/constants";
import { DEPOSITS_PER_TYPE, STALE_THRESHOLD_S } from "./writes";

// ---------------------------------------------------------------------------
// useDeposits — subscribe to all deposit data and derive game-ready views
// ---------------------------------------------------------------------------

export interface DepositState {
  /** True once all edge subscriptions have delivered their first result. */
  settled: boolean;
  /** Uncollected deposits mapped for the game renderer (with fade-in timing). */
  deposits: Array<{ id: string; x: number; type: FuelType; spawnTime: number }>;
  /** Fuel types currently held by this player. */
  inventory: FuelType[];
  /** Raw combined array of uncollected + my-collected deposits (for flush helpers). */
  allDepositsRaw: Array<{
    id: string;
    collected: boolean;
    collectedBy: string;
    fuelType: string;
  }>;
  /** Per-type uncollected counts (same order as FUEL_TYPES). */
  perTypeCounts: number[];
  /** Per-type target counts for top-up (same order as FUEL_TYPES). */
  perTypeLimits: number[];
  /** Local player's rows from the DB (for sync helpers). */
  localPlayerRows: Array<{ id: string }>;
  /** Count of deposits collected by this player (for debug stats). */
  myCollectedCount: number;
  /** Raw uncollected deposit rows from Jazz (for reconcileDeposits). */
  uncollectedRaw: FuelDeposit[];
  /** DEBUG: total rows in fuel_deposits table (all states). */
  debugTotalDeposits: number;
}

export function useDeposits(
  playerId: string,
  remotePlayerRows: Player[],
): DepositState {
  // Local player row — used to derive requiredFuelType and find existing DB row ID
  const localPlayerRows = useAll(app.players.where({ playerId }));

  const localFuelType = localPlayerRows[0]?.requiredFuelType ?? FUEL_TYPES[0];

  // Per-type deposit limits: DEPOSITS_PER_TYPE base + non-stale players needing that type.
  // All clients compute the same limits from the same edge-settled player data.
  const perTypeLimits = useMemo(() => {
    const nowS = Math.floor(Date.now() / 1000);
    const counts = new Map<string, number>();
    for (const ft of FUEL_TYPES) counts.set(ft, DEPOSITS_PER_TYPE);
    // +1 for local player
    counts.set(
      localFuelType,
      (counts.get(localFuelType) ?? DEPOSITS_PER_TYPE) + 1,
    );
    // +1 per non-stale remote player needing each type
    for (const p of remotePlayerRows) {
      if (p.requiredFuelType && nowS - p.lastSeen < STALE_THRESHOLD_S) {
        counts.set(
          p.requiredFuelType,
          (counts.get(p.requiredFuelType) ?? DEPOSITS_PER_TYPE) + 1,
        );
      }
    }
    return FUEL_TYPES.map((ft) => counts.get(ft) ?? DEPOSITS_PER_TYPE);
  }, [remotePlayerRows, localFuelType]);

  // DEBUG: unfiltered subscription to see total rows in table
  const debugAllDeposits = useAll(app.fuel_deposits, "edge");

  const allUncollected = useAll(
    app.fuel_deposits.where({ collected: false }),
    "edge",
  );

  // Deposits collected by this player (inventory)
  const myCollectedDeposits = useAll(
    app.fuel_deposits.where({ collectedBy: playerId }),
    "edge",
  );

  // All edge subscriptions have delivered (none still undefined)
  const settled =
    allUncollected !== undefined &&
    myCollectedDeposits !== undefined &&
    debugAllDeposits !== undefined;

  const uncollectedDeposits = allUncollected ?? [];

  // Combined view for consumers that need both uncollected + my-collected
  const allDepositsRaw = useMemo(
    () => [...uncollectedDeposits, ...(myCollectedDeposits ?? [])],
    [uncollectedDeposits, myCollectedDeposits],
  );

  const perTypeCounts = useMemo(() => {
    const counts = new Array(FUEL_TYPES.length).fill(0);
    for (const d of uncollectedDeposits) {
      const idx = FUEL_TYPES.indexOf(d.fuelType as FuelType);
      if (idx >= 0) counts[idx]++;
    }
    return counts;
  }, [uncollectedDeposits]);

  // Track when each deposit ID was first seen (monotonic seconds) for fade-in
  const depositSpawnTimesRef = useRef<Map<string, number>>(new Map());

  // Map Jazz deposit subscription → Deposit[] for Game (uncollected only, with fade-in timing)
  const deposits = useMemo(() => {
    const spawnTimes = depositSpawnTimesRef.current;
    const now = performance.now() / 1000;
    // Record first-seen time for new deposits
    const activeIds = new Set<string>();
    for (const d of uncollectedDeposits) {
      activeIds.add(d.id);
      if (!spawnTimes.has(d.id)) {
        spawnTimes.set(d.id, now);
      }
    }
    // Prune stale entries
    for (const id of spawnTimes.keys()) {
      if (!activeIds.has(id)) spawnTimes.delete(id);
    }
    return uncollectedDeposits.map((d) => ({
      id: d.id,
      x: d.positionX,
      type: d.fuelType as FuelType,
      spawnTime: spawnTimes.get(d.id) ?? now,
    }));
  }, [uncollectedDeposits]);

  // Derive inventory from Jazz: fuel types where collectedBy = this player
  const inventory = useMemo(() => {
    return (myCollectedDeposits ?? []).map((d) => d.fuelType as FuelType);
  }, [myCollectedDeposits]);

  return {
    settled,
    deposits,
    inventory,
    allDepositsRaw,
    perTypeCounts,
    perTypeLimits,
    localPlayerRows,
    myCollectedCount: myCollectedDeposits?.length ?? 0,
    uncollectedRaw: uncollectedDeposits,
    debugTotalDeposits: debugAllDeposits?.length ?? 0,
  };
}
