import type { useDb } from "jazz-tools/react";
import type { FuelDeposit, PlayerInit } from "../../schema/app";
import { app } from "../../schema/app";
import { FUEL_TYPES, MOON_SURFACE_WIDTH } from "../game/constants";

// ---------------------------------------------------------------------------
// Jazz write helpers and constants
// ---------------------------------------------------------------------------

export const STALE_THRESHOLD_S = 180; // 3 minutes

/** Base number of uncollected deposits per fuel type. */
export const DEPOSITS_PER_TYPE = 3;

/**
 * Reconcile deposits: insert missing deposits and trim excess per fuel type.
 *
 * Each player calls this once on join after the edge subscription settles.
 * Trimming excess handles the concurrent top-up race (two clients both
 * inserting a full set) and players leaving (reducing the target count).
 */
export async function reconcileDeposits(
  db: ReturnType<typeof useDb>,
  uncollectedDeposits: FuelDeposit[],
  perTypeLimits: number[],
) {
  const nowS = Math.floor(Date.now() / 1000);

  // Group uncollected deposits by fuel type
  const byType = new Map<string, FuelDeposit[]>();
  for (const ft of FUEL_TYPES) byType.set(ft, []);
  for (const d of uncollectedDeposits) {
    const arr = byType.get(d.fuelType);
    if (arr) arr.push(d);
  }

  for (let i = 0; i < FUEL_TYPES.length; i++) {
    const ft = FUEL_TYPES[i];
    const deposits = byType.get(ft) ?? [];
    const target = perTypeLimits[i];
    const diff = target - deposits.length;

    if (diff > 0) {
      // Insert missing deposits
      for (let j = 0; j < diff; j++) {
        await db.insertPersisted(
          app.fuel_deposits,
          {
            fuelType: ft,
            positionX: Math.floor(Math.random() * MOON_SURFACE_WIDTH),
            createdAt: nowS,
            collected: false,
            collectedBy: "",
          },
          "edge",
        );
      }
    } else if (diff < 0) {
      // Trim excess: remove the newest deposits first (highest createdAt)
      const sorted = [...deposits].sort((a, b) => b.createdAt - a.createdAt);
      for (let j = 0; j < -diff; j++) {
        await db.updatePersisted(
          app.fuel_deposits,
          sorted[j].id,
          { collected: true, collectedBy: "__trimmed__" },
          "edge",
        );
      }
    }
  }
}

/** Returns true if any synced field in PlayerInit has changed meaningfully. */
export function playerStateChanged(a: PlayerInit, b: PlayerInit): boolean {
  const POSITION_THRESHOLD = 2; // pixels
  const VELOCITY_THRESHOLD = 0.5; // pixels/tick
  return (
    a.mode !== b.mode ||
    Math.abs(a.positionX - b.positionX) > POSITION_THRESHOLD ||
    Math.abs(a.positionY - b.positionY) > POSITION_THRESHOLD ||
    Math.abs(a.velocityX - b.velocityX) > VELOCITY_THRESHOLD ||
    Math.abs(a.velocityY - b.velocityY) > VELOCITY_THRESHOLD ||
    a.landerFuelLevel !== b.landerFuelLevel ||
    a.landerSpawnX !== b.landerSpawnX ||
    a.name !== b.name ||
    a.color !== b.color ||
    a.requiredFuelType !== b.requiredFuelType ||
    a.thrusting !== b.thrusting
  );
}
