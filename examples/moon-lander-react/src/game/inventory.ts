import type { Player } from "../../schema.js";
import type { FuelType } from "./constants";
import type { ArcAnimation } from "./types";
import { wrapDistance } from "./world";

// ---------------------------------------------------------------------------
// Inventory reconciliation — merge Jazz-confirmed state with local state
// ---------------------------------------------------------------------------

export interface InventoryMergeInput {
  /** The Jazz-derived inventory (source of truth from DB). */
  jazzInventory: FuelType[];
  /** Local authoritative inventory (what the engine currently holds). */
  localInventory: Set<FuelType>;
  /** Fuel types shared out, pending Jazz confirmation (prevents bounce-back). */
  sharedOut: Set<FuelType>;
  /** IDs of deposits collected locally but not yet confirmed by Jazz. */
  collectedIds: Set<string>;
  /** External deposits list (for cleaning up confirmed collected IDs). */
  externalDeposits: Array<{ id: string }>;
  /** Remote players (for finding nearest walker to animate share receipt). */
  remotePlayers: Player[];
  /** Local player's world X position. */
  playerX: number;
}

export interface InventoryMergeResult {
  /** Cleaned sharedOut set (confirmed entries removed). */
  sharedOut: Set<FuelType>;
  /** Collected IDs to remove (Jazz has confirmed them). */
  collectedIdsToRemove: string[];
  /** New arc animations for received shares. */
  newArcs: ArcAnimation[];
}

export function mergeInventory(input: InventoryMergeInput): InventoryMergeResult {
  const {
    jazzInventory,
    localInventory,
    sharedOut,
    collectedIds,
    externalDeposits,
    remotePlayers,
    playerX,
  } = input;

  const jazzSet = new Set(jazzInventory);

  // Clean up sharedOut entries Jazz has confirmed (type gone from Jazz inventory)
  const cleanedSharedOut = new Set(sharedOut);
  for (const ft of cleanedSharedOut) {
    if (!jazzSet.has(ft)) cleanedSharedOut.delete(ft);
  }

  // Detect received shares: items in Jazz inventory that aren't in our local
  // inventory and weren't shared out. These arrived from another player.
  const newArcs: ArcAnimation[] = [];
  for (const ft of jazzSet) {
    if (localInventory.has(ft)) continue;
    if (cleanedSharedOut.has(ft)) continue;
    // This fuel type appeared in Jazz but we didn't collect it locally — received share
    localInventory.add(ft);
    let nearestX = playerX;
    let nearestDist = Infinity;
    for (const rp of remotePlayers) {
      if (rp.mode !== "walking") continue;
      const dist = wrapDistance(playerX, rp.positionX);
      if (dist < nearestDist) {
        nearestDist = dist;
        nearestX = rp.positionX;
      }
    }
    newArcs.push({
      fuelType: ft,
      startX: nearestX,
      endX: playerX,
      peakHeight: 60 + Math.random() * 30,
      duration: 0.5,
      elapsed: 0,
      rotation: 0,
      glowPhase: Math.random() * Math.PI * 2,
    });
  }

  // Clean up collected IDs that Jazz has confirmed (no longer in deposit list)
  const collectedIdsToRemove: string[] = [];
  for (const id of collectedIds) {
    if (!externalDeposits.some((d) => d.id === id)) {
      collectedIdsToRemove.push(id);
    }
  }

  return {
    sharedOut: cleanedSharedOut,
    collectedIdsToRemove,
    newArcs,
  };
}
