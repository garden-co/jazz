import type { FuelType } from "./constants.js";
import type { ArcAnimation, RemotePlayerView } from "./types.js";
import { wrapDistance } from "./world.js";

// ---------------------------------------------------------------------------
// Inventory merge — reconcile Jazz props with local optimistic state
// ---------------------------------------------------------------------------

export interface InventoryMergeInput {
  /** The Jazz-derived inventory (source of truth from DB). */
  jazzInventory: FuelType[];
  /** Fuel types collected optimistically this session. */
  optimistic: Set<FuelType>;
  /** Fuel types shared out, pending Jazz confirmation. */
  sharedOut: Set<FuelType>;
  /** IDs of deposits collected locally but not yet confirmed by Jazz. */
  collectedIds: Set<string>;
  /** Previous Jazz inventory snapshot (for detecting received shares). */
  prevJazzInventory: Set<FuelType>;
  /** External deposits list (for cleaning up confirmed collected IDs). */
  externalDeposits: Array<{ id: string }>;
  /** Remote players (for finding nearest walker to animate share receipt). */
  remotePlayers: RemotePlayerView[];
  /** Local player's world X position. */
  playerX: number;
}

export interface InventoryMergeResult {
  /** The merged working inventory set. */
  merged: Set<FuelType>;
  /** Cleaned sharedOut set (confirmed entries removed). */
  sharedOut: Set<FuelType>;
  /** Updated previous Jazz inventory snapshot. */
  prevJazzInventory: Set<FuelType>;
  /** Collected IDs to remove (Jazz has confirmed them). */
  collectedIdsToRemove: string[];
  /** New arc animations for received shares. */
  newArcs: ArcAnimation[];
}

export function mergeInventory(input: InventoryMergeInput): InventoryMergeResult {
  const {
    jazzInventory,
    optimistic,
    sharedOut,
    collectedIds,
    prevJazzInventory,
    externalDeposits,
    remotePlayers,
    playerX,
  } = input;

  // Build merged set: Jazz + optimistic, minus sharedOut
  const merged = new Set([...jazzInventory, ...optimistic]);
  const cleanedSharedOut = new Set(sharedOut);
  for (const ft of cleanedSharedOut) {
    merged.delete(ft);
  }

  // Clean up sharedOut entries Jazz has confirmed (type gone from props)
  const propsSet = new Set(jazzInventory);
  const sharedClean: FuelType[] = [];
  for (const ft of cleanedSharedOut) {
    if (!propsSet.has(ft)) sharedClean.push(ft);
  }
  for (const ft of sharedClean) cleanedSharedOut.delete(ft);

  // Detect received shares: new items in Jazz inventory that weren't
  // optimistically collected locally -> animate fuel arriving from nearest
  // walking remote player
  const newArcs: ArcAnimation[] = [];
  for (const ft of propsSet) {
    if (!prevJazzInventory.has(ft) && !optimistic.has(ft)) {
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
      });
    }
  }

  // Clean up collected IDs that Jazz has confirmed (no longer in deposit list)
  const collectedIdsToRemove: string[] = [];
  for (const id of collectedIds) {
    if (!externalDeposits.some((d) => d.id === id)) {
      collectedIdsToRemove.push(id);
    }
  }

  return {
    merged,
    sharedOut: cleanedSharedOut,
    prevJazzInventory: propsSet,
    collectedIdsToRemove,
    newArcs,
  };
}
