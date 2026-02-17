import { MOON_SURFACE_WIDTH, FUEL_TYPES, type FuelType } from "./constants.js";
import type { Deposit } from "./types.js";

// ---------------------------------------------------------------------------
// World wrapping — the moon is round
// ---------------------------------------------------------------------------

/** Wrap an x coordinate into [0, MOON_SURFACE_WIDTH). */
export function wrapX(x: number): number {
  return ((x % MOON_SURFACE_WIDTH) + MOON_SURFACE_WIDTH) % MOON_SURFACE_WIDTH;
}

/** Shortest distance between two x positions on the wrapping surface. */
export function wrapDistance(a: number, b: number): number {
  const direct = Math.abs(a - b);
  return Math.min(direct, MOON_SURFACE_WIDTH - direct);
}

/** Lerp an X position toward a target, taking the shortest wrapping path. */
export function wrapLerp(current: number, target: number, t: number): number {
  let diff = target - current;
  if (diff > MOON_SURFACE_WIDTH / 2) diff -= MOON_SURFACE_WIDTH;
  if (diff < -MOON_SURFACE_WIDTH / 2) diff += MOON_SURFACE_WIDTH;
  return wrapX(current + diff * t);
}

/** Convert a world X to a screen X relative to the camera, with wrapping. */
export function wrapScreenX(worldX: number, cameraX: number): number {
  let dx = worldX - cameraX;
  if (dx < -MOON_SURFACE_WIDTH / 2) dx += MOON_SURFACE_WIDTH;
  if (dx > MOON_SURFACE_WIDTH / 2) dx -= MOON_SURFACE_WIDTH;
  return dx;
}

// ---------------------------------------------------------------------------
// Fuel deposits — scattered across the moon surface
// ---------------------------------------------------------------------------

/** Deterministic pseudo-random (simple sine hash). */
export function seededRand(seed: number): number {
  const x = Math.sin(seed * 127.1 + seed * 311.7) * 43758.5453;
  return x - Math.floor(x);
}

/**
 * Generate fuel deposits across the surface.
 * 3 of each fuel type spread evenly, plus 1 extra of the player's
 * required type placed 1/4–1/2 of the world away from the spawn point.
 * A no-spawn zone keeps deposits away from where the player lands.
 */
export function generateDeposits(requiredFuelType: FuelType, spawnX: number): Deposit[] {
  const deposits: Deposit[] = [];
  const noSpawnRadius = 300;

  // 3 of each type, spread across the full surface
  for (let ti = 0; ti < FUEL_TYPES.length; ti++) {
    for (let i = 0; i < 3; i++) {
      const seed = ti * 100 + i;
      let x = seededRand(seed) * MOON_SURFACE_WIDTH;
      // Push deposits out of the landing zone
      if (wrapDistance(x, spawnX) < noSpawnRadius) {
        x = wrapX(spawnX + noSpawnRadius + seededRand(seed + 0.7) * 1000);
      }
      deposits.push({ id: String(deposits.length), x, type: FUEL_TYPES[ti] });
    }
  }

  // 1 extra of the required type, placed 1/4–1/2 world away
  const offset = MOON_SURFACE_WIDTH / 4 + seededRand(9999) * (MOON_SURFACE_WIDTH / 4);
  deposits.push({ id: String(deposits.length), x: wrapX(spawnX + offset), type: requiredFuelType });

  return deposits;
}
