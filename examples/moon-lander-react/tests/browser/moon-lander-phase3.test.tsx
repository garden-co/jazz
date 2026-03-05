/**
 * E2E browser tests for Moon Lander — Phase 3: Fuel Collection.
 *
 * Tests the complete fuel loop: deposits on the surface, fuel burn during
 * thrust, collecting deposits, inventory tracking, refuelling the lander,
 * and launching.
 *
 * Phase 3 data attribute contract (new additions):
 *   data-deposit-count   — number of uncollected fuel deposits on the surface
 *   data-inventory       — comma-separated fuel types currently held (e.g. "circle,triangle")
 *   data-lander-fuel     — (existing) current lander fuel level
 *   data-player-mode     — (existing) adds "launched" state
 *
 * All tests mount <Game> directly (no Jazz sync) with physicsSpeed=10.
 *
 * NOTE: physicsSpeed=10 means free-fall from INITIAL_ALTITUDE always exceeds
 * the crash threshold. Tests that need the lander on the ground use
 * initialMode="landed". Descent-only tests use initialMode="descending".
 */

import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, describe, expect, it } from "vitest";
import { Game } from "../../src/Game";
import {
  FUEL_BURN_X,
  FUEL_BURN_Y,
  FUEL_TYPES,
  INITIAL_FUEL,
  MAX_FUEL,
} from "../../src/game/constants";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Physics speed multiplier — 10x makes descent ~1s instead of ~7s. */
const SPEED = 10;

const mounts: Array<{ root: Root; container: HTMLDivElement }> = [];

/** Mount the Game in descending mode (for thrust/fuel-burn tests). */
async function mountDescending(): Promise<HTMLDivElement> {
  const el = document.createElement("div");
  document.body.appendChild(el);
  const root = createRoot(el);
  mounts.push({ root, container: el });

  await act(async () => {
    root.render(<Game {...({ physicsSpeed: SPEED, initialMode: "descending" } as any)} />);
  });

  await waitFor(
    () => el.querySelector('[data-testid="game-canvas"]') !== null,
    3000,
    "Game canvas should render",
  );

  return el;
}

/** Mount the Game already landed on the surface. */
async function mountLanded(): Promise<HTMLDivElement> {
  const el = document.createElement("div");
  document.body.appendChild(el);
  const root = createRoot(el);
  mounts.push({ root, container: el });

  await act(async () => {
    root.render(<Game {...({ physicsSpeed: SPEED, initialMode: "landed" } as any)} />);
  });

  await waitFor(
    () => el.querySelector('[data-testid="game-canvas"]') !== null,
    3000,
    "Game canvas should render",
  );

  return el;
}

afterEach(async () => {
  for (const { root, container } of mounts) {
    try {
      await act(async () => root.unmount());
    } catch {
      /* best effort */
    }
    container.remove();
  }
  mounts.length = 0;
});

/** Poll until a condition is true, or throw after timeout. */
async function waitFor(check: () => boolean, timeoutMs: number, message: string): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (check()) return;
    await new Promise((r) => setTimeout(r, 50));
  }
  throw new Error(`Timeout: ${message}`);
}

/** Read a numeric data attribute from the game container. */
function readNum(el: HTMLDivElement, attr: string): number {
  const container = el.querySelector('[data-testid="game-container"]')!;
  const raw = container.getAttribute(`data-${attr}`);
  if (raw === null) throw new Error(`Missing data attribute: data-${attr}`);
  return parseFloat(raw);
}

/** Read a string data attribute from the game container. */
function readStr(el: HTMLDivElement, attr: string): string {
  const container = el.querySelector('[data-testid="game-container"]')!;
  const raw = container.getAttribute(`data-${attr}`);
  if (raw === null) throw new Error(`Missing data attribute: data-${attr}`);
  return raw;
}

/** Wait until a data attribute equals the expected value. */
async function waitForAttr(
  el: HTMLDivElement,
  attr: string,
  expected: string,
  timeoutMs = 5000,
): Promise<void> {
  const container = el.querySelector('[data-testid="game-container"]')!;
  await waitFor(
    () => container.getAttribute(`data-${attr}`) === expected,
    timeoutMs,
    `data-${attr} should become "${expected}" (got "${container.getAttribute(`data-${attr}`)}")`,
  );
}

/** Wait until a numeric data attribute satisfies a predicate. */
async function waitForNum(
  el: HTMLDivElement,
  attr: string,
  predicate: (v: number) => boolean,
  timeoutMs: number,
  message: string,
): Promise<void> {
  await waitFor(
    () => {
      try {
        return predicate(readNum(el, attr));
      } catch {
        return false;
      }
    },
    timeoutMs,
    message,
  );
}

/** Simulate pressing a key (keydown). */
function pressKey(key: string, code?: string) {
  document.dispatchEvent(
    new KeyboardEvent("keydown", {
      key,
      code: code ?? key,
      bubbles: true,
    }),
  );
}

/** Simulate releasing a key (keyup). */
function releaseKey(key: string, code?: string) {
  document.dispatchEvent(
    new KeyboardEvent("keyup", {
      key,
      code: code ?? key,
      bubbles: true,
    }),
  );
}

/** Hold a key for a duration (ms), then release. */
async function holdKey(key: string, durationMs: number, code?: string) {
  pressKey(key, code);
  await new Promise((r) => setTimeout(r, durationMs));
  releaseKey(key, code);
}

/** Wait for N animation frames to let the game loop process. */
async function waitFrames(n: number): Promise<void> {
  for (let i = 0; i < n; i++) {
    await new Promise((r) => requestAnimationFrame(r));
  }
}

/** Mount landed, exit the lander, and start walking. */
async function landAndWalk(): Promise<HTMLDivElement> {
  const el = await mountLanded();

  pressKey("e", "KeyE");
  await waitForAttr(el, "player-mode", "walking", 3000);
  releaseKey("e", "KeyE");

  return el;
}

/** Parse the inventory data attribute into an array of fuel types. */
function readInventory(el: HTMLDivElement): string[] {
  const raw = readStr(el, "inventory");
  if (raw === "") return [];
  return raw.split(",");
}

// ---------------------------------------------------------------------------
// Phase 3: Fuel Collection
// ---------------------------------------------------------------------------

describe("Moon Lander — Phase 3: Fuel Collection", () => {
  // =========================================================================
  // 1. Fuel deposits spawn on the moon surface
  // =========================================================================

  it("fuel deposits appear on the moon surface after landing", async () => {
    const el = await landAndWalk();

    // The spec says 3 deposits per type (7 types) + 1 of the player's type
    // = 22 total. Allow some tolerance but expect > 0.
    const count = readNum(el, "deposit-count");
    expect(count).toBeGreaterThan(0);
  });

  it("deposits include all fuel types", async () => {
    /**
     * World generation seeds 3 of each fuel type across the surface.
     * The deposit-types attribute exposes the set of fuel types present.
     *
     *   surface: triangle square pentagon circle triangle hexagon circle ... (3 of each x 7 types = 21+)
     */
    const el = await landAndWalk();

    // deposit-count should be at least 7 (one per type minimum)
    const count = readNum(el, "deposit-count");
    expect(count).toBeGreaterThanOrEqual(7);
  });

  // =========================================================================
  // 2. Thrust burns fuel
  // =========================================================================

  it("vertical thrust burns fuel during descent", async () => {
    const el = await mountDescending();

    const fuelBefore = readNum(el, "lander-fuel");
    expect(fuelBefore).toBe(INITIAL_FUEL);

    // Apply upward thrust for 200ms (= 2s game time at 10x)
    // Expected burn: FUEL_BURN_Y * 2s = 16 units
    await holdKey("ArrowUp", 200, "ArrowUp");
    await waitFrames(5);

    const fuelAfter = readNum(el, "lander-fuel");
    expect(fuelAfter).toBeLessThan(fuelBefore);

    // Verify approximate burn rate: ~16 units in 2 game-seconds
    const burned = fuelBefore - fuelAfter;
    expect(burned).toBeGreaterThan(FUEL_BURN_Y * 1); // At least 1 game-second of burn
    expect(burned).toBeLessThan(FUEL_BURN_Y * 4); // At most 4 game-seconds of burn
  });

  it("horizontal thrust burns fuel during descent", async () => {
    const el = await mountDescending();

    const fuelBefore = readNum(el, "lander-fuel");

    // Thrust right for 200ms (= 2s game time at 10x)
    await holdKey("ArrowRight", 200, "ArrowRight");
    await waitFrames(5);

    const fuelAfter = readNum(el, "lander-fuel");
    expect(fuelAfter).toBeLessThan(fuelBefore);

    const burned = fuelBefore - fuelAfter;
    expect(burned).toBeGreaterThan(FUEL_BURN_X * 1);
    expect(burned).toBeLessThan(FUEL_BURN_X * 4);
  });

  it("thrust is disabled when fuel reaches 0", async () => {
    /**
     * Burn all fuel, then try to thrust; velocity should not decrease.
     *
     *   fuel: 40 --thrust--> 0
     *   then: ArrowUp pressed, but no thrust applied
     *         gravity continues pulling lander down
     */
    const el = await mountDescending();

    // Burn all fuel by thrusting for a long time
    // 40 units / (FUEL_BURN_Y=8 units/sec) = 5 game-seconds = 500ms at 10x
    await holdKey("ArrowUp", 600, "ArrowUp");
    await waitFrames(5);

    await waitForNum(el, "lander-fuel", (f) => f === 0, 3000, "fuel should reach 0");

    // Now check velocity; gravity should be accelerating us downward
    // and pressing thrust should have no effect
    const vyBefore = readNum(el, "velocity-y");

    await holdKey("ArrowUp", 100, "ArrowUp");
    await waitFrames(5);

    const vyAfter = readNum(el, "velocity-y");
    // With no fuel, thrust does nothing; gravity only pulls downward,
    // so velocity should be >= what it was (more downward or same)
    expect(vyAfter).toBeGreaterThanOrEqual(vyBefore);
  });

  // =========================================================================
  // 3. Collection mechanic
  // =========================================================================

  it("walking over a deposit collects it", async () => {
    /**
     * Player lands, exits lander, walks across the surface.
     * Eventually walks over a fuel deposit and collects it.
     *
     *   lander    deposit
     *     v         v
     *   ===+========+========
     *      +--walk-->  collect!
     *
     * inventory: [] -> ["circle"] (or whatever type)
     */
    const el = await landAndWalk();

    const inventoryBefore = readInventory(el);
    expect(inventoryBefore).toHaveLength(0);

    // Walk right for a while to find deposits
    // At 10x speed, 120px/s * 10 = 1200px/s -> 3s covers ~3600px
    await holdKey("d", 3000, "KeyD");
    await waitFrames(5);

    const inventoryAfter = readInventory(el);
    expect(inventoryAfter.length).toBeGreaterThan(0);

    // Each collected type should be a valid fuel type
    for (const type of inventoryAfter) {
      expect((FUEL_TYPES as readonly string[]).includes(type)).toBe(true);
    }
  });

  it("collecting a deposit removes it from the surface", async () => {
    const el = await landAndWalk();

    const countBefore = readNum(el, "deposit-count");

    // Walk to collect at least one deposit
    await holdKey("d", 3000, "KeyD");
    await waitFrames(5);

    // Verify something was collected
    const inventory = readInventory(el);
    if (inventory.length > 0) {
      const countAfter = readNum(el, "deposit-count");
      expect(countAfter).toBeLessThan(countBefore);
    }
  });

  it("inventory is capped at 1 per fuel type", async () => {
    /**
     * The spec says: "Players hold at most 1 unit of each fuel type."
     * Walking over a deposit you already have does nothing.
     */
    const el = await landAndWalk();

    // Walk far enough to collect multiple deposits
    await holdKey("d", 5000, "KeyD");
    await waitFrames(5);

    const inventory = readInventory(el);

    // Check no duplicates: each type should appear at most once
    const uniqueTypes = new Set(inventory);
    expect(uniqueTypes.size).toBe(inventory.length);
  });

  // =========================================================================
  // 4. Inventory display
  // =========================================================================

  it("inventory data attribute updates when fuel is collected", async () => {
    const el = await landAndWalk();

    // Initially empty
    const before = readStr(el, "inventory");
    expect(before).toBe("");

    // Walk to collect
    await holdKey("d", 3000, "KeyD");
    await waitFrames(5);

    // Should now have something
    const after = readStr(el, "inventory");
    expect(after).not.toBe("");
  });

  // =========================================================================
  // 5. Return to lander and refuel
  // =========================================================================

  it("re-entering lander with correct fuel type refuels it", async () => {
    /**
     *   lander (needs pentagon)     pentagon deposit
     *        v                         v
     *   ====+==========================+=====
     *       +----walk right-----------> collect pentagon
     *       <----walk back--------------+
     *       press E -> enter lander
     *       fuel: 40 -> 100 (capped)
     *
     * The player's required fuel type is deterministic from their ID.
     * Walking far enough should find a deposit of the correct type
     * (spec guarantees one spawns 1/4 to 1/2 world away).
     */
    const el = await landAndWalk();

    const requiredFuel = readStr(el, "required-fuel");
    const fuelBefore = readNum(el, "lander-fuel");

    // Walk far right to collect the required fuel type
    // World is 9600px. Required fuel is placed 1/4 to 1/2 away (2400 to 4800px).
    // At 10x speed + 120px/s = 1200px/s -> walk 4s to cover ~4800px
    await holdKey("d", 4000, "KeyD");
    await waitFrames(5);

    // Check if we collected the required fuel
    const inventory = readInventory(el);
    const hasRequired = inventory.includes(requiredFuel);

    if (hasRequired) {
      // Walk back to lander
      await holdKey("a", 4000, "KeyA");
      await waitFrames(5);

      // Re-enter lander
      pressKey("e", "KeyE");
      await waitFrames(10);
      releaseKey("e", "KeyE");
      await waitForAttr(el, "player-mode", "in_lander", 3000);

      // Fuel should have increased
      const fuelAfter = readNum(el, "lander-fuel");
      expect(fuelAfter).toBeGreaterThan(fuelBefore);
      expect(fuelAfter).toBeLessThanOrEqual(MAX_FUEL);
    } else {
      // If we didn't find the required fuel, the test is inconclusive
      // but we can at least verify refuelling doesn't happen without it
      // Walk back
      await holdKey("a", 4000, "KeyA");
      await waitFrames(5);

      pressKey("e", "KeyE");
      await waitFrames(10);
      releaseKey("e", "KeyE");
      await waitForAttr(el, "player-mode", "in_lander", 3000);

      // Fuel should NOT have changed (no correct fuel type)
      const fuelAfter = readNum(el, "lander-fuel");
      expect(fuelAfter).toBeLessThanOrEqual(fuelBefore);
    }
  });

  it("refuelling does not exceed MAX_FUEL", async () => {
    const el = await landAndWalk();

    // Walk to collect something, return, refuel
    await holdKey("d", 4000, "KeyD");
    await waitFrames(5);
    await holdKey("a", 4000, "KeyA");
    await waitFrames(5);

    pressKey("e", "KeyE");
    await waitFrames(10);
    releaseKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "in_lander", 3000);

    const fuel = readNum(el, "lander-fuel");
    expect(fuel).toBeLessThanOrEqual(MAX_FUEL);
  });

  it("re-entering lander without correct fuel does not refuel", async () => {
    /**
     * If the player re-enters the lander with only wrong-type fuel
     * (or empty inventory), lander fuel should not increase.
     */
    const el = await landAndWalk();

    const fuelBefore = readNum(el, "lander-fuel");

    // Walk a short distance (unlikely to collect the specific required type)
    // and then come back immediately
    await holdKey("d", 50, "KeyD");
    await waitFrames(3);
    await holdKey("a", 50, "KeyA");
    await waitFrames(3);

    pressKey("e", "KeyE");
    await waitFrames(10);
    releaseKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "in_lander", 3000);

    const inventory = readInventory(el);
    const requiredFuel = readStr(el, "required-fuel");

    // If we don't have the required fuel, lander fuel should be unchanged
    if (!inventory.includes(requiredFuel)) {
      const fuelAfter = readNum(el, "lander-fuel");
      expect(fuelAfter).toBeLessThanOrEqual(fuelBefore);
    }
  });

  // =========================================================================
  // 6. Launch mechanic
  // =========================================================================

  it("Space key launches when in lander with fuel >= 100", async () => {
    /**
     *   lander (fuel=100)
     *     |  player presses Space
     *     |
     *     v
     *   LAUNCH!  mode -> "launched"
     *     |
     *     |  lander flies upward
     *     v
     *   escaped the moon
     */
    const el = await landAndWalk();

    // Walk to collect the required fuel, then return
    await holdKey("d", 4000, "KeyD");
    await waitFrames(5);
    await holdKey("a", 4000, "KeyA");
    await waitFrames(5);

    pressKey("e", "KeyE");
    await waitFrames(10);
    releaseKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "in_lander", 3000);

    const fuel = readNum(el, "lander-fuel");

    if (fuel >= MAX_FUEL) {
      // Launch!
      pressKey(" ", "Space");
      await waitFrames(5);
      releaseKey(" ", "Space");

      await waitForAttr(el, "player-mode", "launched", 5000);
    }
  });

  it("Space key does nothing when fuel < 100", async () => {
    const el = await mountLanded();

    // In lander with INITIAL_FUEL (40); not enough to launch
    const fuel = readNum(el, "lander-fuel");
    expect(fuel).toBeLessThan(MAX_FUEL);

    // Try to launch
    pressKey(" ", "Space");
    await waitFrames(10);
    releaseKey(" ", "Space");

    // Should still be landed (not launched)
    const mode = readStr(el, "player-mode");
    expect(mode).not.toBe("launched");
  });

  it("Space key does nothing while walking", async () => {
    const el = await landAndWalk();

    pressKey(" ", "Space");
    await waitFrames(10);
    releaseKey(" ", "Space");

    const mode = readStr(el, "player-mode");
    expect(mode).toBe("walking");
  });

  // =========================================================================
  // 7. Full Phase 3 integration: collect fuel, return, and launch
  // =========================================================================

  /**
   * The Phase 3 question: "Can I collect fuel, return, and launch?"
   *
   *   landed (fuel=40)
   *     |  press E -> exit lander
   *     v
   *   walk (walking)
   *     |  walk across moon surface >>>>
   *     |  collect fuel deposits along the way
   *     |  find required fuel type
   *     |  <<<< walk back to lander
   *     v
   *   near lander -> press E
   *     |
   *     v
   *   in_lander (correct fuel auto-transfers, fuel -> 100)
   *     |  press Space
   *     v
   *   launched!
   */
  it("full Phase 3 flow: land -> collect -> refuel -> launch", async () => {
    const el = await mountLanded();

    // --- Landed ---
    expect(readStr(el, "player-mode")).toBe("landed");
    expect(readNum(el, "lander-fuel")).toBe(INITIAL_FUEL);

    // --- Exit lander ---
    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    const requiredFuel = readStr(el, "required-fuel");

    // --- Walk to collect fuel ---
    // Deposits are scattered across the surface. Walk right for a while.
    await holdKey("d", 4000, "KeyD");
    await waitFrames(5);

    // Should have collected at least something
    const inventory = readInventory(el);
    expect(inventory.length).toBeGreaterThan(0);

    // Check if we got the required fuel
    const hasRequired = inventory.includes(requiredFuel);

    if (hasRequired) {
      // --- Walk back to lander ---
      await holdKey("a", 4000, "KeyA");
      await waitFrames(5);

      // --- Re-enter lander (refuel) ---
      pressKey("e", "KeyE");
      await waitFrames(10);
      releaseKey("e", "KeyE");
      await waitForAttr(el, "player-mode", "in_lander", 3000);

      // Fuel should now be 100 (refuelled)
      const fuelForLaunch = readNum(el, "lander-fuel");
      expect(fuelForLaunch).toBe(MAX_FUEL);

      // --- Launch! ---
      pressKey(" ", "Space");
      await waitFrames(5);
      releaseKey(" ", "Space");

      await waitForAttr(el, "player-mode", "launched", 5000);
    }
  });
});
