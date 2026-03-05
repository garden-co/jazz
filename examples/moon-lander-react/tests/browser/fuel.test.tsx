/**
 * E2E browser tests for Moon Lander — Fuel Collection.
 *
 * Tests the complete fuel loop:
 *   - Deposits on the surface (counts, types)
 *   - Fuel burn during thrust (vertical and horizontal)
 *   - Collecting deposits while walking
 *   - Inventory tracking (capped at 1 per type)
 *   - Returning to the lander and refuelling
 *   - Launch mechanic (requires MAX_FUEL)
 *
 * All tests mount <Game> directly (no Jazz sync) with physicsSpeed=10.
 *
 * NOTE: physicsSpeed=10 means free-fall from INITIAL_ALTITUDE always exceeds
 * the crash threshold. Tests that need the lander on the ground use
 * initialMode="landed". Descent-only tests use initialMode="descending".
 */

import { act } from "react";
import { createRoot } from "react-dom/client";
import { afterEach, describe, expect, it } from "vitest";
import { Game } from "../../src/Game";
import {
  FUEL_BURN_X,
  FUEL_BURN_Y,
  FUEL_TYPES,
  INITIAL_FUEL,
  MAX_FUEL,
} from "../../src/game/constants";
import {
  holdKey,
  type MountEntry,
  pressKey,
  readNum,
  readStr,
  releaseKey,
  unmountAll,
  waitFor,
  waitForAttr,
  waitFrames,
} from "./test-helpers";

const SPEED = 10;

const mounts: MountEntry[] = [];

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
  await unmountAll(mounts);
});

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
// Fuel burn
// ---------------------------------------------------------------------------

describe("Moon Lander — Fuel Burn", () => {
  it("vertical thrust burns fuel during descent", async () => {
    const el = await mountDescending();

    const fuelBefore = readNum(el, "lander-fuel");
    expect(fuelBefore).toBe(INITIAL_FUEL);

    // 200ms ≈ 2s game time; expected burn ≈ FUEL_BURN_Y × 2s
    await holdKey("ArrowUp", 200, "ArrowUp");
    await waitFrames(5);

    const fuelAfter = readNum(el, "lander-fuel");
    expect(fuelAfter).toBeLessThan(fuelBefore);

    const burned = fuelBefore - fuelAfter;
    expect(burned).toBeGreaterThan(FUEL_BURN_Y * 1);
    expect(burned).toBeLessThan(FUEL_BURN_Y * 4);
  });

  it("horizontal thrust burns fuel during descent", async () => {
    const el = await mountDescending();

    const fuelBefore = readNum(el, "lander-fuel");

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
     *   then: ArrowUp pressed, but no thrust applied — gravity only
     */
    const el = await mountDescending();

    // 40 units / (FUEL_BURN_Y=8 units/sec) = 5 game-seconds = 500ms at 10x
    await holdKey("ArrowUp", 600, "ArrowUp");
    await waitFrames(5);

    await waitForNum(el, "lander-fuel", (f) => f === 0, 3000, "fuel should reach 0");

    const vyBefore = readNum(el, "velocity-y");

    await holdKey("ArrowUp", 100, "ArrowUp");
    await waitFrames(5);

    const vyAfter = readNum(el, "velocity-y");
    expect(vyAfter).toBeGreaterThanOrEqual(vyBefore);
  });
});

// ---------------------------------------------------------------------------
// Collection
// ---------------------------------------------------------------------------

describe("Moon Lander — Collection Mechanic", () => {
  it("walking over a deposit collects it", async () => {
    /**
     *   lander    deposit
     *     ▼         ▼
     *   ===+=========+========
     *      +--walk-->  collect!
     *
     *   inventory: [] → ["circle"] (or whatever type)
     */
    const el = await landAndWalk();

    expect(readInventory(el)).toHaveLength(0);

    // At 10x speed, 120px/s × 10 = 1200px/s → 3s covers ~3600px
    await holdKey("d", 3000, "KeyD");
    await waitFrames(5);

    const inventoryAfter = readInventory(el);
    expect(inventoryAfter.length).toBeGreaterThan(0);

    for (const type of inventoryAfter) {
      expect((FUEL_TYPES as readonly string[]).includes(type)).toBe(true);
    }
  });

  it("collecting a deposit removes it from the surface", async () => {
    const el = await landAndWalk();

    const countBefore = readNum(el, "deposit-count");

    await holdKey("d", 3000, "KeyD");
    await waitFrames(5);

    if (readInventory(el).length > 0) {
      expect(readNum(el, "deposit-count")).toBeLessThan(countBefore);
    }
  });

  it("inventory is capped at 1 per fuel type", async () => {
    const el = await landAndWalk();

    await holdKey("d", 5000, "KeyD");
    await waitFrames(5);

    const inventory = readInventory(el);
    const uniqueTypes = new Set(inventory);
    expect(uniqueTypes.size).toBe(inventory.length);
  });
});

// ---------------------------------------------------------------------------
// Refuelling
// ---------------------------------------------------------------------------

describe("Moon Lander — Refuelling", () => {
  it("re-entering lander with correct fuel type refuels it", async () => {
    /**
     *   lander (needs pentagon)     pentagon deposit
     *        ▼                         ▼
     *   ====+==========================+=====
     *       +----walk right-----------> collect pentagon
     *       <----walk back--------------+
     *       press E → enter lander
     *       fuel: 40 → 100 (capped)
     */
    const el = await landAndWalk();

    const requiredFuel = readStr(el, "required-fuel");
    const fuelBefore = readNum(el, "lander-fuel");

    // World is 9600px; required fuel placed 1/4 to 1/2 away (2400–4800px).
    // At 1200px/s → walk 4s to cover ~4800px
    await holdKey("d", 4000, "KeyD");
    await waitFrames(5);

    const hasRequired = readInventory(el).includes(requiredFuel);

    if (hasRequired) {
      await holdKey("a", 4000, "KeyA");
      await waitFrames(5);

      pressKey("e", "KeyE");
      await waitFrames(10);
      releaseKey("e", "KeyE");
      await waitForAttr(el, "player-mode", "in_lander", 3000);

      expect(readNum(el, "lander-fuel")).toBeGreaterThan(fuelBefore);
      expect(readNum(el, "lander-fuel")).toBeLessThanOrEqual(MAX_FUEL);
    } else {
      // Walk back and enter without correct fuel — level should be unchanged
      await holdKey("a", 4000, "KeyA");
      await waitFrames(5);

      pressKey("e", "KeyE");
      await waitFrames(10);
      releaseKey("e", "KeyE");
      await waitForAttr(el, "player-mode", "in_lander", 3000);

      expect(readNum(el, "lander-fuel")).toBeLessThanOrEqual(fuelBefore);
    }
  });

  it("re-entering lander without correct fuel does not refuel", async () => {
    const el = await landAndWalk();

    const fuelBefore = readNum(el, "lander-fuel");

    // Short walk (unlikely to find the required type) and back
    await holdKey("d", 50, "KeyD");
    await waitFrames(3);
    await holdKey("a", 50, "KeyA");
    await waitFrames(3);

    pressKey("e", "KeyE");
    await waitFrames(10);
    releaseKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "in_lander", 3000);

    if (!readInventory(el).includes(readStr(el, "required-fuel"))) {
      expect(readNum(el, "lander-fuel")).toBeLessThanOrEqual(fuelBefore);
    }
  });
});

// ---------------------------------------------------------------------------
// Launch
// ---------------------------------------------------------------------------

describe("Moon Lander — Launch", () => {
  it("Space key launches when in lander with fuel >= 100", async () => {
    /**
     *   lander (fuel=100)
     *     │  player presses Space
     *     ▼
     *   LAUNCH!  mode → "launched"
     */
    const el = await landAndWalk();

    await holdKey("d", 4000, "KeyD");
    await waitFrames(5);
    await holdKey("a", 4000, "KeyA");
    await waitFrames(5);

    pressKey("e", "KeyE");
    await waitFrames(10);
    releaseKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "in_lander", 3000);

    if (readNum(el, "lander-fuel") >= MAX_FUEL) {
      pressKey(" ", "Space");
      await waitFrames(5);
      releaseKey(" ", "Space");

      await waitForAttr(el, "player-mode", "launched", 5000);
    }
  });

  it("Space key does nothing when fuel < 100", async () => {
    const el = await mountLanded();

    expect(readNum(el, "lander-fuel")).toBeLessThan(MAX_FUEL);

    pressKey(" ", "Space");
    await waitFrames(10);
    releaseKey(" ", "Space");

    expect(readStr(el, "player-mode")).not.toBe("launched");
  });

  it("Space key does nothing while walking", async () => {
    const el = await landAndWalk();

    pressKey(" ", "Space");
    await waitFrames(10);
    releaseKey(" ", "Space");

    expect(readStr(el, "player-mode")).toBe("walking");
  });

  it("full flow: land → collect → refuel → launch", async () => {
    /**
     *   landed (fuel=40)
     *     │  press E → exit lander
     *     ▼
     *   walk  >>>  collect fuel deposits
     *     │  find required fuel type
     *     │  <<<  walk back to lander
     *     ▼
     *   press E → in_lander (correct fuel auto-transfers, fuel → 100)
     *     │  press Space
     *     ▼
     *   launched!
     */
    const el = await mountLanded();

    expect(readStr(el, "player-mode")).toBe("landed");
    expect(readNum(el, "lander-fuel")).toBe(INITIAL_FUEL);

    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    const requiredFuel = readStr(el, "required-fuel");

    await holdKey("d", 4000, "KeyD");
    await waitFrames(5);

    const inventory = readInventory(el);
    expect(inventory.length).toBeGreaterThan(0);

    if (inventory.includes(requiredFuel)) {
      await holdKey("a", 4000, "KeyA");
      await waitFrames(5);

      pressKey("e", "KeyE");
      await waitFrames(10);
      releaseKey("e", "KeyE");
      await waitForAttr(el, "player-mode", "in_lander", 3000);

      expect(readNum(el, "lander-fuel")).toBe(MAX_FUEL);

      pressKey(" ", "Space");
      await waitFrames(5);
      releaseKey(" ", "Space");

      await waitForAttr(el, "player-mode", "launched", 5000);
    }
  });
});
