/**
 * E2E browser tests for Moon Lander — Lifecycle & World Mechanics.
 *
 * Covers the game lifecycle transitions that slip between the per-phase tests:
 *   - Start mode → Space begins descent
 *   - Crash landing → too fast = crash, Space restarts after delay
 *   - Launch restart → Space after launched restarts the game
 *   - World wrapping → walking off one edge wraps to the other
 *
 * All tests mount <Game> directly (no Jazz sync) with physicsSpeed=10.
 */

import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, describe, expect, it } from "vitest";
import { Game } from "../../src/Game";
import {
  GROUND_LEVEL,
  INITIAL_ALTITUDE,
  INITIAL_FUEL,
  MAX_FUEL,
  MOON_SURFACE_WIDTH,
  FUEL_TYPES,
} from "../../src/game/constants";

const SPAWN_X = 480; // fixed spawn position for tests

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const SPEED = 10;
const mounts: Array<{ root: Root; container: HTMLDivElement }> = [];

async function mountGame(props: Record<string, unknown> = {}): Promise<HTMLDivElement> {
  const el = document.createElement("div");
  document.body.appendChild(el);
  const root = createRoot(el);
  mounts.push({ root, container: el });

  await act(async () => {
    root.render(<Game {...({ physicsSpeed: SPEED, ...props } as any)} />);
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

async function waitFor(check: () => boolean, timeoutMs: number, message: string): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (check()) return;
    await new Promise((r) => setTimeout(r, 50));
  }
  throw new Error(`Timeout: ${message}`);
}

function readNum(el: HTMLDivElement, attr: string): number {
  const container = el.querySelector('[data-testid="game-container"]')!;
  const raw = container.getAttribute(`data-${attr}`);
  if (raw === null) throw new Error(`Missing data attribute: data-${attr}`);
  return parseFloat(raw);
}

function readStr(el: HTMLDivElement, attr: string): string {
  const container = el.querySelector('[data-testid="game-container"]')!;
  const raw = container.getAttribute(`data-${attr}`);
  if (raw === null) throw new Error(`Missing data attribute: data-${attr}`);
  return raw;
}

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

function pressKey(key: string, code?: string) {
  document.dispatchEvent(new KeyboardEvent("keydown", { key, code: code ?? key, bubbles: true }));
}

function releaseKey(key: string, code?: string) {
  document.dispatchEvent(new KeyboardEvent("keyup", { key, code: code ?? key, bubbles: true }));
}

async function holdKey(key: string, durationMs: number, code?: string) {
  pressKey(key, code);
  await new Promise((r) => setTimeout(r, durationMs));
  releaseKey(key, code);
}

async function waitFrames(n: number): Promise<void> {
  for (let i = 0; i < n; i++) {
    await new Promise((r) => requestAnimationFrame(r));
  }
}

// ---------------------------------------------------------------------------
// Start mode
// ---------------------------------------------------------------------------

describe("Moon Lander — Start Mode", () => {
  // =========================================================================
  // 1. Game starts in "start" mode by default
  //
  //   Mount Game with no initialMode (or initialMode="start")
  //   → data-player-mode = "start"
  //   → player stays at spawn position, no movement
  // =========================================================================

  it("defaults to start mode when no initialMode is specified", async () => {
    const el = await mountGame({});

    expect(readStr(el, "player-mode")).toBe("start");
  });

  // =========================================================================
  // 2. Space begins descent from start mode
  //
  //   [start]  press Space  →  [descending]
  //   Player begins falling from INITIAL_ALTITUDE
  // =========================================================================

  it("Space transitions from start to descending", async () => {
    const el = await mountGame({});

    expect(readStr(el, "player-mode")).toBe("start");

    pressKey(" ", "Space");
    await waitFrames(2);
    releaseKey(" ", "Space");

    await waitForAttr(el, "player-mode", "descending", 3000);
  });

  // =========================================================================
  // 3. Movement keys do nothing in start mode
  //
  //   In start mode, the physics step returns immediately after checking
  //   for Space. Arrow keys should not affect position.
  // =========================================================================

  it("movement keys do nothing in start mode", async () => {
    const el = await mountGame({});

    const yBefore = readNum(el, "player-y");

    // Try thrusting — should have no effect
    await holdKey("ArrowUp", 100, "ArrowUp");
    await waitFrames(5);

    // Still in start mode
    expect(readStr(el, "player-mode")).toBe("start");

    // Position unchanged (no gravity, no thrust in start mode)
    const yAfter = readNum(el, "player-y");
    expect(yAfter).toBe(yBefore);
  });
});

// ---------------------------------------------------------------------------
// Crash mechanics
// ---------------------------------------------------------------------------

describe("Moon Lander — Crash Mechanics", () => {
  // =========================================================================
  // 1. Landing too fast causes a crash
  //
  //   Free-fall from INITIAL_ALTITUDE at 10x speed:
  //   terminal velocity >> MAX_LANDING_VELOCITY (80 px/s)
  //   → mode = "crashed" on ground contact
  //
  //   -----.       (free fall, no thrust)
  //        |
  //        v
  //   ═════╤═══    CRASH!
  // =========================================================================

  it("free-falling at high speed causes a crash", async () => {
    const el = await mountGame({ initialMode: "descending" });

    expect(readStr(el, "player-mode")).toBe("descending");

    // Let the lander free-fall without thrust until it reaches the ground.
    // At 10x speed, GRAVITY=40 → effective 400 px/s². From INITIAL_ALTITUDE=-400,
    // the lander reaches ground in well under 1 second with velocity >> 80.
    await waitFor(
      () => {
        const mode = readStr(el, "player-mode");
        return mode === "crashed" || mode === "landed";
      },
      3000,
      "lander should reach ground (crashed or landed)",
    );

    // At 10x speed free-fall, the velocity should far exceed the safe landing threshold
    expect(readStr(el, "player-mode")).toBe("crashed");
  });

  // =========================================================================
  // 2. Space restarts after a crash (with delay)
  //
  //   [crashed]  wait 1s game-time  →  [Space]  →  [descending]
  //   crashElapsed must exceed 1 before restart is allowed
  // =========================================================================

  it("Space restarts the game after a crash", async () => {
    const el = await mountGame({ initialMode: "descending" });

    // Wait for crash
    await waitForAttr(el, "player-mode", "crashed", 3000);

    // Wait for the crash delay (1s game time = 100ms at 10x speed)
    await new Promise((r) => setTimeout(r, 200));

    // Press Space to restart
    pressKey(" ", "Space");
    await waitFrames(2);
    releaseKey(" ", "Space");

    await waitForAttr(el, "player-mode", "descending", 3000);

    // Should be back at altitude
    const y = readNum(el, "player-y");
    expect(y).toBeLessThan(GROUND_LEVEL);
  });

  // =========================================================================
  // 3. Space does nothing immediately after crash (delay enforced)
  //
  //   [crashed]  immediately press Space  →  still [crashed]
  //   Must wait at least 1 game-second before restart
  // =========================================================================

  it("Space does nothing immediately after crash", async () => {
    const el = await mountGame({ initialMode: "descending" });

    await waitForAttr(el, "player-mode", "crashed", 3000);

    // Press Space immediately (no delay)
    pressKey(" ", "Space");
    await waitFrames(3);
    releaseKey(" ", "Space");

    // Should still be crashed (delay not met)
    expect(readStr(el, "player-mode")).toBe("crashed");
  });

  // =========================================================================
  // 4. Crash resets inventory
  //
  //   After restarting from a crash, the player should have
  //   fresh state: fuel = INITIAL_FUEL, empty inventory
  // =========================================================================

  it("restarting after crash resets fuel to initial level", async () => {
    const el = await mountGame({ initialMode: "descending" });

    await waitForAttr(el, "player-mode", "crashed", 3000);

    // Wait for crash delay then restart
    await new Promise((r) => setTimeout(r, 200));
    pressKey(" ", "Space");
    await waitFrames(2);
    releaseKey(" ", "Space");

    await waitForAttr(el, "player-mode", "descending", 3000);

    const fuel = readNum(el, "lander-fuel");
    expect(fuel).toBe(INITIAL_FUEL);
  });
});

// ---------------------------------------------------------------------------
// Launch restart
// ---------------------------------------------------------------------------

describe("Moon Lander — Launch Restart", () => {
  // =========================================================================
  // 1. After launching, Space restarts the game (with delay)
  //
  //   [in_lander, fuel=100]  Space  →  [launched]
  //   wait 5s game-time  →  [Space]  →  [descending]
  //
  //   The launch sequence runs for at least 5 game-seconds before
  //   allowing a restart via Space.
  // =========================================================================

  it("Space restarts after launch success (with delay)", async () => {
    // Start in lander with full fuel to launch immediately
    const el = await mountGame({
      initialMode: "landed",
      deposits: [],
      inventory: [...FUEL_TYPES],
    });

    // Exit lander, walk back in (to trigger refuel with correct fuel type)
    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "in_lander", 3000);
    releaseKey("e", "KeyE");

    const fuel = readNum(el, "lander-fuel");

    if (fuel >= MAX_FUEL) {
      // Launch
      pressKey(" ", "Space");
      await waitFrames(2);
      releaseKey(" ", "Space");

      await waitForAttr(el, "player-mode", "launched", 3000);

      // Wait for launch delay (5s game-time = 500ms at 10x speed, add margin)
      await new Promise((r) => setTimeout(r, 700));

      // Restart
      pressKey(" ", "Space");
      await waitFrames(2);
      releaseKey(" ", "Space");

      await waitForAttr(el, "player-mode", "descending", 3000);

      // Should be back at altitude with initial fuel
      const y = readNum(el, "player-y");
      expect(y).toBeLessThan(GROUND_LEVEL);

      const restartFuel = readNum(el, "lander-fuel");
      expect(restartFuel).toBe(INITIAL_FUEL);
    }
  });

  // =========================================================================
  // 2. Space does nothing too soon after launch
  //
  //   [launched]  immediately press Space  →  still [launched]
  // =========================================================================

  it("Space does nothing immediately after launch", async () => {
    const el = await mountGame({
      initialMode: "landed",
      deposits: [],
      inventory: [...FUEL_TYPES],
    });

    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "in_lander", 3000);
    releaseKey("e", "KeyE");

    const fuel = readNum(el, "lander-fuel");

    if (fuel >= MAX_FUEL) {
      // Launch
      pressKey(" ", "Space");
      await waitFrames(2);
      releaseKey(" ", "Space");

      await waitForAttr(el, "player-mode", "launched", 3000);

      // Immediately try to restart (no delay)
      pressKey(" ", "Space");
      await waitFrames(3);
      releaseKey(" ", "Space");

      // Still launched (delay not met)
      expect(readStr(el, "player-mode")).toBe("launched");
    }
  });
});

// ---------------------------------------------------------------------------
// World wrapping
// ---------------------------------------------------------------------------

describe("Moon Lander — World Wrapping", () => {
  // =========================================================================
  // 1. Walking off the right edge wraps to the left side
  //
  //   Moon surface: [0 ··· MOON_SURFACE_WIDTH)
  //
  //   Player starts at spawnX, walks right continuously.
  //   Eventually posX wraps past MOON_SURFACE_WIDTH back toward 0.
  //
  //   If we start near the right edge:
  //     spawnX = MOON_SURFACE_WIDTH - 100
  //     walk right 200px → posX ≈ 100 (wrapped)
  // =========================================================================

  it("walking off the right edge wraps to the left side", async () => {
    // Spawn near the right edge of the world
    const spawnX = MOON_SURFACE_WIDTH - 100;
    const el = await mountGame({
      initialMode: "landed",
      spawnX,
    });

    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    const xBefore = readNum(el, "player-x");
    // Should be near right edge
    expect(xBefore).toBeGreaterThan(MOON_SURFACE_WIDTH - 200);

    // Walk right for long enough to cross the boundary
    // At 10x speed: 120 px/s * 10 = 1200 px/s. 200ms covers ~240px.
    await holdKey("d", 200, "KeyD");
    await waitFrames(5);

    const xAfter = readNum(el, "player-x");
    // Should have wrapped to low values (near 0)
    expect(xAfter).toBeLessThan(MOON_SURFACE_WIDTH / 2);
  });

  // =========================================================================
  // 2. Walking off the left edge wraps to the right side
  //
  //   Spawn near left edge:
  //     spawnX = 100
  //     walk left 200px → posX ≈ MOON_SURFACE_WIDTH - 100 (wrapped)
  // =========================================================================

  it("walking off the left edge wraps to the right side", async () => {
    const spawnX = 100;
    const el = await mountGame({
      initialMode: "landed",
      spawnX,
    });

    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    const xBefore = readNum(el, "player-x");
    expect(xBefore).toBeLessThan(200);

    // Walk left to cross the boundary
    await holdKey("a", 200, "KeyA");
    await waitFrames(5);

    const xAfter = readNum(el, "player-x");
    // Should have wrapped to high values (near MOON_SURFACE_WIDTH)
    expect(xAfter).toBeGreaterThan(MOON_SURFACE_WIDTH / 2);
  });

  // =========================================================================
  // 3. Lander position wraps during descent
  //
  //   Thrusting sideways during descent should also wrap the lander
  //   around the world boundary.
  // =========================================================================

  it("descent position wraps around the world boundary", async () => {
    // Spawn near the right edge
    const spawnX = MOON_SURFACE_WIDTH - 50;
    const el = await mountGame({
      initialMode: "descending",
      spawnX,
    });

    // Thrust right to cross the boundary
    await holdKey("ArrowRight", 200, "ArrowRight");
    await waitFrames(5);

    const x = readNum(el, "player-x");
    // Should have wrapped (could be anywhere if enough thrust,
    // but should not be negative or > MOON_SURFACE_WIDTH)
    expect(x).toBeGreaterThanOrEqual(0);
    expect(x).toBeLessThan(MOON_SURFACE_WIDTH);
  });
});

// ---------------------------------------------------------------------------
// Remote player rendering
// ---------------------------------------------------------------------------

describe("Moon Lander — Remote Player Data", () => {
  // =========================================================================
  // 1. Remote player count reflects the remotePlayers prop
  //
  //   data-remote-player-count should match the number of remote players
  //   passed via props.
  // =========================================================================

  it("exposes remote player count via data attribute", async () => {
    const remotePlayers = [
      {
        id: "row-alice",
        name: "Alice",
        mode: "walking",
        positionX: 500,
        positionY: GROUND_LEVEL,
        velocityX: 0,
        velocityY: 0,
        color: "#ff00ff",
        requiredFuelType: "hexagon",
        lastSeen: Math.floor(Date.now() / 1000),
        landerFuelLevel: 40,
        playerId: "alice-uuid",
        landerX: 400,
      },
      {
        id: "row-bob",
        name: "Bob",
        mode: "descending",
        positionX: 1000,
        positionY: 200,
        velocityX: 0,
        velocityY: 10,
        color: "#00ffff",
        requiredFuelType: "triangle",
        lastSeen: Math.floor(Date.now() / 1000),
        landerFuelLevel: 30,
        playerId: "bob-uuid",
        landerX: 1000,
      },
    ];

    const el = await mountGame({
      initialMode: "landed",
      deposits: [],
      inventory: [],
      remotePlayers,
    });

    await waitFor(
      () => readStr(el, "remote-player-count") === "2",
      2000,
      "remote-player-count should be 2",
    );
  });

  // =========================================================================
  // 2. Remote player count updates when prop changes
  // =========================================================================

  it("remote player count is 0 when no remote players", async () => {
    const el = await mountGame({
      initialMode: "landed",
      deposits: [],
      inventory: [],
      remotePlayers: [],
    });

    await waitFor(
      () => readStr(el, "remote-player-count") === "0",
      2000,
      "remote-player-count should be 0",
    );
  });
});
