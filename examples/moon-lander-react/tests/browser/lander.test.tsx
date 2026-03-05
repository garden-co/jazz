/**
 * E2E browser tests for Moon Lander — Lander Flight & Landing.
 *
 * Covers the descent and landing cycle:
 *   - Canvas rendering and initial descending state
 *   - Gravity and thrust (vertical and horizontal)
 *   - Crash landing (too fast) and recovery
 *   - Successful launch and post-launch restart
 *
 * All tests mount <Game> directly (no Jazz sync) with physicsSpeed=10.
 */

import { act } from "react";
import { createRoot } from "react-dom/client";
import { afterEach, describe, expect, it } from "vitest";
import { Game } from "../../src/Game";
import {
  FUEL_TYPES,
  GROUND_LEVEL,
  INITIAL_ALTITUDE,
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
/** Fixed spawn position (mid-world to avoid wrapping edge cases). */
const SPAWN_X = 4800;

const mounts: MountEntry[] = [];

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

  await new Promise((r) => requestAnimationFrame(r));
  return el;
}

afterEach(async () => {
  await unmountAll(mounts);
});

// ---------------------------------------------------------------------------
// Descent
// ---------------------------------------------------------------------------

describe("Moon Lander — Descent", () => {
  it("renders a game canvas", async () => {
    const el = await mountGame({ initialMode: "descending", spawnX: SPAWN_X });
    const canvas = el.querySelector<HTMLCanvasElement>('[data-testid="game-canvas"]');
    expect(canvas).toBeTruthy();
    expect(canvas!.width).toBeGreaterThan(0);
    expect(canvas!.height).toBeGreaterThan(0);
  });

  it("player starts in descending mode above the moon", async () => {
    const el = await mountGame({ initialMode: "descending", spawnX: SPAWN_X });

    expect(readStr(el, "player-mode")).toBe("descending");

    const y = readNum(el, "player-y");
    expect(y).toBeLessThan(GROUND_LEVEL);
    expect(y).toBeCloseTo(INITIAL_ALTITUDE, -1);
  });

  it("lander descends under gravity", async () => {
    const el = await mountGame({ initialMode: "descending", spawnX: SPAWN_X });

    const y0 = readNum(el, "player-y");
    // At 10x speed, 100ms ≈ 1s game time
    await new Promise((r) => setTimeout(r, 100));
    const y1 = readNum(el, "player-y");
    expect(y1).toBeGreaterThan(y0);
  });

  it("upward thrust reduces descent speed", async () => {
    const el = await mountGame({ initialMode: "descending", spawnX: SPAWN_X });

    // Let gravity build downward velocity
    await new Promise((r) => setTimeout(r, 100));
    const vy0 = readNum(el, "velocity-y");
    expect(vy0).toBeGreaterThan(0);

    // Apply upward thrust
    await holdKey("ArrowUp", 50, "ArrowUp");
    await waitFrames(5);

    const vy1 = readNum(el, "velocity-y");
    expect(vy1).toBeLessThan(vy0);
  });

  it("horizontal thrust moves the lander sideways", async () => {
    const el = await mountGame({ initialMode: "descending", spawnX: SPAWN_X });
    await waitFrames(5);

    const x0 = readNum(el, "player-x");
    await holdKey("ArrowRight", 50, "ArrowRight");
    await waitFrames(5);

    const x1 = readNum(el, "player-x");
    expect(x1).toBeGreaterThan(x0);
  });
});

// ---------------------------------------------------------------------------
// Crash mechanics
// ---------------------------------------------------------------------------

describe("Moon Lander — Crash Mechanics", () => {
  /**
   * Free-fall from INITIAL_ALTITUDE at 10x speed:
   * terminal velocity >> MAX_LANDING_VELOCITY (80 px/s)
   *
   *   -----.       (free fall, no thrust)
   *        |
   *        v
   *   ═════╤═══    CRASH!
   */
  it("free-falling at high speed causes a crash", async () => {
    const el = await mountGame({ initialMode: "descending" });

    await waitFor(
      () => {
        const mode = readStr(el, "player-mode");
        return mode === "crashed" || mode === "landed";
      },
      3000,
      "lander should reach ground",
    );

    expect(readStr(el, "player-mode")).toBe("crashed");
  });

  it("Space restarts the game after a crash", async () => {
    const el = await mountGame({ initialMode: "descending" });

    await waitForAttr(el, "player-mode", "crashed", 3000);

    // Wait for the crash delay (1s game time = 100ms at 10x speed)
    await new Promise((r) => setTimeout(r, 200));

    pressKey(" ", "Space");
    await waitFrames(2);
    releaseKey(" ", "Space");

    await waitForAttr(el, "player-mode", "descending", 3000);

    expect(readNum(el, "player-y")).toBeLessThan(GROUND_LEVEL);
  });

  it("Space does nothing immediately after crash", async () => {
    const el = await mountGame({ initialMode: "descending" });

    await waitForAttr(el, "player-mode", "crashed", 3000);

    // Press Space before the delay elapses
    pressKey(" ", "Space");
    await waitFrames(3);
    releaseKey(" ", "Space");

    expect(readStr(el, "player-mode")).toBe("crashed");
  });

  it("restarting after crash resets fuel to initial level", async () => {
    const el = await mountGame({ initialMode: "descending" });

    await waitForAttr(el, "player-mode", "crashed", 3000);

    await new Promise((r) => setTimeout(r, 200));
    pressKey(" ", "Space");
    await waitFrames(2);
    releaseKey(" ", "Space");

    await waitForAttr(el, "player-mode", "descending", 3000);

    expect(readNum(el, "lander-fuel")).toBe(INITIAL_FUEL);
  });
});

// ---------------------------------------------------------------------------
// Launch mechanics
// ---------------------------------------------------------------------------

describe("Moon Lander — Launch Mechanics", () => {
  /**
   *   [in_lander, fuel=MAX_FUEL]  Space  →  [launched]
   *   wait 5s game-time  →  [Space]  →  [descending]
   */
  it("Space restarts after launch (with delay)", async () => {
    const el = await mountGame({
      initialMode: "landed",
      deposits: [],
      inventory: [...FUEL_TYPES],
    });

    // Exit and re-enter to trigger refuel
    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "in_lander", 3000);
    releaseKey("e", "KeyE");

    expect(readNum(el, "lander-fuel")).toBe(MAX_FUEL);

    pressKey(" ", "Space");
    await waitFrames(2);
    releaseKey(" ", "Space");

    await waitForAttr(el, "player-mode", "launched", 3000);

    // Wait for launch delay (5s game-time = 500ms at 10x speed, plus margin)
    await new Promise((r) => setTimeout(r, 700));

    pressKey(" ", "Space");
    await waitFrames(2);
    releaseKey(" ", "Space");

    await waitForAttr(el, "player-mode", "descending", 3000);

    expect(readNum(el, "player-y")).toBeLessThan(GROUND_LEVEL);
    expect(readNum(el, "lander-fuel")).toBe(INITIAL_FUEL);
  });

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

    expect(readNum(el, "lander-fuel")).toBe(MAX_FUEL);

    pressKey(" ", "Space");
    await waitFrames(2);
    releaseKey(" ", "Space");

    await waitForAttr(el, "player-mode", "launched", 3000);

    // Immediately try to restart
    pressKey(" ", "Space");
    await waitFrames(3);
    releaseKey(" ", "Space");

    expect(readStr(el, "player-mode")).toBe("launched");
  });
});
