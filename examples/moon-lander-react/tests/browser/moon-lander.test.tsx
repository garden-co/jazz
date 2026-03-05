/**
 * E2E browser tests for Moon Lander — Phase 1: Solo Landing & Walking.
 *
 * Mounts the real Game component in Chromium via @vitest/browser + Playwright.
 * Interacts through keyboard events and reads game state from data attributes
 * on [data-testid="game-container"].
 *
 * Data attribute contract (set by Game component):
 *   data-player-mode   — "descending" | "landed" | "walking" | "in_lander" | "launched"
 *   data-player-x      — player X position (number as string)
 *   data-player-y      — player Y position (number as string)
 *   data-velocity-y    — player Y velocity (number as string, positive = downward)
 *   data-lander-x      — lander X position (set once landed)
 *   data-lander-y      — lander Y position (always GROUND_LEVEL)
 *
 * NOTE: physicsSpeed=10 means free-fall from INITIAL_ALTITUDE always exceeds the
 * crash threshold (50 px/s). Tests that need the lander on the ground use
 * initialMode="landed" to bypass descent entirely. Descent mechanics (gravity,
 * thrust) are tested separately without requiring a safe landing.
 */

import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, describe, expect, it } from "vitest";
import { Game } from "../../src/Game";
import { GROUND_LEVEL, INITIAL_ALTITUDE, LANDER_INTERACT_RADIUS } from "../../src/game/constants";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Physics speed multiplier — 10x makes descent ~1s instead of ~7s. */
const SPEED = 10;
/** Fixed spawn position (mid-world to avoid wrapping edge cases). */
const SPAWN_X = 4800;

const mounts: Array<{ root: Root; container: HTMLDivElement }> = [];

/** Mount the Game component in descending mode. Returns the wrapper element. */
async function mountDescending(): Promise<HTMLDivElement> {
  const el = document.createElement("div");
  document.body.appendChild(el);
  const root = createRoot(el);
  mounts.push({ root, container: el });

  await act(async () => {
    root.render(
      <Game {...({ physicsSpeed: SPEED, initialMode: "descending", spawnX: SPAWN_X } as any)} />,
    );
  });

  await waitFor(
    () => el.querySelector('[data-testid="game-canvas"]') !== null,
    3000,
    "Game canvas should render",
  );

  await new Promise((r) => requestAnimationFrame(r));
  return el;
}

/** Mount the Game component already landed on the surface. */
async function mountLanded(): Promise<HTMLDivElement> {
  const el = document.createElement("div");
  document.body.appendChild(el);
  const root = createRoot(el);
  mounts.push({ root, container: el });

  await act(async () => {
    root.render(
      <Game {...({ physicsSpeed: SPEED, initialMode: "landed", spawnX: SPAWN_X } as any)} />,
    );
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

// ---------------------------------------------------------------------------
// Phase 1: Solo Landing & Walking
// ---------------------------------------------------------------------------

describe("Moon Lander — Phase 1: Solo Landing & Walking", () => {
  // -------------------------------------------------------------------------
  // 1. Canvas rendering
  // -------------------------------------------------------------------------

  it("renders a game canvas", async () => {
    const el = await mountDescending();
    const canvas = el.querySelector<HTMLCanvasElement>('[data-testid="game-canvas"]');
    expect(canvas).toBeTruthy();
    expect(canvas!.width).toBeGreaterThan(0);
    expect(canvas!.height).toBeGreaterThan(0);
  });

  // -------------------------------------------------------------------------
  // 2. Initial state (descending)
  // -------------------------------------------------------------------------

  it("player starts in descending mode above the moon", async () => {
    const el = await mountDescending();

    const mode = readStr(el, "player-mode");
    expect(mode).toBe("descending");

    const y = readNum(el, "player-y");
    expect(y).toBeLessThan(GROUND_LEVEL);
    expect(y).toBeCloseTo(INITIAL_ALTITUDE, -1);
  });

  // -------------------------------------------------------------------------
  // 3. Gravity
  // -------------------------------------------------------------------------

  it("lander descends under gravity", async () => {
    const el = await mountDescending();

    const y0 = readNum(el, "player-y");

    // Let the game loop run briefly (at 10x speed, 100ms ~ 1s game time)
    await new Promise((r) => setTimeout(r, 100));

    const y1 = readNum(el, "player-y");
    expect(y1).toBeGreaterThan(y0); // Y increases = moving downward
  });

  // -------------------------------------------------------------------------
  // 4. Thrust
  // -------------------------------------------------------------------------

  it("upward thrust reduces descent speed", async () => {
    const el = await mountDescending();

    // Let gravity build up some downward velocity (100ms ~ 1s game time)
    await new Promise((r) => setTimeout(r, 100));
    const vy0 = readNum(el, "velocity-y");
    expect(vy0).toBeGreaterThan(0); // Falling downward

    // Apply upward thrust (50ms ~ 0.5s game time)
    await holdKey("ArrowUp", 50, "ArrowUp");
    await waitFrames(5);

    const vy1 = readNum(el, "velocity-y");
    expect(vy1).toBeLessThan(vy0); // Thrust should reduce downward velocity
  });

  it("horizontal thrust moves the lander sideways", async () => {
    const el = await mountDescending();
    await waitFrames(5); // Let game initialise

    const x0 = readNum(el, "player-x");

    // Thrust right (50ms ~ 0.5s game time)
    await holdKey("ArrowRight", 50, "ArrowRight");
    await waitFrames(5);

    const x1 = readNum(el, "player-x");
    expect(x1).toBeGreaterThan(x0); // Moved right
  });

  // -------------------------------------------------------------------------
  // 5. Landed state
  // -------------------------------------------------------------------------

  it("landed mode places lander at ground level", async () => {
    const el = await mountLanded();

    expect(readStr(el, "player-mode")).toBe("landed");

    const y = readNum(el, "player-y");
    expect(y).toBeGreaterThanOrEqual(GROUND_LEVEL - 5);
    expect(y).toBeLessThanOrEqual(GROUND_LEVEL);
  });

  // -------------------------------------------------------------------------
  // 7. Ground line
  // -------------------------------------------------------------------------

  it("renders the moon surface", async () => {
    const el = await mountLanded();

    const canvas = el.querySelector<HTMLCanvasElement>('[data-testid="game-canvas"]')!;
    const ctx = canvas.getContext("2d")!;

    // Wait a few frames for the scene to draw with the ground visible
    await waitFrames(5);

    // Sample the bottom portion of the canvas; ground should be visible there.
    const sampleY = canvas.height - 40;
    const imageData = ctx.getImageData(0, sampleY, canvas.width, 2);
    const { data } = imageData;

    let groundPixels = 0;
    for (let i = 0; i < data.length; i += 4) {
      // Ground is #2a1a3a -> RGB(42, 26, 58); distinctly purple-ish
      if (data[i] > 20 && data[i + 2] > 30) {
        groundPixels++;
      }
    }

    expect(groundPixels).toBeGreaterThan(0);
  });

  // -------------------------------------------------------------------------
  // 8. Exit lander
  // -------------------------------------------------------------------------

  it("pressing E after landing exits the lander to walking mode", async () => {
    const el = await mountLanded();

    pressKey("e", "KeyE");
    await waitFrames(5);
    releaseKey("e", "KeyE");

    await waitForAttr(el, "player-mode", "walking", 3000);
  });

  // -------------------------------------------------------------------------
  // 9. Walking
  // -------------------------------------------------------------------------

  it("astronaut walks right with D key", async () => {
    const el = await mountLanded();

    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    const x0 = readNum(el, "player-x");

    // Walk right (50ms ~ 0.5s game time -> ~60px)
    await holdKey("d", 50, "KeyD");
    await waitFrames(5);

    const x1 = readNum(el, "player-x");
    expect(x1).toBeGreaterThan(x0);
  });

  it("astronaut walks left with A key", async () => {
    const el = await mountLanded();

    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    const x0 = readNum(el, "player-x");

    // Walk left (50ms ~ 0.5s game time -> ~60px)
    await holdKey("a", 50, "KeyA");
    await waitFrames(5);

    const x1 = readNum(el, "player-x");
    expect(x1).toBeLessThan(x0);
  });

  // -------------------------------------------------------------------------
  // 10. Lander visibility while walking
  // -------------------------------------------------------------------------

  it("lander remains visible at landing position while walking", async () => {
    const el = await mountLanded();

    // Wait for the engine to sync landerX (first sync is after ~50ms)
    await waitFor(
      () => {
        try {
          return readNum(el, "lander-x") > 0;
        } catch {
          return false;
        }
      },
      2000,
      "lander-x should be set",
    );
    const landerX = readNum(el, "lander-x");

    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    // Walk away (80ms ~ 0.8s game time -> ~96px)
    await holdKey("d", 80, "KeyD");
    await waitFrames(5);

    const landerX2 = readNum(el, "lander-x");
    expect(landerX2).toBe(landerX);

    const playerX = readNum(el, "player-x");
    expect(playerX).not.toBe(landerX);
  });

  // -------------------------------------------------------------------------
  // 11. Re-enter lander
  // -------------------------------------------------------------------------

  it("pressing E near the lander re-enters it", async () => {
    const el = await mountLanded();

    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    // Walk a short distance away (50ms ~ 0.5s -> ~60px)
    await holdKey("d", 50, "KeyD");
    await waitFrames(3);

    // Walk back toward lander (50ms ~ 0.5s -> ~60px, ending near start)
    await holdKey("a", 50, "KeyA");
    await waitFrames(3);

    // Should be near lander; press E to re-enter
    pressKey("e", "KeyE");
    await waitFrames(5);
    releaseKey("e", "KeyE");

    await waitForAttr(el, "player-mode", "in_lander", 3000);
  });

  it("pressing E when far from lander does nothing", async () => {
    const el = await mountLanded();

    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    // Walk far away (200ms ~ 2s game time -> ~240px)
    await holdKey("d", 200, "KeyD");
    await waitFrames(5);

    const playerX = readNum(el, "player-x");
    const landerX = readNum(el, "lander-x");
    expect(Math.abs(playerX - landerX)).toBeGreaterThan(LANDER_INTERACT_RADIUS);

    // Press E; should do nothing
    pressKey("e", "KeyE");
    await waitFrames(10);
    releaseKey("e", "KeyE");

    const mode = readStr(el, "player-mode");
    expect(mode).toBe("walking");
  });

  // -------------------------------------------------------------------------
  // 12. Full flow integration test (landed -> walk -> return -> re-enter)
  // -------------------------------------------------------------------------

  /**
   * The Phase 1 question: "Can I land and walk around?"
   *
   *   landed (on ground)
   *     |  press E
   *     v
   *   exit lander (walking)
   *     |  walk right with D
   *     |  walk left with A (back toward lander)
   *     v
   *   near lander -> press E
   *     |
   *     v
   *   re-enter (in_lander)
   */
  it("full Phase 1 flow: land -> walk -> return -> re-enter", async () => {
    const el = await mountLanded();

    // --- Landed ---
    expect(readStr(el, "player-mode")).toBe("landed");

    // Wait for engine state to sync (landerX takes ~50ms)
    await waitFor(
      () => {
        try {
          return readNum(el, "lander-x") > 0;
        } catch {
          return false;
        }
      },
      2000,
      "lander-x should be set",
    );
    expect(readNum(el, "player-y")).toBeGreaterThanOrEqual(GROUND_LEVEL - 5);

    // --- Exit lander ---
    const landerX = readNum(el, "lander-x");
    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    // --- Walk right (80ms ~ 0.8s -> ~96px) ---
    await holdKey("d", 80, "KeyD");
    await waitFrames(5);
    const walkRightX = readNum(el, "player-x");
    expect(walkRightX).toBeGreaterThan(landerX);

    // Lander hasn't moved
    expect(readNum(el, "lander-x")).toBe(landerX);

    // --- Walk back left toward lander (100ms ~ 1s -> ~120px) ---
    await holdKey("a", 100, "KeyA");
    await waitFrames(5);
    const walkBackX = readNum(el, "player-x");
    expect(walkBackX).toBeLessThan(walkRightX);

    // --- Re-enter lander ---
    // Net movement: ~96px right, ~120px left -> ~24px left of lander -> within radius
    pressKey("e", "KeyE");
    await waitFrames(5);
    releaseKey("e", "KeyE");

    await waitForAttr(el, "player-mode", "in_lander", 3000);
  });
});
