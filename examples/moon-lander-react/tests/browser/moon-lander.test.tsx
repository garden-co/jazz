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
 */

import { describe, it, expect, afterEach } from "vitest";
import { createRoot, type Root } from "react-dom/client";
import { act } from "react";
import { Game } from "../../src/Game.js";
import {
  GROUND_LEVEL,
  INITIAL_ALTITUDE,
  CANVAS_WIDTH,
  LANDER_INTERACT_RADIUS,
  WALK_SPEED,
  GRAVITY,
} from "../../src/game/constants.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const mounts: Array<{ root: Root; container: HTMLDivElement }> = [];

/** Mount the Game component. Returns the wrapper element. */
async function mountGame(): Promise<HTMLDivElement> {
  const el = document.createElement("div");
  document.body.appendChild(el);
  const root = createRoot(el);
  mounts.push({ root, container: el });

  await act(async () => {
    root.render(<Game />);
  });

  // Wait for canvas to appear
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
async function waitFor(
  check: () => boolean,
  timeoutMs: number,
  message: string,
): Promise<void> {
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
  timeoutMs = 10000,
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
    const el = await mountGame();
    const canvas = el.querySelector<HTMLCanvasElement>(
      '[data-testid="game-canvas"]',
    );
    expect(canvas).toBeTruthy();
    expect(canvas!.width).toBeGreaterThan(0);
    expect(canvas!.height).toBeGreaterThan(0);
  });

  // -------------------------------------------------------------------------
  // 2. Initial state
  // -------------------------------------------------------------------------

  it("player starts in descending mode above the moon", async () => {
    const el = await mountGame();

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
    const el = await mountGame();

    const y0 = readNum(el, "player-y");

    // Let the game loop run for a bit
    await new Promise((r) => setTimeout(r, 500));

    const y1 = readNum(el, "player-y");
    expect(y1).toBeGreaterThan(y0); // Y increases = moving downward
  });

  // -------------------------------------------------------------------------
  // 4. Thrust
  // -------------------------------------------------------------------------

  it("upward thrust reduces descent speed", async () => {
    const el = await mountGame();

    // Let gravity build up some downward velocity
    await new Promise((r) => setTimeout(r, 300));
    const vy0 = readNum(el, "velocity-y");
    expect(vy0).toBeGreaterThan(0); // Falling downward

    // Apply upward thrust
    await holdKey("ArrowUp", 200, "ArrowUp");
    await waitFrames(5);

    const vy1 = readNum(el, "velocity-y");
    expect(vy1).toBeLessThan(vy0); // Thrust should reduce downward velocity
  });

  it("horizontal thrust moves the lander sideways", async () => {
    const el = await mountGame();
    await waitFrames(5); // Let game initialise

    const x0 = readNum(el, "player-x");

    // Thrust right
    await holdKey("ArrowRight", 300, "ArrowRight");
    await waitFrames(5);

    const x1 = readNum(el, "player-x");
    expect(x1).toBeGreaterThan(x0); // Moved right
  });

  // -------------------------------------------------------------------------
  // 5. Landing
  // -------------------------------------------------------------------------

  it("lands safely when reaching ground at low velocity", async () => {
    const el = await mountGame();

    // Strategy: let the lander descend. The initial altitude is low enough
    // and gravity gentle enough that we can use thrust to land safely.
    // We wait for the lander to approach the ground, then check for landing.
    await waitForAttr(el, "player-mode", "landed", 15000);

    const y = readNum(el, "player-y");
    // Player should be at or very near ground level after landing
    expect(y).toBeGreaterThanOrEqual(GROUND_LEVEL - 5);
    expect(y).toBeLessThanOrEqual(GROUND_LEVEL);
  });

  // -------------------------------------------------------------------------
  // 6. Parallax starfield
  // -------------------------------------------------------------------------

  it("renders a parallax starfield background", async () => {
    const el = await mountGame();
    const canvas = el.querySelector<HTMLCanvasElement>(
      '[data-testid="game-canvas"]',
    )!;
    const ctx = canvas.getContext("2d")!;

    // Sample pixels from the upper portion of the canvas (space area).
    // A starfield should have at least some non-background-colour pixels.
    const imageData = ctx.getImageData(0, 0, canvas.width, 100);
    const { data } = imageData;

    let brightPixels = 0;
    for (let i = 0; i < data.length; i += 4) {
      // Background is #0a0a0f → RGB(10, 10, 15). Stars are brighter.
      if (data[i] > 50 || data[i + 1] > 50 || data[i + 2] > 50) {
        brightPixels++;
      }
    }

    // Stars are spread across the full starfield. With the camera high up,
    // fewer are visible in the top 100 rows — but at least some should appear.
    expect(brightPixels).toBeGreaterThan(0);
  });

  // -------------------------------------------------------------------------
  // 7. Ground line
  // -------------------------------------------------------------------------

  it("renders the moon surface", async () => {
    const el = await mountGame();

    // Ground is only on screen after camera follows the player down.
    // Wait for landing so the camera locks with ground visible.
    await waitForAttr(el, "player-mode", "landed", 15000);

    const canvas = el.querySelector<HTMLCanvasElement>(
      '[data-testid="game-canvas"]',
    )!;
    const ctx = canvas.getContext("2d")!;

    // Sample the bottom portion of the canvas — ground should be visible there.
    const sampleY = canvas.height - 40;
    const imageData = ctx.getImageData(0, sampleY, canvas.width, 2);
    const { data } = imageData;

    let groundPixels = 0;
    for (let i = 0; i < data.length; i += 4) {
      // Ground is #2a1a3a → RGB(42, 26, 58) — distinctly purple-ish
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
    const el = await mountGame();

    // Wait for landing
    await waitForAttr(el, "player-mode", "landed", 15000);

    // Press E to exit
    pressKey("e", "KeyE");
    await waitFrames(5);
    releaseKey("e", "KeyE");

    await waitForAttr(el, "player-mode", "walking", 3000);
  });

  // -------------------------------------------------------------------------
  // 9. Walking
  // -------------------------------------------------------------------------

  it("astronaut walks right with D key", async () => {
    const el = await mountGame();
    await waitForAttr(el, "player-mode", "landed", 15000);

    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    const x0 = readNum(el, "player-x");

    // Walk right
    await holdKey("d", 300, "KeyD");
    await waitFrames(5);

    const x1 = readNum(el, "player-x");
    expect(x1).toBeGreaterThan(x0);
  });

  it("astronaut walks left with A key", async () => {
    const el = await mountGame();
    await waitForAttr(el, "player-mode", "landed", 15000);

    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    const x0 = readNum(el, "player-x");

    // Walk left
    await holdKey("a", 300, "KeyA");
    await waitFrames(5);

    const x1 = readNum(el, "player-x");
    expect(x1).toBeLessThan(x0);
  });

  // -------------------------------------------------------------------------
  // 10. Lander visibility while walking
  // -------------------------------------------------------------------------

  it("lander remains visible at landing position while walking", async () => {
    const el = await mountGame();
    await waitForAttr(el, "player-mode", "landed", 15000);

    // Record lander position
    const landerX = readNum(el, "lander-x");

    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    // Walk away from the lander
    await holdKey("d", 500, "KeyD");
    await waitFrames(5);

    // Lander should still be at the same position
    const landerX2 = readNum(el, "lander-x");
    expect(landerX2).toBe(landerX);

    // Player should have moved away
    const playerX = readNum(el, "player-x");
    expect(playerX).not.toBe(landerX);
  });

  // -------------------------------------------------------------------------
  // 11. Re-enter lander
  // -------------------------------------------------------------------------

  it("pressing E near the lander re-enters it", async () => {
    const el = await mountGame();
    await waitForAttr(el, "player-mode", "landed", 15000);

    // Exit lander
    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    // Walk a short distance away (but stay close enough to return)
    await holdKey("d", 150, "KeyD");
    await waitFrames(3);

    // Walk back toward lander
    await holdKey("a", 200, "KeyA");
    await waitFrames(3);

    // Should be near lander — press E to re-enter
    pressKey("e", "KeyE");
    await waitFrames(5);
    releaseKey("e", "KeyE");

    await waitForAttr(el, "player-mode", "in_lander", 3000);
  });

  it("pressing E when far from lander does nothing", async () => {
    const el = await mountGame();
    await waitForAttr(el, "player-mode", "landed", 15000);

    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    // Walk far away from lander
    await holdKey("d", 1000, "KeyD");
    await waitFrames(5);

    // Verify we're far from the lander
    const playerX = readNum(el, "player-x");
    const landerX = readNum(el, "lander-x");
    expect(Math.abs(playerX - landerX)).toBeGreaterThan(LANDER_INTERACT_RADIUS);

    // Press E — should do nothing
    pressKey("e", "KeyE");
    await waitFrames(10);
    releaseKey("e", "KeyE");

    const mode = readStr(el, "player-mode");
    expect(mode).toBe("walking");
  });

  // -------------------------------------------------------------------------
  // 12. Full flow integration test
  // -------------------------------------------------------------------------

  /**
   * The Phase 1 question: "Can I land and walk around?"
   *
   *   spawn (descending)
   *     │  gravity pulls lander down
   *     │  player uses thrust to control descent
   *     ▼
   *   land (landed)
   *     │  press E
   *     ▼
   *   exit lander (walking)
   *     │  walk right with D
   *     │  walk left with A (back toward lander)
   *     ▼
   *   near lander → press E
   *     │
   *     ▼
   *   re-enter (in_lander)
   */
  it("full Phase 1 flow: descend → land → walk → return → re-enter", async () => {
    const el = await mountGame();

    // --- Descending ---
    expect(readStr(el, "player-mode")).toBe("descending");
    const startY = readNum(el, "player-y");
    expect(startY).toBeLessThan(GROUND_LEVEL);

    // Use gentle thrust to control descent
    // (just let it descend naturally — initial altitude is low)
    await waitForAttr(el, "player-mode", "landed", 15000);
    expect(readNum(el, "player-y")).toBeGreaterThanOrEqual(GROUND_LEVEL - 5);

    // --- Exit lander ---
    const landerX = readNum(el, "lander-x");
    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    // --- Walk right ---
    await holdKey("d", 400, "KeyD");
    await waitFrames(5);
    const walkRightX = readNum(el, "player-x");
    expect(walkRightX).toBeGreaterThan(landerX);

    // Lander hasn't moved
    expect(readNum(el, "lander-x")).toBe(landerX);

    // --- Walk back left toward lander ---
    await holdKey("a", 600, "KeyA");
    await waitFrames(5);
    const walkBackX = readNum(el, "player-x");
    expect(walkBackX).toBeLessThan(walkRightX);

    // --- Re-enter lander ---
    // We should be near the lander now (walked back past it)
    pressKey("e", "KeyE");
    await waitFrames(5);
    releaseKey("e", "KeyE");

    await waitForAttr(el, "player-mode", "in_lander", 3000);
  });
});
