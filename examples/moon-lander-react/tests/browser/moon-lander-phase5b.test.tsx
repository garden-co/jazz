/**
 * E2E browser tests for Moon Lander — Phase 5b: Walking Jump.
 *
 * Tests the astronaut jump mechanic: Space/W while walking triggers a
 * short, floaty lunar-gravity hop. Deposit collection is horizontal-only,
 * so jumping doesn't cause pickups to be missed.
 *
 * Data attribute contract:
 *   data-player-mode  — "walking" during jump
 *   data-player-y     — goes above GROUND_LEVEL during jump, returns after
 */

import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, describe, expect, it } from "vitest";
import { Game } from "../../src/Game";
import { GROUND_LEVEL } from "../../src/game/constants";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const SPEED = 10;
const mounts: Array<{ root: Root; container: HTMLDivElement }> = [];

async function mountGame(): Promise<HTMLDivElement> {
  const el = document.createElement("div");
  document.body.appendChild(el);
  const root = createRoot(el);
  mounts.push({ root, container: el });

  await act(async () => {
    root.render(<Game physicsSpeed={SPEED} initialMode={"landed" as any} />);
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

async function waitFrames(n: number): Promise<void> {
  for (let i = 0; i < n; i++) {
    await new Promise((r) => requestAnimationFrame(r));
  }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("Moon Lander — Phase 5b: Walking Jump", () => {
  // =========================================================================
  // 1. Space triggers a jump while walking
  //
  //   Player (walking, on ground)   presses Space
  //        ▼                            ▼
  //   ════╤════════════════════════════════
  //        posY rises above GROUND_LEVEL
  //        then returns to GROUND_LEVEL
  // =========================================================================

  it("Space triggers a jump that rises above ground and returns", async () => {
    const el = await mountGame();

    // Walk
    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    // Confirm on ground
    const yBefore = readNum(el, "player-y");
    expect(yBefore).toBe(GROUND_LEVEL);

    // Jump
    pressKey(" ", "Space");
    await waitFrames(2);
    releaseKey(" ", "Space");

    // Should rise above ground (posY < GROUND_LEVEL, since Y increases downward)
    await waitFor(
      () => readNum(el, "player-y") < GROUND_LEVEL,
      2000,
      "player should rise above ground after jump",
    );

    // Should return to ground
    await waitFor(
      () => readNum(el, "player-y") >= GROUND_LEVEL,
      3000,
      "player should land back on ground",
    );

    // Still walking (jump doesn't change mode)
    expect(
      el.querySelector('[data-testid="game-container"]')!.getAttribute("data-player-mode"),
    ).toBe("walking");
  });

  // =========================================================================
  // 2. W triggers the same jump
  // =========================================================================

  it("W key also triggers a jump while walking", async () => {
    const el = await mountGame();

    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    // Jump with W
    pressKey("w", "KeyW");
    await waitFrames(2);
    releaseKey("w", "KeyW");

    await waitFor(
      () => readNum(el, "player-y") < GROUND_LEVEL,
      2000,
      "W key should trigger jump above ground",
    );
  });

  // =========================================================================
  // 3. Jump peak is modest (not too high)
  //
  //   With JUMP_VELOCITY=-140 and JUMP_GRAVITY=200,
  //   theoretical peak = 140²/(2×200) = 49px.
  //   Allow some margin for physicsSpeed timing.
  // =========================================================================

  it("jump peak is modest (under 80px)", async () => {
    const el = await mountGame();

    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    pressKey(" ", "Space");
    await waitFrames(2);
    releaseKey(" ", "Space");

    // Track minimum Y (highest point, since Y axis is inverted)
    let minY = GROUND_LEVEL;
    const deadline = Date.now() + 3000;
    while (Date.now() < deadline) {
      const y = readNum(el, "player-y");
      if (y < minY) minY = y;
      if (y >= GROUND_LEVEL && minY < GROUND_LEVEL) break; // landed
      await new Promise((r) => setTimeout(r, 30));
    }

    const peakHeight = GROUND_LEVEL - minY;
    expect(peakHeight).toBeGreaterThan(10); // actually jumped
    expect(peakHeight).toBeLessThan(80); // not too high
  });

  // =========================================================================
  // 4. No double-jump (pressing Space mid-air does nothing)
  // =========================================================================

  it("cannot double-jump while airborne", async () => {
    const el = await mountGame();

    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    // First jump
    pressKey(" ", "Space");
    await waitFrames(2);
    releaseKey(" ", "Space");

    // Wait until airborne
    await waitFor(() => readNum(el, "player-y") < GROUND_LEVEL, 2000, "should be airborne");

    // Record current Y
    const yMidAir = readNum(el, "player-y");

    // Try to jump again mid-air
    pressKey(" ", "Space");
    await waitFrames(2);
    releaseKey(" ", "Space");

    // Y should not go significantly higher than it was
    // (it may drift slightly due to existing velocity)
    await new Promise((r) => setTimeout(r, 100));
    const yAfterSecondPress = readNum(el, "player-y");

    // The second press should NOT have boosted us higher than the first
    // jump's natural arc would allow. We check that we're not dramatically
    // higher (more than 20px above the mid-air reading).
    expect(yAfterSecondPress).toBeGreaterThan(yMidAir - 20);
  });
});
