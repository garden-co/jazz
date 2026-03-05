/**
 * E2E browser tests for Moon Lander — Surface Mechanics.
 *
 * Covers everything that happens on (or near) the moon's surface:
 *   - Start mode (waiting to begin descent)
 *   - Lander: landing state, exiting, re-entering, visibility while walking
 *   - Walking: movement keys, walking mode transitions
 *   - Jump: Space/W while walking triggers a floaty lunar hop
 *   - World wrapping: walking or thrusting off one edge reappears at the other
 *
 * All tests mount <Game> directly (no Jazz sync) with physicsSpeed=10.
 */

import { act } from "react";
import { createRoot } from "react-dom/client";
import { afterEach, describe, expect, it } from "vitest";
import { Game } from "../../src/Game";
import { GROUND_LEVEL, LANDER_INTERACT_RADIUS, MOON_SURFACE_WIDTH } from "../../src/game/constants";
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
// Start mode
// ---------------------------------------------------------------------------

describe("Moon Lander — Start Mode", () => {
  it("defaults to start mode when no initialMode is specified", async () => {
    const el = await mountGame({});

    expect(readStr(el, "player-mode")).toBe("start");
  });

  it("Space transitions from start to descending", async () => {
    const el = await mountGame({});

    expect(readStr(el, "player-mode")).toBe("start");

    pressKey(" ", "Space");
    await waitFrames(2);
    releaseKey(" ", "Space");

    await waitForAttr(el, "player-mode", "descending", 3000);
  });

  it("movement keys do nothing in start mode", async () => {
    const el = await mountGame({});

    const yBefore = readNum(el, "player-y");

    await holdKey("ArrowUp", 100, "ArrowUp");
    await waitFrames(5);

    expect(readStr(el, "player-mode")).toBe("start");
    expect(readNum(el, "player-y")).toBe(yBefore);
  });
});

// ---------------------------------------------------------------------------
// Lander interaction
// ---------------------------------------------------------------------------

describe("Moon Lander — Lander Interaction", () => {
  it("landed mode places lander at ground level", async () => {
    const el = await mountGame({ initialMode: "landed", spawnX: SPAWN_X });

    expect(readStr(el, "player-mode")).toBe("landed");

    const y = readNum(el, "player-y");
    expect(y).toBeGreaterThanOrEqual(GROUND_LEVEL - 5);
    expect(y).toBeLessThanOrEqual(GROUND_LEVEL);
  });

  it("pressing E after landing exits the lander to walking mode", async () => {
    const el = await mountGame({ initialMode: "landed", spawnX: SPAWN_X });

    pressKey("e", "KeyE");
    await waitFrames(5);
    releaseKey("e", "KeyE");

    await waitForAttr(el, "player-mode", "walking", 3000);
  });

  it("lander remains visible at landing position while walking", async () => {
    const el = await mountGame({ initialMode: "landed", spawnX: SPAWN_X });

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

    // Walk away
    await holdKey("d", 80, "KeyD");
    await waitFrames(5);

    expect(readNum(el, "lander-x")).toBe(landerX);
    expect(readNum(el, "player-x")).not.toBe(landerX);
  });

  it("pressing E near the lander re-enters it", async () => {
    const el = await mountGame({ initialMode: "landed", spawnX: SPAWN_X });

    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    // Walk a short distance away then back
    await holdKey("d", 50, "KeyD");
    await waitFrames(3);
    await holdKey("a", 50, "KeyA");
    await waitFrames(3);

    pressKey("e", "KeyE");
    await waitFrames(5);
    releaseKey("e", "KeyE");

    await waitForAttr(el, "player-mode", "in_lander", 3000);
  });

  it("pressing E when far from lander does nothing", async () => {
    const el = await mountGame({ initialMode: "landed", spawnX: SPAWN_X });

    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    // Walk far away
    await holdKey("d", 200, "KeyD");
    await waitFrames(5);

    const playerX = readNum(el, "player-x");
    const landerX = readNum(el, "lander-x");
    expect(Math.abs(playerX - landerX)).toBeGreaterThan(LANDER_INTERACT_RADIUS);

    pressKey("e", "KeyE");
    await waitFrames(10);
    releaseKey("e", "KeyE");

    expect(readStr(el, "player-mode")).toBe("walking");
  });
});

// ---------------------------------------------------------------------------
// Walking
// ---------------------------------------------------------------------------

describe("Moon Lander — Walking", () => {
  it("astronaut walks right with D key", async () => {
    const el = await mountGame({ initialMode: "landed", spawnX: SPAWN_X });

    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    const x0 = readNum(el, "player-x");
    await holdKey("d", 50, "KeyD");
    await waitFrames(5);

    expect(readNum(el, "player-x")).toBeGreaterThan(x0);
  });

  it("astronaut walks left with A key", async () => {
    const el = await mountGame({ initialMode: "landed", spawnX: SPAWN_X });

    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    const x0 = readNum(el, "player-x");
    await holdKey("a", 50, "KeyA");
    await waitFrames(5);

    expect(readNum(el, "player-x")).toBeLessThan(x0);
  });

  it("full flow: land → walk → return → re-enter", async () => {
    /**
     *   landed (on ground)
     *     │  press E
     *     ▼
     *   walking  ──D──►  walk right (~96px)
     *             ◄──A──  walk back (~120px, ending left of lander)
     *     │  press E (within radius)
     *     ▼
     *   in_lander
     */
    const el = await mountGame({ initialMode: "landed", spawnX: SPAWN_X });

    expect(readStr(el, "player-mode")).toBe("landed");

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

    const landerX = readNum(el, "lander-x");

    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    // Walk right
    await holdKey("d", 80, "KeyD");
    await waitFrames(5);
    expect(readNum(el, "player-x")).toBeGreaterThan(landerX);
    expect(readNum(el, "lander-x")).toBe(landerX);

    // Walk back left (a bit further to end up left of lander, within radius)
    await holdKey("a", 100, "KeyA");
    await waitFrames(5);
    expect(readNum(el, "player-x")).toBeLessThan(readNum(el, "lander-x") + 100);

    pressKey("e", "KeyE");
    await waitFrames(5);
    releaseKey("e", "KeyE");

    await waitForAttr(el, "player-mode", "in_lander", 3000);
  });
});

// ---------------------------------------------------------------------------
// Jump
// ---------------------------------------------------------------------------

describe("Moon Lander — Jump", () => {
  /**
   *   Player (walking, on ground)  presses Space
   *        ▼
   *   ════╤════════════════════════
   *        posY rises above GROUND_LEVEL, then returns
   */
  it("Space triggers a jump that rises above ground and returns", async () => {
    const el = await mountGame({ initialMode: "landed" });

    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    expect(readNum(el, "player-y")).toBe(GROUND_LEVEL);

    pressKey(" ", "Space");
    await waitFrames(2);
    releaseKey(" ", "Space");

    // Should rise above ground (Y increases downward, so < GROUND_LEVEL = higher up)
    await waitFor(
      () => readNum(el, "player-y") < GROUND_LEVEL,
      2000,
      "player should rise above ground after jump",
    );

    await waitFor(
      () => readNum(el, "player-y") >= GROUND_LEVEL,
      3000,
      "player should land back on ground",
    );

    expect(
      el.querySelector('[data-testid="game-container"]')!.getAttribute("data-player-mode"),
    ).toBe("walking");
  });

  it("W key also triggers a jump while walking", async () => {
    const el = await mountGame({ initialMode: "landed" });

    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    pressKey("w", "KeyW");
    await waitFrames(2);
    releaseKey("w", "KeyW");

    await waitFor(() => readNum(el, "player-y") < GROUND_LEVEL, 2000, "W key should trigger jump");
  });

  it("jump peak is modest (under 80px)", async () => {
    /**
     * With JUMP_VELOCITY=-140 and JUMP_GRAVITY=200,
     * theoretical peak = 140²/(2×200) = 49px.
     */
    const el = await mountGame({ initialMode: "landed" });

    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    pressKey(" ", "Space");
    await waitFrames(2);
    releaseKey(" ", "Space");

    let minY = GROUND_LEVEL;
    const deadline = Date.now() + 3000;
    while (Date.now() < deadline) {
      const y = readNum(el, "player-y");
      if (y < minY) minY = y;
      if (y >= GROUND_LEVEL && minY < GROUND_LEVEL) break;
      await new Promise((r) => setTimeout(r, 30));
    }

    const peakHeight = GROUND_LEVEL - minY;
    expect(peakHeight).toBeGreaterThan(10);
    expect(peakHeight).toBeLessThan(80);
  });

  it("cannot double-jump while airborne", async () => {
    const el = await mountGame({ initialMode: "landed" });

    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    pressKey(" ", "Space");
    await waitFrames(2);
    releaseKey(" ", "Space");

    await waitFor(() => readNum(el, "player-y") < GROUND_LEVEL, 2000, "should be airborne");

    const yMidAir = readNum(el, "player-y");

    // Try second jump mid-air
    pressKey(" ", "Space");
    await waitFrames(2);
    releaseKey(" ", "Space");

    await new Promise((r) => setTimeout(r, 100));
    // Should not boost significantly higher than mid-air position
    expect(readNum(el, "player-y")).toBeGreaterThan(yMidAir - 20);
  });
});

// ---------------------------------------------------------------------------
// World wrapping
// ---------------------------------------------------------------------------

describe("Moon Lander — World Wrapping", () => {
  /**
   * Moon surface: [0 ··· MOON_SURFACE_WIDTH)
   *
   * Walking or thrusting off one edge wraps to the other.
   */
  it("walking off the right edge wraps to the left side", async () => {
    const spawnX = MOON_SURFACE_WIDTH - 100;
    const el = await mountGame({ initialMode: "landed", spawnX });

    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    expect(readNum(el, "player-x")).toBeGreaterThan(MOON_SURFACE_WIDTH - 200);

    // Walk right to cross the boundary (~1200px/s at 10x, 200ms covers ~240px)
    await holdKey("d", 200, "KeyD");
    await waitFrames(5);

    expect(readNum(el, "player-x")).toBeLessThan(MOON_SURFACE_WIDTH / 2);
  });

  it("walking off the left edge wraps to the right side", async () => {
    const spawnX = 100;
    const el = await mountGame({ initialMode: "landed", spawnX });

    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    expect(readNum(el, "player-x")).toBeLessThan(200);

    await holdKey("a", 200, "KeyA");
    await waitFrames(5);

    expect(readNum(el, "player-x")).toBeGreaterThan(MOON_SURFACE_WIDTH / 2);
  });

  it("descent position wraps around the world boundary", async () => {
    const spawnX = MOON_SURFACE_WIDTH - 50;
    const el = await mountGame({ initialMode: "descending", spawnX });

    await holdKey("ArrowRight", 200, "ArrowRight");
    await waitFrames(5);

    const x = readNum(el, "player-x");
    expect(x).toBeGreaterThanOrEqual(0);
    expect(x).toBeLessThan(MOON_SURFACE_WIDTH);
  });
});
