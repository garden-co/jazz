/**
 * E2E browser tests for Moon Lander — Phase 4: Automatic Fuel Sharing.
 *
 * Tests the proximity-based fuel sharing mechanic: when two walking players
 * are near each other, fuel the giver doesn't need is automatically
 * transferred to the receiver who does need it.
 *
 * Most tests mount <Game> directly with connected-mode props (deposits=[],
 * inventory=[...]) and a mock onShareFuel callback to verify the sharing
 * decision logic without Jazz.
 *
 * Phase 4 data attribute contract (new additions):
 *   (existing) data-player-mode, data-required-fuel, data-inventory
 *
 * New callback: onShareFuel(fuelType, receiverPlayerId)
 *   Fired by the engine when a nearby walking remote player needs a fuel
 *   type the local player has but doesn't need.
 */

import { describe, it, expect, afterEach } from "vitest";
import { createRoot, type Root } from "react-dom/client";
import { act } from "react";
import { Game } from "../../src/Game.js";
import {
  CANVAS_WIDTH,
  GROUND_LEVEL,
  LANDER_INTERACT_RADIUS,
  FUEL_TYPES,
} from "../../src/game/constants.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const SPEED = 10;
const mounts: Array<{ root: Root; container: HTMLDivElement }> = [];

/**
 * Mount Game with explicit props (connected-mode style).
 *
 * Passing deposits=[] puts the engine into connected mode where inventory
 * comes from the inventory prop. This gives the test full control over
 * what fuel the player carries without needing to walk and collect.
 */
async function mountGameWith(props: Record<string, unknown>): Promise<HTMLDivElement> {
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
  document.dispatchEvent(
    new KeyboardEvent("keydown", { key, code: code ?? key, bubbles: true }),
  );
}

function releaseKey(key: string, code?: string) {
  document.dispatchEvent(
    new KeyboardEvent("keyup", { key, code: code ?? key, bubbles: true }),
  );
}

/** Build a remote player object at the given position. */
function remotePlayer(overrides: {
  id?: string;
  name?: string;
  mode?: string;
  positionX?: number;
  requiredFuelType?: string;
  playerId?: string;
}) {
  return {
    id: overrides.id ?? "row-remote",
    name: overrides.name ?? "Remote",
    mode: overrides.mode ?? "walking",
    positionX: overrides.positionX ?? CANVAS_WIDTH / 2,
    positionY: GROUND_LEVEL,
    velocityX: 0,
    velocityY: 0,
    color: "#ff00ff",
    requiredFuelType: overrides.requiredFuelType ?? "hexagon",
    lastSeen: Math.floor(Date.now() / 1000),
    landerFuelLevel: 40,
    playerId: overrides.playerId ?? "remote-uuid",
    landerX: (overrides.positionX ?? CANVAS_WIDTH / 2) - 100,
  };
}

// ---------------------------------------------------------------------------
// Phase 4a: Proximity fuel sharing
// ---------------------------------------------------------------------------

describe("Moon Lander — Phase 4: Fuel Sharing", () => {
  // =========================================================================
  // 1. Basic sharing mechanic
  //
  //   Local player (has ⬡)     Remote player (needs ⬡)
  //        ▼                         ▼
  //   ════╤═════════════════════════╤════
  //        └───── within range ─────┘
  //              ⬡ transfers! →
  //
  //   onShareFuel("hexagon", "remote-uuid") fires
  // =========================================================================

  it("shares fuel with a nearby walking player who needs it", async () => {
    const shares: Array<{ fuelType: string; receiverPlayerId: string }> = [];

    const el = await mountGameWith({
      deposits: [],
      inventory: ["hexagon", "circle", "triangle"],
      remotePlayers: [
        remotePlayer({ requiredFuelType: "hexagon", playerId: "receiver-uuid" }),
      ],
      onShareFuel: (fuelType: string, receiverPlayerId: string) => {
        shares.push({ fuelType, receiverPlayerId });
      },
    });

    // Land and exit lander — local player starts at CANVAS_WIDTH/2,
    // same position as the remote player
    await waitForAttr(el, "player-mode", "landed", 3000);
    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    const localRequired = readStr(el, "required-fuel");

    // Both walking, same position → proximity met immediately.
    // Wait for the engine's game loop to detect and fire.
    await waitFor(
      () => shares.length > 0 || localRequired === "hexagon",
      2000,
      "onShareFuel should fire (or local also needs hexagon — guard case)",
    );

    if (localRequired !== "hexagon") {
      // Local has hexagon, doesn't need it → shared with remote
      const hexShare = shares.find((s) => s.fuelType === "hexagon");
      expect(hexShare).toBeTruthy();
      expect(hexShare!.receiverPlayerId).toBe("receiver-uuid");
    } else {
      // Guard: local also needs hexagon → no share
      expect(shares.find((s) => s.fuelType === "hexagon")).toBeUndefined();
    }
  });

  // =========================================================================
  // 2. Guard: never give away fuel the giver needs
  //
  //   Local (needs ⬡, has all 7)    7 remotes (each needs 1 type)
  //        ▼                               ▼
  //   ════╤═══════════════════════════════╤════
  //        shares 6 types ──────────────→
  //        keeps ⬡ (own required type)
  // =========================================================================

  it("does not share fuel the local player needs", async () => {
    const shares: Array<{ fuelType: string; receiverPlayerId: string }> = [];

    // Local player has ALL 7 fuel types. 7 remote players at the same
    // position, each needing exactly one type. The local player should
    // share 6 types and keep their own required type.
    const el = await mountGameWith({
      deposits: [],
      inventory: [...FUEL_TYPES],
      remotePlayers: FUEL_TYPES.map((ft) =>
        remotePlayer({
          id: `row-${ft}`,
          name: `Needs-${ft}`,
          requiredFuelType: ft,
          playerId: `recv-${ft}`,
        }),
      ),
      onShareFuel: (fuelType: string, receiverPlayerId: string) => {
        shares.push({ fuelType, receiverPlayerId });
      },
    });

    await waitForAttr(el, "player-mode", "landed", 3000);
    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    const localRequired = readStr(el, "required-fuel");

    // Wait for all 6 shareable types to be shared
    await waitFor(
      () => shares.length >= 6,
      3000,
      `should share 6 of 7 types (shared ${shares.length} so far)`,
    );

    const sharedTypes = new Set(shares.map((s) => s.fuelType));

    // The local player's required type must NOT be shared
    expect(sharedTypes.has(localRequired)).toBe(false);

    // All other types should have been shared
    expect(sharedTypes.size).toBe(6);
  });

  // =========================================================================
  // 3. No sharing when remote player is not walking
  //
  //   Local (walking, has ⬡)     Remote (landed, needs ⬡)
  //        ▼                           ▼
  //   ════╤═══════════════════════════╤════
  //        no transfer (remote not walking)
  // =========================================================================

  it("does not share when remote player is not walking", async () => {
    const shares: Array<{ fuelType: string; receiverPlayerId: string }> = [];

    const el = await mountGameWith({
      deposits: [],
      inventory: [...FUEL_TYPES],
      remotePlayers: [
        remotePlayer({ mode: "landed", requiredFuelType: "hexagon" }),
      ],
      onShareFuel: (fuelType: string, receiverPlayerId: string) => {
        shares.push({ fuelType, receiverPlayerId });
      },
    });

    await waitForAttr(el, "player-mode", "landed", 3000);
    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    // Give plenty of time — sharing should NOT fire
    await new Promise((r) => setTimeout(r, 1000));

    expect(shares).toHaveLength(0);
  });

  // =========================================================================
  // 4. No sharing when players are far apart
  //
  //   Local (walking)                          Remote (walking, 1000px away)
  //        ▼                                         ▼
  //   ════╤═════════════════════════════════════════╤════
  //        too far → no transfer
  // =========================================================================

  it("does not share when players are far apart", async () => {
    const shares: Array<{ fuelType: string; receiverPlayerId: string }> = [];

    const el = await mountGameWith({
      deposits: [],
      inventory: [...FUEL_TYPES],
      remotePlayers: [
        remotePlayer({
          positionX: CANVAS_WIDTH / 2 + 1000,
          requiredFuelType: "hexagon",
        }),
      ],
      onShareFuel: (fuelType: string, receiverPlayerId: string) => {
        shares.push({ fuelType, receiverPlayerId });
      },
    });

    await waitForAttr(el, "player-mode", "landed", 3000);
    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    await new Promise((r) => setTimeout(r, 1000));

    expect(shares).toHaveLength(0);
  });

  // =========================================================================
  // 5. No sharing when local player is not walking
  //
  //   Local (in_lander)           Remote (walking, nearby)
  //        ▼                           ▼
  //   ════╤═══════════════════════════╤════
  //        no transfer (local not walking)
  // =========================================================================

  it("does not share when local player is not walking", async () => {
    const shares: Array<{ fuelType: string; receiverPlayerId: string }> = [];

    const el = await mountGameWith({
      deposits: [],
      inventory: [...FUEL_TYPES],
      remotePlayers: [
        remotePlayer({ requiredFuelType: "hexagon" }),
      ],
      onShareFuel: (fuelType: string, receiverPlayerId: string) => {
        shares.push({ fuelType, receiverPlayerId });
      },
    });

    // Stay in lander (don't press E)
    await waitForAttr(el, "player-mode", "landed", 3000);

    await new Promise((r) => setTimeout(r, 1000));

    expect(shares).toHaveLength(0);
  });
});
