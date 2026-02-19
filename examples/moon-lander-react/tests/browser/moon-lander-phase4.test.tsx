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

import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, describe, expect, it } from "vitest";
import { Game } from "../../src/Game";
import {
  CANVAS_WIDTH,
  FUEL_TYPES,
  GROUND_LEVEL,
  SHARE_PROXIMITY_RADIUS,
} from "../../src/game/constants";

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
async function mountGameWith(
  props: Record<string, unknown>,
): Promise<HTMLDivElement> {
  const el = document.createElement("div");
  document.body.appendChild(el);
  const root = createRoot(el);
  mounts.push({ root, container: el });

  await act(async () => {
    root.render(<Game {...({ physicsSpeed: SPEED, initialMode: "landed", spawnX: CANVAS_WIDTH / 2, ...props } as any)} />);
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
        remotePlayer({
          requiredFuelType: "hexagon",
          playerId: "receiver-uuid",
        }),
      ],
      onShareFuel: (fuelType: string, receiverPlayerId: string) => {
        shares.push({ fuelType, receiverPlayerId });
      },
    });

    // Exit lander — local player starts at CANVAS_WIDTH/2,
    // same position as the remote player
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
      remotePlayers: [remotePlayer({ requiredFuelType: "hexagon" })],
      onShareFuel: (fuelType: string, receiverPlayerId: string) => {
        shares.push({ fuelType, receiverPlayerId });
      },
    });

    // Stay in lander (don't press E)
    await new Promise((r) => setTimeout(r, 1000));

    expect(shares).toHaveLength(0);
  });

  // =========================================================================
  // 6. Proximity hint shows when sharing would be possible at 2x radius
  //
  //   Local (walking, has ⬡)     Remote (walking, needs ⬡, 120px away)
  //        ▼                           ▼
  //   ════╤═══════════════════════════╤════
  //        hint zone (80–160px)
  //        data-share-hint="true"
  // =========================================================================

  it("shows share hint when a receiver is nearby but not close enough", async () => {
    // Place remote player at 120px — between SHARE_PROXIMITY_RADIUS (80)
    // and 2x SHARE_PROXIMITY_RADIUS (160)
    const hintDistance = SHARE_PROXIMITY_RADIUS + 40; // 120px

    const el = await mountGameWith({
      deposits: [],
      inventory: ["hexagon", "circle"],
      remotePlayers: [
        remotePlayer({
          positionX: CANVAS_WIDTH / 2 + hintDistance,
          requiredFuelType: "hexagon",
          playerId: "hint-receiver",
        }),
      ],
      onShareFuel: () => {},
    });

    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    const localRequired = readStr(el, "required-fuel");

    // Only expect the hint if local doesn't also need hexagon
    if (localRequired !== "hexagon") {
      await waitForAttr(el, "share-hint", "true", 3000);
    }
  });

  // =========================================================================
  // 7. No hint when remote is beyond 2x radius
  //
  //   Local (walking, has ⬡)     Remote (walking, needs ⬡, 500px away)
  //        ▼                                    ▼
  //   ════╤═════════════════════════════════════╤════
  //        too far for hint → data-share-hint="false"
  // =========================================================================

  it("does not show share hint when receiver is too far away", async () => {
    const el = await mountGameWith({
      deposits: [],
      inventory: [...FUEL_TYPES],
      remotePlayers: [
        remotePlayer({
          positionX: CANVAS_WIDTH / 2 + 500,
          requiredFuelType: "hexagon",
        }),
      ],
      onShareFuel: () => {},
    });

    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    // Give time for the engine loop to evaluate
    await new Promise((r) => setTimeout(r, 500));

    expect(readStr(el, "share-hint")).toBe("false");
  });

  // =========================================================================
  // 8. No sharing when receiver already has their required fuel
  //
  //   Local (has ⬡)     Remote (needs ⬡, already has ⬡)
  //        ▼                     ▼
  //   ════╤═════════════════════╤════
  //        no transfer (receiver satisfied)
  // =========================================================================

  it("does not share when receiver already has their required fuel", async () => {
    const shares: Array<{ fuelType: string; receiverPlayerId: string }> = [];

    const el = await mountGameWith({
      deposits: [],
      inventory: ["hexagon", "circle"],
      remotePlayers: [
        {
          ...remotePlayer({ requiredFuelType: "hexagon", playerId: "recv-1" }),
          landerFuelLevel: 100,
        },
      ],
      onShareFuel: (fuelType: string, receiverPlayerId: string) => {
        shares.push({ fuelType, receiverPlayerId });
      },
    });

    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    // Give plenty of time — sharing should NOT fire
    await new Promise((r) => setTimeout(r, 1000));

    expect(shares).toHaveLength(0);
  });
});

// ---------------------------------------------------------------------------
// Phase 4b: Inventory burst on lander entry
// ---------------------------------------------------------------------------

describe("Moon Lander — Phase 4b: Inventory Burst", () => {
  // =========================================================================
  // 1. Entering lander ejects non-required fuel types
  //
  //   Player (walking, has ⬡ ● ▲ ■)   requiredFuel = ???
  //        ▼  presses E near lander
  //   ════╤════════════════════════════
  //        keeps required type → refuels
  //        ejects others → onBurstDeposit fires for each
  // =========================================================================

  it("ejects non-required fuel types on lander entry", async () => {
    const bursts: string[] = [];
    const refuels: string[] = [];

    const el = await mountGameWith({
      deposits: [],
      inventory: [...FUEL_TYPES],
      onBurstDeposit: (fuelType: string) => {
        bursts.push(fuelType);
      },
      onRefuel: (fuelType: string) => {
        refuels.push(fuelType);
      },
    });

    // Land and walk out
    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    const localRequired = readStr(el, "required-fuel");

    // Walk back to lander and enter
    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "in_lander", 3000);
    releaseKey("e", "KeyE");

    // Wait for bursts to fire
    await waitFor(
      () => bursts.length >= FUEL_TYPES.length - 1,
      2000,
      `should eject ${FUEL_TYPES.length - 1} non-required types (got ${bursts.length})`,
    );

    const ejectedTypes = new Set(bursts);

    // Required type NOT ejected
    expect(ejectedTypes.has(localRequired)).toBe(false);

    // All other types ejected
    expect(ejectedTypes.size).toBe(FUEL_TYPES.length - 1);

    // Required type was consumed for refuelling
    expect(refuels).toContain(localRequired);
  });

  // =========================================================================
  // 2. Inventory is empty after entering lander
  //
  //   All fuel consumed or ejected — nothing lingers
  // =========================================================================

  it("non-required inventory is cleared after entering lander", async () => {
    const el = await mountGameWith({
      deposits: [],
      inventory: [...FUEL_TYPES],
      onBurstDeposit: () => {},
    });

    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    const localRequired = readStr(el, "required-fuel");

    // Re-enter lander
    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "in_lander", 3000);
    releaseKey("e", "KeyE");

    // After entering the lander, all non-required types are ejected
    // and the required type is consumed for refuelling.
    // With a static inventory prop, the merge re-adds the required type
    // each frame (in a real game, the DB would have removed it).
    // So we check that at most the required type remains.
    await waitFor(
      () => {
        const inv = readStr(el, "inventory");
        return inv === "" || inv === localRequired;
      },
      2000,
      "inventory should contain at most the required type after lander entry",
    );
  });

  // =========================================================================
  // 3. Re-entering lander after burst produces no additional bursts
  //
  //   First entry: 6 types ejected, required consumed
  //   Walk out, re-enter: inventory empty → 0 new bursts
  // =========================================================================

  it("re-entering lander produces no additional bursts", async () => {
    const bursts: string[] = [];

    const el = await mountGameWith({
      deposits: [],
      inventory: [...FUEL_TYPES],
      onBurstDeposit: (fuelType: string) => {
        bursts.push(fuelType);
      },
    });

    // Land, walk out, re-enter → first burst
    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "in_lander", 3000);
    releaseKey("e", "KeyE");

    await waitFor(
      () => bursts.length >= FUEL_TYPES.length - 1,
      2000,
      "first burst should eject non-required types",
    );
    const firstBurstCount = bursts.length;

    // Walk out again
    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    // Re-enter lander
    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "in_lander", 3000);
    releaseKey("e", "KeyE");

    // No additional bursts
    await new Promise((r) => setTimeout(r, 500));
    expect(bursts.length).toBe(firstBurstCount);
  });

  // =========================================================================
  // 4. No burst when inventory is empty
  // =========================================================================

  it("does not burst when inventory is empty", async () => {
    const bursts: string[] = [];

    const el = await mountGameWith({
      deposits: [],
      inventory: [],
      onBurstDeposit: (fuelType: string) => {
        bursts.push(fuelType);
      },
    });

    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "in_lander", 3000);
    releaseKey("e", "KeyE");

    await new Promise((r) => setTimeout(r, 500));
    expect(bursts).toHaveLength(0);
  });
});
