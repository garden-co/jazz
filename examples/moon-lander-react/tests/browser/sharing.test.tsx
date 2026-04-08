/**
 * E2E browser tests for Moon Lander — Fuel Sharing.
 *
 * Tests the proximity-based fuel sharing mechanic: when two walking players
 * are near each other, fuel the giver doesn't need is automatically
 * transferred to the receiver who does need it.
 *
 * Also tests the inventory burst on lander entry: when re-entering the lander,
 * non-required fuel types are ejected back onto the surface.
 *
 * Most tests mount <Game> directly with connected-mode props (deposits=[],
 * inventory=[...]) and a mock onShareFuel callback to verify the sharing
 * decision logic without Jazz.
 *
 * Data attribute contract:
 *   data-player-mode, data-required-fuel, data-inventory, data-share-hint
 *
 * Callbacks tested:
 *   onShareFuel(fuelType, receiverPlayerId)
 *   onBurstDeposit(fuelType)
 *   onRefuel(fuelType)
 */

import { act } from "react";
import { createRoot } from "react-dom/client";
import { afterEach, describe, expect, it } from "vitest";
import { Game } from "../../src/Game";
import { FUEL_TYPES, GROUND_LEVEL, SHARE_PROXIMITY_RADIUS } from "../../src/game/constants";
import {
  type MountEntry,
  pressKey,
  readStr,
  releaseKey,
  unmountAll,
  waitFor,
  waitForAttr,
} from "./test-helpers";

const SPEED = 10;
const SPAWN_X = 480;

const mounts: MountEntry[] = [];

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
    root.render(
      <Game
        {...({ physicsSpeed: SPEED, initialMode: "landed", spawnX: SPAWN_X, ...props } as any)}
      />,
    );
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
    positionX: overrides.positionX ?? SPAWN_X,
    positionY: GROUND_LEVEL,
    velocityX: 0,
    velocityY: 0,
    color: "#ff00ff",
    requiredFuelType: overrides.requiredFuelType ?? "hexagon",
    lastSeen: Math.floor(Date.now() / 1000),
    landerFuelLevel: 40,
    playerId: overrides.playerId ?? "remote-uuid",
    landerX: (overrides.positionX ?? SPAWN_X) - 100,
  };
}

// ---------------------------------------------------------------------------
// Proximity fuel sharing
// ---------------------------------------------------------------------------

describe("Moon Lander — Fuel Sharing", () => {
  it("shares fuel with a nearby walking player who needs it", async () => {
    /**
     *   Local (has ⬡)     Remote (needs ⬡)
     *        ▼                   ▼
     *   ════╤═══════════════════╤════
     *        └─── within range ─┘
     *              ⬡ transfers! →
     *
     *   onShareFuel("hexagon", "receiver-uuid") fires
     */
    const shares: Array<{ fuelType: string; receiverPlayerId: string }> = [];

    const el = await mountGameWith({
      deposits: [],
      inventory: ["hexagon", "circle", "triangle"],
      remotePlayers: [remotePlayer({ requiredFuelType: "hexagon", playerId: "receiver-uuid" })],
      onShareFuel: (fuelType: string, receiverPlayerId: string) => {
        shares.push({ fuelType, receiverPlayerId });
      },
    });

    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    const localRequired = readStr(el, "required-fuel");

    await waitFor(
      () => shares.length > 0 || localRequired === "hexagon",
      2000,
      "onShareFuel should fire (or local also needs hexagon — guard case)",
    );

    if (localRequired !== "hexagon") {
      const hexShare = shares.find((s) => s.fuelType === "hexagon");
      expect(hexShare).toBeTruthy();
      expect(hexShare!.receiverPlayerId).toBe("receiver-uuid");
    } else {
      expect(shares.find((s) => s.fuelType === "hexagon")).toBeUndefined();
    }
  });

  it("does not share fuel the local player needs", async () => {
    /**
     *   Local (needs ⬡, has all 7)    7 remotes (each needs 1 type)
     *        ▼                               ▼
     *   ════╤═══════════════════════════════╤════
     *        shares 6 types ──────────────→
     *        keeps ⬡ (own required type)
     */
    const shares: Array<{ fuelType: string; receiverPlayerId: string }> = [];

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

    await waitFor(
      () => shares.length >= 6,
      3000,
      `should share 6 of 7 types (shared ${shares.length} so far)`,
    );

    const sharedTypes = new Set(shares.map((s) => s.fuelType));
    expect(sharedTypes.has(localRequired)).toBe(false);
    expect(sharedTypes.size).toBe(6);
  });

  it("does not share when remote player is not walking", async () => {
    /**
     *   Local (walking, has ⬡)     Remote (landed, needs ⬡)
     *        ▼                           ▼
     *   ════╤═══════════════════════════╤════
     *        no transfer (remote not walking)
     */
    const shares: Array<{ fuelType: string; receiverPlayerId: string }> = [];

    const el = await mountGameWith({
      deposits: [],
      inventory: [...FUEL_TYPES],
      remotePlayers: [remotePlayer({ mode: "landed", requiredFuelType: "hexagon" })],
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

  it("does not share when players are far apart", async () => {
    /**
     *   Local (walking)                   Remote (walking, 1000px away)
     *        ▼                                   ▼
     *   ════╤═══════════════════════════════════╤════
     *        too far → no transfer
     */
    const shares: Array<{ fuelType: string; receiverPlayerId: string }> = [];

    const el = await mountGameWith({
      deposits: [],
      inventory: [...FUEL_TYPES],
      remotePlayers: [remotePlayer({ positionX: SPAWN_X + 1000, requiredFuelType: "hexagon" })],
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

  it("does not share when local player is not walking", async () => {
    /**
     *   Local (in_lander)           Remote (walking, nearby)
     *        ▼                           ▼
     *   ════╤═══════════════════════════╤════
     *        no transfer (local not walking)
     */
    const shares: Array<{ fuelType: string; receiverPlayerId: string }> = [];

    const _el = await mountGameWith({
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

  it("shows share hint when a receiver is nearby but not close enough", async () => {
    /**
     *   Local (walking, has ⬡)     Remote (walking, needs ⬡, 120px away)
     *        ▼                           ▼
     *   ════╤═══════════════════════════╤════
     *        hint zone (SHARE_PROXIMITY_RADIUS to 2× SHARE_PROXIMITY_RADIUS)
     *        data-share-hint="true"
     */
    const hintDistance = SHARE_PROXIMITY_RADIUS + 40;

    const el = await mountGameWith({
      deposits: [],
      inventory: ["hexagon", "circle"],
      remotePlayers: [
        remotePlayer({
          positionX: SPAWN_X + hintDistance,
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

    if (localRequired !== "hexagon") {
      await waitForAttr(el, "share-hint", "true", 3000);
    }
  });

  it("does not show share hint when receiver is too far away", async () => {
    /**
     *   Local (walking, has ⬡)     Remote (walking, needs ⬡, 500px away)
     *        ▼                                    ▼
     *   ════╤═════════════════════════════════════╤════
     *        too far → data-share-hint="false"
     */
    const el = await mountGameWith({
      deposits: [],
      inventory: [...FUEL_TYPES],
      remotePlayers: [remotePlayer({ positionX: SPAWN_X + 500, requiredFuelType: "hexagon" })],
      onShareFuel: () => {},
    });

    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    await new Promise((r) => setTimeout(r, 500));

    expect(readStr(el, "share-hint")).toBe("false");
  });

  it("does not share when receiver already has their required fuel", async () => {
    /**
     *   Local (has ⬡)     Remote (needs ⬡, already has ⬡ — landerFuelLevel=100)
     *        ▼                     ▼
     *   ════╤═════════════════════╤════
     *        no transfer (receiver satisfied)
     */
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

    await new Promise((r) => setTimeout(r, 1000));

    expect(shares).toHaveLength(0);
  });
});

// ---------------------------------------------------------------------------
// Inventory burst on lander entry
// ---------------------------------------------------------------------------

describe("Moon Lander — Inventory Burst", () => {
  it("ejects non-required fuel types on lander entry", async () => {
    /**
     *   Player (walking, has all 7)   requiredFuel = ???
     *        ▼  presses E near lander
     *   ════╤════════════════════════
     *        keeps required type → refuels
     *        ejects others → onBurstDeposit fires for each
     */
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

    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    const localRequired = readStr(el, "required-fuel");

    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "in_lander", 3000);
    releaseKey("e", "KeyE");

    await waitFor(
      () => bursts.length >= FUEL_TYPES.length - 1,
      2000,
      `should eject ${FUEL_TYPES.length - 1} non-required types (got ${bursts.length})`,
    );

    const ejectedTypes = new Set(bursts);
    expect(ejectedTypes.has(localRequired)).toBe(false);
    expect(ejectedTypes.size).toBe(FUEL_TYPES.length - 1);
    expect(refuels).toContain(localRequired);
  });

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

    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "in_lander", 3000);
    releaseKey("e", "KeyE");

    await waitFor(
      () => {
        const inv = readStr(el, "inventory");
        return inv === "" || inv === localRequired;
      },
      2000,
      "inventory should contain at most the required type after lander entry",
    );
  });

  it("re-entering lander produces no additional bursts", async () => {
    /**
     *   First entry: 6 types ejected, required consumed
     *   Walk out, re-enter: inventory empty → 0 new bursts
     */
    const bursts: string[] = [];

    const el = await mountGameWith({
      deposits: [],
      inventory: [...FUEL_TYPES],
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

    await waitFor(
      () => bursts.length >= FUEL_TYPES.length - 1,
      2000,
      "first burst should eject non-required types",
    );
    const firstBurstCount = bursts.length;

    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "in_lander", 3000);
    releaseKey("e", "KeyE");

    await new Promise((r) => setTimeout(r, 500));
    expect(bursts.length).toBe(firstBurstCount);
  });

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
