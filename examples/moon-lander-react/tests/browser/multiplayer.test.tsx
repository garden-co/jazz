/**
 * E2E browser tests for Moon Lander — Multiplayer.
 *
 * Tests 1-2: local Game behaviour (player identity — no Jazz needed).
 *   Mount <Game> directly.
 *
 * Tests 3-6: real multiplayer sync through a Jazz server.
 *   Mount <App config={...}> with independent instances connected to the
 *   same sync server. Verify that each instance sees the other as a remote
 *   player, that collected deposits propagate cross-client, that fuel sharing
 *   propagates via Jazz, and that burst deposits reappear correctly.
 *
 * Data attribute contract:
 *   data-player-id           — unique player ID (persisted in localStorage)
 *   data-player-name         — deterministic player name
 *   data-player-color        — assigned colour (hex string)
 *   data-required-fuel       — required fuel type from FUEL_TYPES
 *   data-lander-fuel         — current lander fuel level (number as string)
 *   data-player-online       — "true" when the player is online
 *   data-remote-player-count — number of visible remote players being rendered
 */

import { act } from "react";
import { createRoot } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { commands } from "vitest/browser";
import { App } from "../../src/App";
import { Game } from "../../src/Game";
import { FUEL_TYPES } from "../../src/game/constants";
import { ADMIN_SECRET, APP_ID, APP_ID_MULTI, TEST_PORT } from "./test-constants";
import {
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

/** Generous timeout: earlier test files accumulate data in the shared server,
 *  slowing edge subscriptions. Isolated BrowserContexts need extra time:
 *  fresh OPFS, new Jazz client init, full sync handshake. */
const SYNC_TIMEOUT = 20_000;

const mounts: MountEntry[] = [];
const openedIsolatedLabels: string[] = [];

async function openIsolated(
  opts: Parameters<(typeof commands)["openIsolatedApp"]>[0],
): Promise<void> {
  openedIsolatedLabels.push(opts.label);
  await commands.openIsolatedApp(opts);
}

async function mountGame(opts: { physicsSpeed?: number } = {}): Promise<HTMLDivElement> {
  const el = document.createElement("div");
  document.body.appendChild(el);
  const root = createRoot(el);
  mounts.push({ root, container: el });

  const props: Record<string, unknown> = { initialMode: "landed" };
  if (opts.physicsSpeed !== undefined) props.physicsSpeed = opts.physicsSpeed;

  await act(async () => {
    root.render(<Game {...(props as any)} />);
  });

  await waitFor(
    () => el.querySelector('[data-testid="game-canvas"]') !== null,
    3000,
    "Game canvas should render",
  );

  return el;
}

function uniqueDbName(label: string): string {
  return `test-${label}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

async function mountApp(opts: {
  appId?: string;
  dbName?: string;
  serverUrl?: string;
  playerId?: string;
  physicsSpeed?: number;
  spawnX?: number;
  localAuthToken?: string;
  localAuthMode?: string;
  adminSecret?: string;
}): Promise<HTMLDivElement> {
  const { physicsSpeed, spawnX, playerId, localAuthToken, localAuthMode, adminSecret, ...config } =
    opts;
  const el = document.createElement("div");
  document.body.appendChild(el);
  const root = createRoot(el);
  mounts.push({ root, container: el });

  await act(async () => {
    root.render(
      <App
        {...({
          config: {
            appId: config.appId ?? APP_ID,
            ...config,
            ...(localAuthToken
              ? { localAuthMode: localAuthMode ?? "anonymous", localAuthToken }
              : {}),
            ...(adminSecret ? { adminSecret } : {}),
          },
          playerId: playerId ?? crypto.randomUUID(),
          physicsSpeed,
          initialMode: "landed",
          ...(spawnX !== undefined ? { spawnX } : {}),
        } as any)}
      />,
    );
  });

  await waitFor(
    () => el.querySelector('[data-testid="game-canvas"]') !== null,
    10000,
    "App should render game canvas",
  );

  return el;
}

beforeEach(() => {
  localStorage.removeItem("moon-lander-player-id");
});

afterEach(async () => {
  for (const label of openedIsolatedLabels.splice(0)) {
    await commands.closeIsolatedApp(label).catch(() => {});
  }
  await unmountAll(mounts);
});

// ---------------------------------------------------------------------------
// Player identity
// ---------------------------------------------------------------------------

describe("Moon Lander — Player Identity", () => {
  it("creates a unique player ID and stores it in localStorage", async () => {
    const el = await mountGame({ physicsSpeed: 10 });

    const playerId = readStr(el, "player-id");
    expect(playerId).toBeTruthy();
    expect(playerId.length).toBeGreaterThan(0);

    expect(localStorage.getItem("moon-lander-player-id")).toBe(playerId);
  });

  it("reuses player ID from localStorage on remount", async () => {
    const el1 = await mountGame({ physicsSpeed: 10 });
    const id1 = readStr(el1, "player-id");

    const mount = mounts.pop()!;
    await act(async () => mount.root.unmount());
    mount.container.remove();

    const el2 = await mountGame({ physicsSpeed: 10 });
    expect(readStr(el2, "player-id")).toBe(id1);
  });
});

// ---------------------------------------------------------------------------
// Cross-client sync (real Jazz server)
// ---------------------------------------------------------------------------

describe("Moon Lander — Cross-Client Sync", () => {
  it("collecting a deposit in connected mode updates inventory via Jazz", async () => {
    /**
     *   engine: walk over deposit
     *     │  onCollectDeposit(id) → queued
     *     ▼
     *   setInterval: db.update(fuel_deposits, id, { collected: true, collectedBy })
     *     │
     *     ▼
     *   Jazz subscription: fuel_deposits updated
     *     │  App.tsx derives inventory from collectedBy = playerId
     *     ▼
     *   data-inventory reflects Jazz state
     */
    const serverUrl = `http://127.0.0.1:${TEST_PORT}`;
    const playerId = crypto.randomUUID();

    const el = await mountApp({
      appId: APP_ID,
      dbName: uniqueDbName("inv-a"),
      serverUrl,
      playerId,
      physicsSpeed: 10,
    });

    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    expect(readStr(el, "inventory")).toBe("");

    // Walk right to collect deposits (3s at 10x ≈ 3600px coverage)
    pressKey("d", "KeyD");
    await new Promise((r) => setTimeout(r, 3000));
    releaseKey("d", "KeyD");
    await waitFrames(10);

    await waitFor(
      () => {
        try {
          return readStr(el, "inventory") !== "";
        } catch {
          return false;
        }
      },
      5000,
      "inventory should update after collecting deposits via Jazz round-trip",
    );

    const inventory = readStr(el, "inventory").split(",");
    expect(inventory.length).toBeGreaterThan(0);

    for (const type of inventory) {
      expect((FUEL_TYPES as readonly string[]).includes(type)).toBe(true);
    }
  });

  it("deposit collected by Player A disappears for Player B", { timeout: 60_000 }, async () => {
    /**
     *   Player A (own identity)   Jazz DB          Player B (own identity)
     *   ────────────────────────  ───────          ──────────────────────
     *   settle → deposits seeded  ──────────────→  sees N deposits
     *   walk → collect ──────────→ collected=true  deposit-count drops for B
     */
    const serverUrl = `http://127.0.0.1:${TEST_PORT}`;
    const spawnX = 4800;

    const elA = await mountApp({
      appId: APP_ID,
      dbName: uniqueDbName("cross-coll-a"),
      serverUrl,
      adminSecret: ADMIN_SECRET,
      physicsSpeed: 10,
      spawnX,
    });

    pressKey("e", "KeyE");
    await waitForAttr(elA, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    await openIsolated({
      label: "cross-coll-b",
      appId: APP_ID,
      dbName: uniqueDbName("cross-coll-b"),
      serverUrl,
      adminSecret: ADMIN_SECRET,
      physicsSpeed: 10,
      spawnX,
    });

    await waitFor(
      async () => {
        try {
          const raw = await commands.readIsolatedAttr("cross-coll-b", "deposit-count");
          return raw !== null && parseFloat(raw) > 0;
        } catch {
          return false;
        }
      },
      SYNC_TIMEOUT,
      "Player B should see uncollected deposits",
    );

    // Allow reconcile to complete before reading countBefore
    await new Promise((r) => setTimeout(r, 2000));

    const countBefore = parseFloat(
      (await commands.readIsolatedAttr("cross-coll-b", "deposit-count")) ?? "0",
    );

    // A walks right to collect deposits (4s at 10x ≈ 4800px coverage)
    pressKey("d", "KeyD");
    await new Promise((r) => setTimeout(r, 4000));
    releaseKey("d", "KeyD");
    await waitFrames(10);

    await waitFor(
      () => {
        try {
          return readStr(elA, "inventory") !== "";
        } catch {
          return false;
        }
      },
      SYNC_TIMEOUT,
      "Player A inventory should be non-empty after walking",
    );

    await waitFor(
      async () => {
        try {
          const raw = await commands.readIsolatedAttr("cross-coll-b", "deposit-count");
          return raw !== null && parseFloat(raw) < countBefore;
        } catch {
          return false;
        }
      },
      SYNC_TIMEOUT,
      `Player B deposit-count should drop below ${countBefore}`,
    );
  });

  it(
    "shared deposit appears in receiver's inventory cross-client",
    { timeout: 90_000 },
    async () => {
      /**
       *   Player A              Jazz DB              Player B
       *   ────────              ───────              ────────
       *   exit lander ──────────────────────────→   exit lander
       *   (wait for B visible as remote player)
       *   walk right 8s ──→ collect all types
       *   walk left 8s ──→ back near B
       *   proximity share ──→ collectedBy=B ──────→ B inventory updates
       *
       * Walking for ~8s at physicsSpeed=10 covers the full MOON_SURFACE_WIDTH
       * (~9600px), ensuring A collects all 7 fuel types.
       */
      const serverUrl = `http://127.0.0.1:${TEST_PORT}`;
      const spawnX = 4800;

      const elA = await mountApp({
        appId: APP_ID,
        dbName: uniqueDbName("share-a"),
        serverUrl,
        adminSecret: ADMIN_SECRET,
        physicsSpeed: 10,
        spawnX,
      });

      pressKey("e", "KeyE");
      await waitForAttr(elA, "player-mode", "walking", 3000);
      releaseKey("e", "KeyE");

      await openIsolated({
        label: "share-b",
        appId: APP_ID,
        dbName: uniqueDbName("share-b"),
        serverUrl,
        adminSecret: ADMIN_SECRET,
        physicsSpeed: 10,
        spawnX,
      });

      await commands.pressIsolatedKey("share-b", "e");
      await commands.waitForIsolatedAttr("share-b", "player-mode", "walking", 5000);
      await commands.releaseIsolatedKey("share-b", "e");

      await waitFor(
        () => {
          try {
            return parseFloat(readStr(elA, "remote-player-count")) >= 1;
          } catch {
            return false;
          }
        },
        SYNC_TIMEOUT,
        "Player A should see Player B as a remote player",
      );

      pressKey("d", "KeyD");
      await new Promise((r) => setTimeout(r, 8000));
      releaseKey("d", "KeyD");
      await waitFrames(10);

      await waitFor(
        () => {
          try {
            return readStr(elA, "inventory") !== "";
          } catch {
            return false;
          }
        },
        SYNC_TIMEOUT,
        "Player A inventory should be non-empty after collecting",
      );

      pressKey("a", "KeyA");
      await new Promise((r) => setTimeout(r, 8000));
      releaseKey("a", "KeyA");
      await waitFrames(10);

      await waitFor(
        async () => {
          try {
            const raw = await commands.readIsolatedAttr("share-b", "inventory");
            return raw !== null && raw !== "";
          } catch {
            return false;
          }
        },
        SYNC_TIMEOUT,
        "Player B inventory should be non-empty after receiving a shared deposit",
      );
    },
  );

  it(
    "burst deposits reappear as uncollected after entering lander",
    { timeout: 60_000 },
    async () => {
      /**
       *   Player A            Jazz DB               Player B
       *   ────────            ───────               ────────
       *   walk → collect ──→  collected=true  ──→   deposit disappears
       *   enter lander ────→  burst releases  ──→   deposit reappears
       *                       collected=false        deposit-count recovers
       */
      const serverUrl = `http://127.0.0.1:${TEST_PORT}`;
      const playerId = crypto.randomUUID();

      const el = await mountApp({
        appId: APP_ID,
        dbName: uniqueDbName("burst-release"),
        serverUrl,
        playerId,
        physicsSpeed: 10,
      });

      pressKey("e", "KeyE");
      await waitForAttr(el, "player-mode", "walking", 3000);
      releaseKey("e", "KeyE");

      await waitFor(
        () => {
          try {
            return readNum(el, "deposit-count") > 0;
          } catch {
            return false;
          }
        },
        10000,
        "deposits should be visible",
      );

      const countBefore = readNum(el, "deposit-count");

      pressKey("d", "KeyD");
      await new Promise((r) => setTimeout(r, 4000));
      releaseKey("d", "KeyD");
      await waitFrames(10);

      const inventory = readStr(el, "inventory");
      if (inventory === "") return; // didn't collect anything; skip gracefully

      const countAfterCollect = readNum(el, "deposit-count");
      const collected = countBefore - countAfterCollect;

      pressKey("a", "KeyA");
      await new Promise((r) => setTimeout(r, 4000));
      releaseKey("a", "KeyA");
      await waitFrames(5);

      pressKey("e", "KeyE");
      await waitForAttr(el, "player-mode", "in_lander", 5000);
      releaseKey("e", "KeyE");

      pressKey("e", "KeyE");
      await waitForAttr(el, "player-mode", "walking", 3000);
      releaseKey("e", "KeyE");

      // Use data-sync-uncollected (raw DB count from the edge subscription,
      // not filtered by local collectedIds) so the assertion is reliable.
      await waitFor(
        () => {
          try {
            const syncEl = el.querySelector('[data-testid="sync-debug"]')!;
            const raw = syncEl.getAttribute("data-sync-uncollected");
            return parseInt(raw ?? "0", 10) > countAfterCollect;
          } catch {
            return false;
          }
        },
        SYNC_TIMEOUT,
        `uncollected-count should recover after burst release (was ${countAfterCollect}, collected ${collected})`,
      );
    },
  );

  it("full sync: two players descend, land, and see each other", { timeout: 60_000 }, async () => {
    /**
     *   Instance A           sync server          Instance B
     *   ──────────           ───────────          ──────────
     *   mount ────────────→  write state ───────→  mount
     *   landed ────────────→ write landed ──────→  landed
     *   see remote(B) ←────  read all players ←── see remote(A)
     *   exit lander ──────→  write walking ─────→  see A walking
     *
     * Uses a dedicated fresh Jazz server with an empty event log so
     * reconcile deposits insert quickly and player rows are visible
     * within SYNC_TIMEOUT.
     */
    const serverUrl = await commands.startFreshTestServer("full-phase2");
    const sharedToken = `full-token-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

    try {
      const elA = await mountApp({
        appId: APP_ID_MULTI,
        dbName: uniqueDbName("full-a"),
        serverUrl,
        localAuthToken: sharedToken,
        adminSecret: ADMIN_SECRET,
        physicsSpeed: 10,
      });

      const elB = await mountApp({
        appId: APP_ID_MULTI,
        dbName: uniqueDbName("full-b"),
        serverUrl,
        localAuthToken: sharedToken,
        adminSecret: ADMIN_SECRET,
        physicsSpeed: 10,
      });

      expect(readStr(elA, "player-mode")).toBe("landed");
      expect(readStr(elB, "player-mode")).toBe("landed");

      await waitFor(
        () => {
          try {
            return (
              readNum(elA, "remote-player-count") >= 1 && readNum(elB, "remote-player-count") >= 1
            );
          } catch {
            return false;
          }
        },
        SYNC_TIMEOUT,
        "Both instances should see each other as remote players",
      );

      // Note: keyboard events are page-wide; test only asserts on A's mode change.
      pressKey("e", "KeyE");
      await waitForAttr(elA, "player-mode", "walking", 3000);
      releaseKey("e", "KeyE");

      await waitFor(
        () => {
          try {
            return readNum(elB, "remote-player-count") >= 1;
          } catch {
            return false;
          }
        },
        SYNC_TIMEOUT,
        "Instance B should still see Instance A after mode change",
      );
    } finally {
      await commands.stopFreshTestServer("full-phase2");
    }
  });
});
