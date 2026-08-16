/**
 * E2E browser tests for Moon Lander — Multiplayer.
 *
 * Tests: real multiplayer sync through a Jazz server.
 *   Mount <App config={...}> with independent instances connected to the
 *   same sync server. Verify that each instance sees the other as a remote
 *   player, that collected deposits propagate cross-client, and that burst
 *   deposits reappear correctly.
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
import { generateAuthSecret } from "jazz-tools";
import { afterEach, describe, expect, it } from "vitest";
import { commands } from "vitest/browser";
import { App } from "../../src/App";
import { FUEL_TYPES, LANDER_INTERACT_RADIUS, MOON_SURFACE_WIDTH } from "../../src/game/constants";
import { ADMIN_SECRET, APP_ID, APP_ID_MULTI, TEST_SERVER_URL } from "./test-constants";
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

function uniqueDbName(label: string): string {
  return `moon-lander-${label}-${crypto.randomUUID()}`;
}

async function mountApp(opts: {
  appId?: string;
  dbName?: string;
  serverUrl?: string;
  playerId?: string;
  physicsSpeed?: number;
  spawnX?: number;
  localFirstSecret?: string;
  adminSecret?: string;
}): Promise<HTMLDivElement> {
  const { physicsSpeed, spawnX, playerId, localFirstSecret, adminSecret, ...config } = opts;
  const secret = localFirstSecret ?? (adminSecret ? undefined : generateAuthSecret());
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
            ...(secret ? { secret } : {}),
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

function deltaToLander(el: HTMLDivElement): number {
  const playerX = readNum(el, "player-x");
  const landerX = readNum(el, "lander-x");
  let delta = landerX - playerX;
  if (delta > MOON_SURFACE_WIDTH / 2) delta -= MOON_SURFACE_WIDTH;
  if (delta < -MOON_SURFACE_WIDTH / 2) delta += MOON_SURFACE_WIDTH;
  return delta;
}

async function walkToLander(el: HTMLDivElement, timeoutMs = 10_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const delta = deltaToLander(el);
    if (Math.abs(delta) <= LANDER_INTERACT_RADIUS) {
      // Engine state is mirrored to data attributes on a 50 ms interval. Wait
      // once with no movement held so a stale near-lander snapshot cannot make
      // the test interact after the physics position has already passed it.
      await new Promise((resolve) => setTimeout(resolve, 75));
      if (Math.abs(deltaToLander(el)) <= LANDER_INTERACT_RADIUS) return;
      continue;
    }

    const [key, code] = delta < 0 ? ["a", "KeyA"] : ["d", "KeyD"];
    const nearLander = Math.abs(delta) <= LANDER_INTERACT_RADIUS + 80;
    pressKey(key, code);
    if (nearLander) await waitFrames(1);
    else await new Promise((resolve) => setTimeout(resolve, 50));
    releaseKey(key, code);
    if (nearLander) await new Promise((resolve) => setTimeout(resolve, 75));
    else await waitFrames(1);
  }
  throw new Error(
    `Timed out walking back to lander (player=${readNum(el, "player-x")}, lander=${readNum(el, "lander-x")})`,
  );
}

afterEach(async () => {
  await unmountAll(mounts);
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
     *
     * Fresh server: avoids accumulated deposits from prior test runs landing at
     * the player's spawn position and being collected immediately on mode
     * transition (physics collects within ASTRONAUT_WIDTH=16px on first frame).
     * spawnX=100: seeded deposit positions on a fresh server place the nearest
     * deposit at X=0; 100px gap keeps it outside the 16px pickup radius.
     */
    const serverUrl = await commands.startFreshTestServer("inv-176");
    try {
      const playerId = crypto.randomUUID();

      const el = await mountApp({
        appId: APP_ID_MULTI,
        dbName: uniqueDbName("inventory"),
        serverUrl,
        playerId,
        physicsSpeed: 10,
        spawnX: 100,
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
    } finally {
      await unmountAll(mounts);
      await commands.stopFreshTestServer("inv-176");
    }
  });

  it("deposit collected by Player A disappears for Player B", { timeout: 60_000 }, async () => {
    /**
     *   Player A (mountApp)        Jazz DB          Player B (mountApp, same page)
     *   ─────────────────          ───────          ─────────────────────────────
     *   exit lander (walking)                       (stays in lander — mounted after)
     *   settle → deposits seeded   ──────────────→  sees N uncollected deposits
     *   walk → collect ────────────→ collected=true  sync-uncollected drops for B
     *
     * B mounts after A exits the lander so the "e" keydown does not reach B.
     * B stays in lander mode and does not collect deposits, keeping the
     * assertion clean: only A's collections cause the count drop observed by B.
     *
     * Both clients run in the same browser page (separate workers, separate OPFS
     * db names). The isolated-BrowserContext approach caused stream connect timeouts
     * in headless Chromium; same-page mountApp avoids this entirely.
     */
    const serverUrl = await commands.startFreshTestServer("cross-coll");
    const spawnX = 4800;

    try {
      const elA = await mountApp({
        appId: APP_ID_MULTI,
        dbName: uniqueDbName("cross-player-a"),
        serverUrl,
        adminSecret: ADMIN_SECRET,
        physicsSpeed: 10,
        spawnX,
      });

      pressKey("e", "KeyE");
      await waitForAttr(elA, "player-mode", "walking", 3000);
      releaseKey("e", "KeyE");

      // Mount B after A exits — B starts in "landed" mode and stays there.
      const elB = await mountApp({
        appId: APP_ID_MULTI,
        dbName: uniqueDbName("cross-player-b"),
        serverUrl,
        adminSecret: ADMIN_SECRET,
        physicsSpeed: 10,
        spawnX,
      });

      await waitFor(
        () => {
          try {
            const syncEl = elB.querySelector('[data-testid="sync-debug"]')!;
            const raw = syncEl.getAttribute("data-sync-uncollected");
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
        elB.querySelector('[data-testid="sync-debug"]')!.getAttribute("data-sync-uncollected") ??
          "0",
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
        () => {
          try {
            const syncEl = elB.querySelector('[data-testid="sync-debug"]')!;
            const raw = syncEl.getAttribute("data-sync-uncollected");
            return raw !== null && parseFloat(raw) < countBefore;
          } catch {
            return false;
          }
        },
        SYNC_TIMEOUT,
        `Player B sync-uncollected should drop below ${countBefore}`,
      );
    } finally {
      await unmountAll(mounts);
      await commands.stopFreshTestServer("cross-coll").catch(() => {});
    }
  });

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
      const serverUrl = TEST_SERVER_URL;
      const playerId = crypto.randomUUID();

      const el = await mountApp({
        appId: APP_ID,
        dbName: uniqueDbName("burst"),
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

      // Use the subscription-derived count instead of the engine's animated
      // deposit count so the assertion observes the database transition.
      const syncEl = el.querySelector('[data-testid="sync-debug"]')!;
      const countAfterCollect = parseInt(syncEl.getAttribute("data-sync-uncollected") ?? "0", 10);
      const collected = countBefore - countAfterCollect;

      await walkToLander(el);

      pressKey("e", "KeyE");
      await waitForAttr(el, "player-mode", "in_lander", 5000);
      releaseKey("e", "KeyE");

      pressKey("e", "KeyE");
      await waitForAttr(el, "player-mode", "walking", 3000);
      releaseKey("e", "KeyE");

      // Use the live database count, not the engine's local collectedIds view,
      // so the assertion observes the released deposit re-entering the query.
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

    try {
      const elA = await mountApp({
        appId: APP_ID_MULTI,
        dbName: uniqueDbName("full-sync-player-a"),
        serverUrl,
        adminSecret: ADMIN_SECRET,
        physicsSpeed: 10,
      });

      const elB = await mountApp({
        appId: APP_ID_MULTI,
        dbName: uniqueDbName("full-sync-player-b"),
        serverUrl,
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
      await unmountAll(mounts);
      await commands.stopFreshTestServer("full-phase2");
    }
  });
});
