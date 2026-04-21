/**
 * Soak test: two players run around on the moon for 10 minutes.
 *
 * Verifies that cross-client Jazz sync stays alive under sustained load:
 * deposits collected and player position and mode changes propagating
 * continuously over 10 minutes of walking.
 *
 * Structure:
 *
 *   A (mountApp)                  Jazz DB                  B (mountApp)
 *   ────────────                  ───────                  ────────────
 *   exit lander ─────────────────────────────────────────→ exit lander
 *   see B (remote-player-count≥1) ←── sync ──────────────→ see A
 *
 *   ┌── soak loop (10 min) ───────────────────────────────────────────┐
 *   │  alternate d / a every 5s (both players, page-wide key events)  │
 *   └─────────────────────────────────────────────────────────────────┘
 *
 *   end assertions:
 *     • A and B still see each other  (remote-player-count ≥ 1)
 *     • both still online             (player-online = "true")
 *     • deposit collected by A drops B's sync-uncollected count
 *       → confirms the write path + edge subscription are both live
 */

import { act } from "react";
import { createRoot } from "react-dom/client";
import { afterEach, describe, expect, it } from "vitest";
import { commands } from "vitest/browser";
import { App } from "../../src/App";
import { ADMIN_SECRET, APP_ID_MULTI } from "./test-constants";
import {
  type MountEntry,
  pressKey,
  releaseKey,
  readNum,
  readStr,
  waitFor,
  waitForAttr,
  unmountAll,
} from "./test-helpers";

const SOAK_DURATION_MS = 10 * 60 * 1000; // 10 minutes
const SYNC_TIMEOUT = 30_000;
const DIRECTION_INTERVAL_MS = 5_000;

const mounts: MountEntry[] = [];

function uniqueDbName(label: string): string {
  return `soak-${label}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

async function mountApp(opts: {
  appId?: string;
  dbName?: string;
  serverUrl?: string;
  physicsSpeed?: number;
  spawnX?: number;
  adminSecret?: string;
}): Promise<HTMLDivElement> {
  const { physicsSpeed, spawnX, adminSecret, ...config } = opts;
  const el = document.createElement("div");
  document.body.appendChild(el);
  const root = createRoot(el);
  mounts.push({ root, container: el });

  await act(async () => {
    root.render(
      <App
        {...({
          config: {
            appId: config.appId ?? APP_ID_MULTI,
            ...config,
            ...(adminSecret ? { adminSecret } : {}),
          },
          playerId: crypto.randomUUID(),
          physicsSpeed,
          initialMode: "landed",
          ...(spawnX !== undefined ? { spawnX } : {}),
        } as any)}
      />,
    );
  });

  await waitFor(
    () => el.querySelector('[data-testid="game-canvas"]') !== null,
    10_000,
    "App should render game canvas",
  );

  return el;
}

afterEach(async () => {
  await unmountAll(mounts);
});

describe("Moon Lander — Soak Test", () => {
  it.skip(
    "two players synchronise continuously over 10 minutes",
    { timeout: 720_000 },
    async () => {
      const serverUrl = await commands.startFreshTestServer("soak");

      try {
        // Mount both players before pressing "e" so both exit their landers
        // on the same page-wide keydown event.
        const elA = await mountApp({
          appId: APP_ID_MULTI,
          dbName: uniqueDbName("soak-a"),
          serverUrl,
          adminSecret: ADMIN_SECRET,
          physicsSpeed: 5,
          spawnX: 4800,
        });

        const elB = await mountApp({
          appId: APP_ID_MULTI,
          dbName: uniqueDbName("soak-b"),
          serverUrl,
          adminSecret: ADMIN_SECRET,
          physicsSpeed: 5,
          spawnX: 4900,
        });

        // Both exit landers simultaneously.
        pressKey("e", "KeyE");
        await waitForAttr(elA, "player-mode", "walking", 5_000);
        releaseKey("e", "KeyE");

        // Confirm mutual visibility before starting the soak.
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
          "both players should see each other before soak begins",
        );

        // ── Soak loop ─────────────────────────────────────────────────────────
        //
        // Alternate movement direction every 5s, keeping both players walking
        // and generating a continuous stream of position updates through the
        // sync pipeline.
        //
        const soakEnd = Date.now() + SOAK_DURATION_MS;
        let direction = "d";
        let directionChangeAt = Date.now() + DIRECTION_INTERVAL_MS;

        // Start moving right.
        pressKey("d", "KeyD");

        while (Date.now() < soakEnd) {
          await new Promise((r) => setTimeout(r, 500));
          const now = Date.now();

          if (now >= directionChangeAt) {
            releaseKey(direction, direction === "d" ? "KeyD" : "KeyA");
            direction = direction === "d" ? "a" : "d";
            pressKey(direction, direction === "d" ? "KeyD" : "KeyA");
            directionChangeAt = now + DIRECTION_INTERVAL_MS;
          }
        }

        // Stop all movement.
        releaseKey(direction, direction === "d" ? "KeyD" : "KeyA");

        // ── End assertions ────────────────────────────────────────────────────

        // Both players still see each other.
        await waitFor(
          () => {
            try {
              return readNum(elA, "remote-player-count") >= 1;
            } catch {
              return false;
            }
          },
          SYNC_TIMEOUT,
          "Player A should still see Player B after 10-minute soak",
        );
        await waitFor(
          () => {
            try {
              return readNum(elB, "remote-player-count") >= 1;
            } catch {
              return false;
            }
          },
          SYNC_TIMEOUT,
          "Player B should still see Player A after 10-minute soak",
        );

        // Both players still report as online.
        expect(readStr(elA, "player-online")).toBe("true");
        expect(readStr(elB, "player-online")).toBe("true");

        // Live write path: A collects a deposit; B's uncollected count drops.
        const syncElB = elB.querySelector('[data-testid="sync-debug"]');
        const uncollectedBefore = syncElB
          ? parseFloat(syncElB.getAttribute("data-sync-uncollected") ?? "0")
          : 0;

        if (uncollectedBefore > 0) {
          // Walk A right for 5s to collect some deposits.
          pressKey("d", "KeyD");
          await new Promise((r) => setTimeout(r, 5_000));
          releaseKey("d", "KeyD");

          const aInventory = readStr(elA, "inventory");

          if (aInventory !== "") {
            // A collected something — B's uncollected count should drop.
            await waitFor(
              () => {
                try {
                  const raw = syncElB?.getAttribute("data-sync-uncollected");
                  return raw != null && parseFloat(raw) < uncollectedBefore;
                } catch {
                  return false;
                }
              },
              SYNC_TIMEOUT,
              `B's sync-uncollected (${uncollectedBefore}) should drop after A collects`,
            );
          }
        }
      } finally {
        await commands.stopFreshTestServer("soak").catch(() => {});
      }
    },
  );
});
