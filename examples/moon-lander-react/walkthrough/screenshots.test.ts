/**
 * Screenshot capture script for the Jazz Moon Lander walkthrough.
 * Run with: pnpm walkthrough:shots
 *
 * Generates PNG files in walkthrough/screenshots/ for use in jazz-moon-lander.md.
 * Screenshots are committed so the presentation works without running the script.
 */

import { test } from "@playwright/test";
import { join } from "node:path";
import { SCREENSHOT_APP_ID, SCREENSHOT_PORT } from "./screenshots-global-setup.js";

const SHOTS = join(import.meta.dirname, "screenshots");

test.use({ viewport: { width: 800, height: 500 } });

test("capture walkthrough screenshots", async ({ page }) => {
  // Navigate to the app with Jazz config supplied via URL params.
  // main.tsx reads appId + serverUrl from search params; passing appId also
  // sets initialMode="landed" so the player starts on the moon surface.
  // physicsSpeed=5 keeps real-time waits short without making screenshots look
  // too frantic — at 5x, 120 px/s walking speed becomes 600 px/s real.
  const url =
    `/?appId=${SCREENSHOT_APP_ID}` +
    `&serverUrl=http://127.0.0.1:${SCREENSHOT_PORT}` +
    `&spawnX=4800` +
    `&physicsSpeed=5`;
  await page.goto(url);

  // ── 1. Wait for canvas ─────────────────────────────────────────────────────
  await page.waitForSelector('[data-testid="game-canvas"]', { timeout: 30_000 });

  // ── 2. Wait for Jazz edge subscription to settle ───────────────────────────
  // data-sync-settled="true" is set by GameWithSync once useAll(…,"edge") has
  // received its first response from the server.
  await page.waitForSelector('[data-sync-settled="true"]', { timeout: 30_000 });

  // ── 3. Wait for deposits to appear (reconcileDeposits has run) ─────────────
  // After settle, SyncManager calls reconcileDeposits on the next 200ms flush,
  // which inserts fuel deposits via edge-tier INSERTs. The surface should show
  // deposits once the WHERE ENTRY events arrive.
  await page.waitForFunction(
    () => {
      const el = document.querySelector("[data-sync-uncollected]");
      return el !== null && Number(el.getAttribute("data-sync-uncollected")) > 0;
    },
    { timeout: 15_000 },
  );

  // ── 4. Game landed — player in lander, deposits on surface ─────────────────
  await page.screenshot({ path: join(SHOTS, "01-game-landed.png") });
  await page.locator('[data-testid="game-canvas"]').screenshot({
    path: join(SHOTS, "02-surface-deposits.png"),
  });

  // ── 5. Exit lander — player starts walking ─────────────────────────────────
  await page.keyboard.press("e");
  await page.waitForSelector('[data-player-mode="walking"]', { timeout: 5_000 });

  // ── 6. Walk right to collect fuel deposits ─────────────────────────────────
  // At 5x speed, 2 s real ≈ 10 s game → ~1200 px covered, picks up deposits.
  await page.keyboard.down("d");
  await page.waitForTimeout(2_000);
  await page.keyboard.up("d");

  // ── 7. Walking with inventory ──────────────────────────────────────────────
  await page.screenshot({ path: join(SHOTS, "03-player-walking.png") });
});
