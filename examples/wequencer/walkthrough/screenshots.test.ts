/**
 * Screenshot capture script for the Jazz walkthrough slideshow.
 * Run with: npm run walkthrough:shots
 */
import { test, expect } from "@playwright/test";
import { join } from "node:path";

const SHOTS = join(import.meta.dirname, "screenshots");

test.use({ viewport: { width: 1280, height: 800 } });

test("capture walkthrough screenshots", async ({ page }) => {
  await page.goto("/");

  // ── 1. Start screen ────────────────────────────────────────────────────────
  const startButton = page.locator(".start-prompt button");
  // First run seeds 7 instruments from scratch — allow plenty of time
  await expect(startButton).toBeVisible({ timeout: 60_000 });
  await page.screenshot({ path: join(SHOTS, "01-start-screen.png") });

  // ── 2. Activate audio + wait for all 7 instruments ─────────────────────────
  await startButton.click();
  await expect(page.locator(".sequencer .grid")).toBeVisible({ timeout: 10_000 });
  await expect(page.locator(".instrument-name")).toHaveCount(7, { timeout: 60_000 });

  // ── 3. Empty app ───────────────────────────────────────────────────────────
  await page.screenshot({ path: join(SHOTS, "02-app-empty.png") });

  // ── 4. Place beats to make the grid look lively ────────────────────────────
  const cells = page.locator(".beat-cell");

  // Kick: four on the floor
  for (const i of [0, 4, 8, 12]) await cells.nth(i).click();
  // Snare: 2 and 4
  for (const i of [4, 12]) await cells.nth(16 + i).click();
  // Hi-hat: every even beat
  for (const i of [0, 2, 4, 6, 8, 10, 12, 14]) await cells.nth(32 + i).click();
  // Piano 1: sparse melody
  for (const i of [0, 3, 5, 7, 10]) await cells.nth(48 + i).click();
  // Piano 2: counter-melody
  for (const i of [2, 6, 9, 13]) await cells.nth(64 + i).click();

  // ── 5. Full app with beats ─────────────────────────────────────────────────
  await page.screenshot({ path: join(SHOTS, "03-app-with-beats.png") });

  // ── 6. Grid close-up ───────────────────────────────────────────────────────
  await page.locator(".sequencer").screenshot({ path: join(SHOTS, "04-grid-closeup.png") });

  // ── 7. Instrument manager ──────────────────────────────────────────────────
  await page
    .locator(".instrument-manager")
    .screenshot({ path: join(SHOTS, "05-instrument-manager.png") });

  // ── 8. Instrument add form open ────────────────────────────────────────────
  await page.locator(".toggle-form-btn").click();
  await expect(page.locator(".add-form")).toBeVisible();
  await page
    .locator(".instrument-manager")
    .screenshot({ path: join(SHOTS, "06-instrument-add-form.png") });
  await page.locator(".toggle-form-btn").click(); // close again

  // ── 9. Participants panel ──────────────────────────────────────────────────
  await page.locator(".participants").screenshot({ path: join(SHOTS, "07-participants.png") });
});
