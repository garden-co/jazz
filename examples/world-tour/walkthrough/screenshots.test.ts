/**
 * Screenshot capture script for the Jazz walkthrough slideshow.
 * Run with: pnpm walkthrough:shots
 */
import { test, expect } from "@playwright/test";
import { join } from "node:path";

const SHOTS = join(import.meta.dirname, "screenshots");

test.use({ viewport: { width: 1280, height: 800 } });

test("capture walkthrough screenshots", async ({ page }) => {
  // ── 0. Load the app (logged-in by default) ───────────────────────────────
  await page.goto("/");
  await expect(page.locator("#map")).toBeVisible({ timeout: 60_000 });
  // Give MapLibre tiles and WASM runtime time to settle
  await page.waitForTimeout(5000);

  // ── 1. Splash modal (shown to logged-in users) ──────────────────────────
  const splashBtn = page.locator(".splash-btn");
  if (await splashBtn.isVisible({ timeout: 3_000 }).catch(() => false)) {
    await page.screenshot({ path: join(SHOTS, "01-globe-overview.png") });
    await splashBtn.click();
    await page.waitForTimeout(500);
  }

  // ── 2. Logged-in globe with stop dots ────────────────────────────────────
  await page.waitForTimeout(2000);
  await page.screenshot({ path: join(SHOTS, "03-logged-in-globe.png") });

  // ── 3. Click a stop dot to open the detail sheet ─────────────────────────
  // Use the calendar's first stop chip to reliably open a stop
  const stopChip = page.locator(".stop-chip").first();
  if (await stopChip.isVisible({ timeout: 3_000 }).catch(() => false)) {
    await stopChip.dispatchEvent("click");
    await page.waitForTimeout(2000);
  }
  await page.screenshot({ path: join(SHOTS, "04-stop-detail.png") });

  // ── 4. Calendar close-up ─────────────────────────────────────────────────
  const calendar = page.locator(".tour-calendar");
  if (await calendar.isVisible({ timeout: 2_000 }).catch(() => false)) {
    await calendar.screenshot({ path: join(SHOTS, "05-calendar.png") });
  }

  // ── 5. Close sheet, trigger add-stop popover ─────────────────────────────
  const sheetClose = page.locator(".sheet-close");
  if (await sheetClose.isVisible({ timeout: 1_000 }).catch(() => false)) {
    await sheetClose.click();
    await page.waitForTimeout(500);
  }

  // Click on the map to trigger the add-stop popover
  const mapEl = page.locator("#map");
  const box = await mapEl.boundingBox();
  if (box) {
    await mapEl.click({ position: { x: box.width * 0.6, y: box.height * 0.5 } });
    await page.waitForTimeout(500);
  }

  const popover = page.locator(".popover");
  if (await popover.isVisible({ timeout: 3_000 }).catch(() => false)) {
    await page.screenshot({ path: join(SHOTS, "06-add-stop-popover.png") });

    // Confirm to open the create form
    await page.locator(".popover-btn.confirm").click();
    await expect(page.locator(".stop-create-form")).toBeVisible({ timeout: 5_000 });
    await page.screenshot({ path: join(SHOTS, "07-create-form.png") });
  }

  // ── 6. Control bar close-up ──────────────────────────────────────────────
  await page.locator(".control-bar").screenshot({ path: join(SHOTS, "08-control-bar.png") });

  // ── 7. Public view ───────────────────────────────────────────────────────
  await page.goto("/?public");
  await expect(page.locator("#map")).toBeVisible({ timeout: 60_000 });
  await page.waitForTimeout(5000);

  const posterOverlay = page.locator(".poster-overlay");
  if (await posterOverlay.isVisible({ timeout: 10_000 }).catch(() => false)) {
    await page.screenshot({ path: join(SHOTS, "02-public-globe.png") });
  }
});
