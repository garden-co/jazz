/**
 * Screenshot capture script for the Jazz walkthrough slideshow.
 * Run with: pnpm walkthrough:shots
 */
import { test, expect } from "@playwright/test";
import { join } from "node:path";

const SHOTS = join(import.meta.dirname, "screenshots");

test.use({ viewport: { width: 1280, height: 800 } });

test("capture walkthrough screenshots", async ({ page }) => {
  await page.goto("/");
  await expect(page.locator("#map")).toBeVisible({ timeout: 60_000 });
  // Give the WASM runtime time to settle and cobe a few frames to render.
  await page.waitForTimeout(2000);

  const splashBtn = page.locator(".splash-btn");
  if (await splashBtn.isVisible({ timeout: 3_000 }).catch(() => false)) {
    await page.screenshot({ path: join(SHOTS, "01-globe-overview.png") });
    await splashBtn.click();
    await page.waitForTimeout(500);
  }

  await page.waitForTimeout(2000);
  await page.screenshot({ path: join(SHOTS, "03-logged-in-globe.png") });

  const stopChip = page.locator(".stop-chip").first();
  if (await stopChip.isVisible({ timeout: 3_000 }).catch(() => false)) {
    await stopChip.dispatchEvent("click");
    await page.waitForTimeout(2000);
  }
  await page.screenshot({ path: join(SHOTS, "04-stop-detail.png") });

  const calendar = page.locator(".tour-calendar");
  if (await calendar.isVisible({ timeout: 2_000 }).catch(() => false)) {
    await calendar.screenshot({ path: join(SHOTS, "05-calendar.png") });
  }

  const sheetClose = page.locator(".sheet-close");
  if (await sheetClose.isVisible({ timeout: 1_000 }).catch(() => false)) {
    await sheetClose.click();
    await page.waitForTimeout(500);
  }

  const mapEl = page.locator("#map");
  const box = await mapEl.boundingBox();
  if (box) {
    await mapEl.click({ position: { x: box.width * 0.6, y: box.height * 0.5 } });
    await page.waitForTimeout(500);
  }

  const popover = page.locator(".popover");
  if (await popover.isVisible({ timeout: 3_000 }).catch(() => false)) {
    await page.screenshot({ path: join(SHOTS, "06-add-stop-popover.png") });
    await page.locator(".popover-btn.confirm").click();
    await expect(page.locator(".stop-create-form")).toBeVisible({ timeout: 5_000 });
    await page.screenshot({ path: join(SHOTS, "07-create-form.png") });
  }

  await page.locator(".control-bar").screenshot({ path: join(SHOTS, "08-control-bar.png") });

  await page.goto("/?public");
  await expect(page.locator("#map")).toBeVisible({ timeout: 60_000 });
  await page.waitForTimeout(2000);

  const posterOverlay = page.locator(".poster-overlay");
  if (await posterOverlay.isVisible({ timeout: 10_000 }).catch(() => false)) {
    await page.screenshot({ path: join(SHOTS, "02-public-globe.png") });
  }
});
