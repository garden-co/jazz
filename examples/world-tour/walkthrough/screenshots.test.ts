/**
 * Captures the walkthrough screenshots. Run with: pnpm walkthrough:shots
 *
 * Three browser contexts are three accounts: the band owner (who gets the
 * seeded demo tour), a public visitor, and an invitee who joins with the
 * owner's invite link.
 */
import { test, expect, type Browser, type Page } from "@playwright/test";
import { join } from "node:path";

const SHOTS = join(import.meta.dirname, "screenshots");
const desktop = { width: 1280, height: 800 };

async function open(
  browser: Browser,
  url: string,
  options: Parameters<Browser["newContext"]>[0] = {},
) {
  const context = await browser.newContext({ viewport: desktop, ...options });
  const page = await context.newPage();
  await page.goto(url);
  await expect(page.locator(".masthead")).toBeVisible({ timeout: 60_000 });
  return page;
}

/** Let the globe finish its fly-to and settle. */
const settle = (page: Page, ms = 2500) => page.waitForTimeout(ms);

test("capture walkthrough screenshots", async ({ browser }) => {
  // The owner: a fresh server has no bands, so the first visitor starts the demo tour.
  const owner = await open(browser, "/");
  await expect(owner.getByText("Your band.")).toBeVisible({ timeout: 60_000 });
  await expect(owner.locator(".stop-pin")).toHaveCount(12, { timeout: 30_000 });
  await settle(owner);
  await owner.screenshot({ path: join(SHOTS, "01-globe-overview.png") });
  await owner.locator(".masthead").screenshot({ path: join(SHOTS, "08-masthead.png") });

  await owner.getByRole("button", { name: "Band" }).click();
  const inviteInput = owner.getByLabel("Invite link");
  await expect(inviteInput).toHaveValue(/\/join\//);
  const inviteLink = await inviteInput.inputValue();
  const publicLink = inviteLink.replace(/\/join\/.*$/, "");

  // A public visitor sees confirmed dates only.
  const visitor = await open(browser, publicLink);
  await expect(visitor.getByRole("dialog")).toBeVisible({ timeout: 30_000 });
  await settle(visitor);
  await visitor.screenshot({ path: join(SHOTS, "02-public-globe.png") });

  // An invitee joins with the link.
  const invitee = await open(browser, inviteLink);
  await expect(invitee.getByRole("dialog", { name: /^Join / })).toBeVisible({ timeout: 30_000 });
  await invitee.getByLabel("Your name").fill("Robin");
  await settle(invitee, 500);
  await invitee.screenshot({ path: join(SHOTS, "09-join-invite.png") });
  await invitee.getByRole("button", { name: "Join band" }).click();
  await expect(invitee.getByText("You're in this band.")).toBeVisible({ timeout: 30_000 });

  await expect(owner.locator(".member-list li")).toHaveCount(2, { timeout: 30_000 });
  await owner.screenshot({ path: join(SHOTS, "03-band-panel.png") });
  await owner.getByRole("button", { name: "Close" }).click();

  // Stop detail with the calendar.
  await owner.locator(".stop-pin").first().dispatchEvent("click");
  await expect(owner.locator(".stop-detail")).toBeVisible();
  await settle(owner, 2000);
  await owner.screenshot({ path: join(SHOTS, "04-stop-detail.png") });
  await owner.locator(".calendar").screenshot({ path: join(SHOTS, "05-calendar.png") });
  await owner.getByRole("button", { name: "Close" }).click();

  // Add a stop by clicking the globe.
  await settle(owner, 800);
  const box = (await owner.locator("#map").boundingBox())!;
  await owner
    .locator("#map canvas")
    .click({ position: { x: box.width * 0.5, y: box.height * 0.55 } });
  await expect(owner.locator(".popover")).toBeVisible();
  await owner.screenshot({ path: join(SHOTS, "06-add-stop-popover.png") });
  await owner.getByRole("button", { name: "Add stop" }).click();
  await expect(owner.locator(".sheet.open form")).toBeVisible();
  await settle(owner, 800);
  await owner.screenshot({ path: join(SHOTS, "07-create-form.png") });

  // The same tour on a phone, in dark mode.
  const phone = await open(browser, publicLink, {
    viewport: { width: 375, height: 812 },
    colorScheme: "dark",
    isMobile: true,
    hasTouch: true,
  });
  await expect(phone.getByRole("dialog")).toBeVisible({ timeout: 30_000 });
  await phone.getByRole("button", { name: "Explore the globe" }).click();
  await settle(phone);
  await phone.screenshot({ path: join(SHOTS, "10-phone-dark.png") });
});
