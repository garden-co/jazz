// Browser receipt for the running app: `pnpm dev` in one terminal, then
// `pnpm test:e2e`. Drives real Better Auth sign-up, the server bootstrap, the
// editor and an invite link across two isolated browser contexts. Set
// SCREENSHOTS=<dir> to also capture the main screens at 1280 and 375 px in
// light and dark mode.
import assert from "node:assert/strict";
import { mkdir } from "node:fs/promises";
import { join } from "node:path";
import { chromium } from "@playwright/test";

const origin = process.env.APP_ORIGIN ?? "http://127.0.0.1:3000";
const screenshots = process.env.SCREENSHOTS;
const run = Date.now().toString(36);
const timeout = 60_000;

const browser = await chromium.launch({
  executablePath: process.env.CHROMIUM_PATH || undefined,
});

async function signUp(context, name) {
  const page = await context.newPage();
  page.setDefaultTimeout(timeout);
  await page.goto(origin);
  await page.getByRole("button", { name: "Create an account" }).click();
  await page.getByLabel("Name").fill(name);
  await page.getByLabel("Email").fill(`${name.toLowerCase()}-${run}@band-book.test`);
  await page.getByLabel("Password").fill("correct horse battery");
  await page.getByRole("button", { name: "Create account" }).click();
  await page.waitForURL(/\/workspace/);
  await page.getByRole("button", { name: "Setlist: spring tour" }).first().waitFor();
  return page;
}

async function shoot(page, name) {
  if (!screenshots) return;
  await mkdir(screenshots, { recursive: true });
  for (const scheme of ["light", "dark"]) {
    await page.emulateMedia({ colorScheme: scheme });
    for (const width of [1280, 375]) {
      await page.setViewportSize({ width, height: width === 375 ? 812 : 800 });
      await page.waitForTimeout(400);
      await page.screenshot({
        path: join(screenshots, `${name}-${width}-${scheme}.png`),
        fullPage: true,
      });
    }
  }
  await page.setViewportSize({ width: 1280, height: 800 });
  await page.emulateMedia({ colorScheme: "light" });
}

const sidebarItem = (page, name) => page.getByRole("button", { name, exact: true }).first();

try {
  const ownerContext = await browser.newContext({ viewport: { width: 1280, height: 800 } });
  const owner = await signUp(ownerContext, "Ada");

  // The seeded band: a setlist with to-dos, nested songs, tour notes, issues.
  await sidebarItem(owner, "Setlist: spring tour").click();
  await owner.getByLabel("To-do", { exact: true }).first().waitFor();
  await shoot(owner, "setlist");

  // Blocks are editable in place and new ones append at the end.
  await sidebarItem(owner, "Songs").click();
  await sidebarItem(owner, "Harbour lights").click();
  await owner.getByRole("textbox", { name: "Page title" }).waitFor();
  assert.equal(
    await owner.getByRole("textbox", { name: "Page title" }).inputValue(),
    "Harbour lights",
  );
  await owner.getByRole("button", { name: "Add block" }).click();
  await owner.getByRole("menuitem", { name: "Text" }).click();
  await owner.keyboard.type("Bridge: stay on the IV chord");
  await shoot(owner, "song");

  // Issues: table, then board.
  await sidebarItem(owner, "Issues").click();
  await owner.getByRole("table").waitFor();
  assert.ok(await owner.getByText("Confirm backline with Porto venue").isVisible());
  await shoot(owner, "issues-table");
  await owner.getByRole("tab", { name: "Board" }).click();
  await owner.getByRole("region", { name: "In progress" }).waitFor();
  await shoot(owner, "issues-board");

  // Share one song with a guest.
  await sidebarItem(owner, "Harbour lights").click();
  await owner.getByRole("button", { name: "Share" }).click();
  await owner.getByRole("button", { name: "Create link" }).click();
  const linkInput = owner.getByRole("dialog").getByRole("textbox").last();
  await linkInput.waitFor();
  const inviteUrl = await linkInput.inputValue();
  assert.match(inviteUrl, /\/invite\//);
  await shoot(owner, "share");
  await owner.keyboard.press("Escape");

  // The guest signs up and redeems the link: they see the song subtree only.
  const guestContext = await browser.newContext({ viewport: { width: 1280, height: 800 } });
  const guest = await signUp(guestContext, "Bo");
  await guest.goto(inviteUrl);
  await guest.waitForURL(/p=/);
  await guest.getByRole("textbox", { name: "Page title" }).waitFor();
  assert.equal(
    await guest.getByRole("textbox", { name: "Page title" }).inputValue(),
    "Harbour lights",
  );
  await guest
    .getByText("Bridge: stay on the IV chord")
    .or(guest.locator("textarea", { hasText: "Bridge" }))
    .first()
    .waitFor();
  const guestNav = await guest.getByRole("navigation", { name: "Pages" }).innerText();
  assert.ok(guestNav.includes("Harbour lights"), "guest sees the shared song");
  assert.ok(!guestNav.includes("Tour notes"), "guest does not see the rest of the band");
  await shoot(guest, "guest");

  // Live collaboration: the owner's edit reaches the guest without a reload.
  await owner.getByRole("textbox", { name: "Page title" }).fill("Harbour lights (final)");
  await guest.waitForFunction(
    () =>
      document.querySelector('textarea[aria-label="Page title"]')?.value ===
      "Harbour lights (final)",
    undefined,
    { timeout },
  );
  console.log("BandBook browser receipt passed");
} finally {
  await browser.close();
}
