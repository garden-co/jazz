/**
 * Screenshot capture for the Jazz chat walkthrough.
 *
 * Two browser contexts (Alice and Bob) sync via the local Jazz server started
 * by global-setup.ts. All screenshots are taken from Alice's viewport.
 *
 * Run with: pnpm run walkthrough:shots
 */
import { test } from "@playwright/test";
import { join } from "node:path";

const SHOTS = join(import.meta.dirname, "screenshots");
const VIEWPORT = { width: 390, height: 844 };

/** Type into the ProseMirror editor via its exposed handle. */
async function typeIntoEditor(page: import("@playwright/test").Page, text: string) {
  await page.evaluate((t) => {
    const el = document.querySelector("#messageEditor") as any;
    el.__editorHandle?.insertText(t);
  }, text);
}

/** Open the profile sheet, set the display name, then close it. */
async function setProfileName(page: import("@playwright/test").Page, name: string) {
  const nav = page.locator('header [data-slot="dropdown-menu-trigger"]');
  await nav.click();
  const profileItem = page.locator('[role="menuitem"]').filter({ hasText: "Profile" });
  await profileItem.waitFor({ timeout: 5_000 });
  await profileItem.click();
  await page.waitForSelector('[data-slot="sheet-content"]', { timeout: 5_000 });
  const nameInput = page.locator("input#name");
  await nameInput.waitFor({ timeout: 5_000 });
  await nameInput.fill(name);
  await page.keyboard.press("Escape");
  await page.waitForTimeout(300);
}

test("capture walkthrough screenshots", async ({ browser }) => {
  const aliceContext = await browser.newContext({ viewport: VIEWPORT });
  const bobContext = await browser.newContext({ viewport: VIEWPORT });
  const alicePage = await aliceContext.newPage();
  const bobPage = await bobContext.newPage();

  // ── Alice: land in her default public chat (ensures profile is created) ────

  await alicePage.goto("/");
  await alicePage.waitForSelector("#messageEditor", { timeout: 30_000 });
  await setProfileName(alicePage, "Alice");

  // ── Alice: go to chat list, create a private chat ─────────────────────────

  const aliceNav = alicePage.locator('header [data-slot="dropdown-menu-trigger"]');
  await aliceNav.click();
  await alicePage
    .locator('[data-slot="dropdown-menu-item"]')
    .filter({ hasText: "Chat List" })
    .click();
  await alicePage
    .locator("button")
    .filter({ hasText: "New Private Chat" })
    .waitFor({ timeout: 10_000 });
  await alicePage.locator("button").filter({ hasText: "New Private Chat" }).click();
  await alicePage.waitForSelector("#messageEditor", { timeout: 15_000 });

  // ── Alice: open ChatSettings, capture invite link ────────────────────────

  await alicePage.waitForSelector('[data-testid="chat-header"]', { timeout: 5_000 });
  await alicePage.locator('[data-testid="chat-header"] button:has(.lucide-settings)').click();
  await alicePage.waitForSelector('[data-slot="sheet-content"]', { timeout: 5_000 });
  await alicePage
    .locator('[data-slot="sheet-content"] button')
    .filter({ hasText: /invite/i })
    .waitFor({ timeout: 5_000 });
  await alicePage
    .locator('[data-slot="sheet-content"] button')
    .filter({ hasText: /invite/i })
    .click();
  await alicePage.waitForSelector("input#link", { timeout: 5_000 });
  const inviteLink = await alicePage.locator("input#link").inputValue();
  await alicePage.locator("button").filter({ hasText: "Done" }).click();
  await alicePage.waitForTimeout(300);
  // Close the settings sheet
  const sheetClose = alicePage.locator('[data-slot="sheet-content"] .lucide-x').locator('..');
  await sheetClose.click();
  await alicePage.waitForTimeout(300);

  // Give the server time to receive Alice's private chat before Bob arrives.
  // The jazz client syncs in the background; 4s is generous but avoids a race.
  await alicePage.waitForTimeout(4_000);

  // ── Bob: join via invite link, send a message ─────────────────────────────

  await bobPage.goto(inviteLink, { timeout: 30_000 });

  // Debug: capture Bob's page immediately after navigation so we can see
  // whether InviteHandler is rendering, or whether the app is still loading.
  await bobPage.waitForTimeout(2_000);
  await bobPage.screenshot({ path: join(SHOTS, "debug-bob-01-initial.png") });

  // Wait for InviteHandler to navigate to the chat view.
  // This is the slow step: Bob must initialise WASM, connect to the server,
  // subscribe with the join_code claim, receive the chat row, insert a
  // chatMember, then navigate.
  await bobPage.waitForURL(/\/#\/chat\//, { timeout: 120_000 });
  await bobPage.screenshot({ path: join(SHOTS, "debug-bob-02-chat-url.png") });
  await setProfileName(bobPage, "Bob");

  // Wait for ProseMirror to fully initialise (handle attached by useEffect).
  await bobPage.waitForFunction(
    () => !!(document.querySelector("#messageEditor") as any)?.__editorHandle,
    undefined,
    { timeout: 30_000 },
  );
  await typeIntoEditor(bobPage, "Hey Alice! Bob here 👋");
  await bobPage.locator("button:has(.lucide-send)").click();

  // ── Alice: wait for Bob's message to arrive ───────────────────────────────

  await alicePage.waitForFunction(() => document.body.textContent?.includes("Bob here"), {
    timeout: 15_000,
  });

  // ── 1. Chat view ──────────────────────────────────────────────────────────

  await alicePage.screenshot({ path: join(SHOTS, "01-chat-view.png") });

  // ── 2. Composing a message ────────────────────────────────────────────────

  await typeIntoEditor(alicePage, "Hey, this is Jazz!");
  await alicePage.screenshot({ path: join(SHOTS, "02-composing.png") });

  await alicePage.locator("button:has(.lucide-send)").click();
  await alicePage.waitForFunction(
    () =>
      [...document.querySelectorAll("article")].some((a) =>
        a.textContent?.includes("Hey, this is Jazz!"),
      ),
    { timeout: 5_000 },
  );

  // ── 3. Message sent ───────────────────────────────────────────────────────

  await alicePage.screenshot({ path: join(SHOTS, "03-message-sent.png") });

  // ── 4. Reaction picker ────────────────────────────────────────────────────
  // Click the message menu, hover the "React" sub-trigger to open the emoji
  // panel, then hover over ❤️ so the hover state is visible in the screenshot.

  const bobArticle = alicePage.locator("article").filter({ hasText: "Bob here" });
  await bobArticle.locator('[data-slot="dropdown-menu-trigger"]').click();
  const reactSubtrigger = alicePage.locator('[data-slot="dropdown-menu-sub-trigger"]').first();
  await reactSubtrigger.waitFor({ timeout: 3_000 });
  await reactSubtrigger.hover();
  await alicePage
    .locator('[data-slot="dropdown-menu-sub-content"] button')
    .first()
    .waitFor({ timeout: 3_000 });
  // Let the submenu slide-in animation settle, then hover over ❤️
  await alicePage.waitForTimeout(300);
  await alicePage
    .locator('[data-slot="dropdown-menu-sub-content"] button')
    .filter({ hasText: "❤️" })
    .hover();
  await alicePage.waitForTimeout(150);
  await alicePage.screenshot({ path: join(SHOTS, "04-reaction-picker.png") });

  // ── 5. Reaction applied ───────────────────────────────────────────────────

  await alicePage
    .locator('[data-slot="dropdown-menu-sub-content"] button')
    .filter({ hasText: "❤️" })
    .click();
  await alicePage.waitForFunction(
    () =>
      [...document.querySelectorAll("article")].some(
        (a) => a.textContent?.includes("Bob here") && a.textContent?.includes("❤️"),
      ),
    { timeout: 5_000 },
  );
  await alicePage.screenshot({ path: join(SHOTS, "05-reaction-applied.png") });

  // ── 6. Canvas ─────────────────────────────────────────────────────────────

  await alicePage.locator("button:has(.lucide-plus)").click();
  const canvasItem = alicePage
    .locator('[data-slot="dropdown-menu-item"]')
    .filter({ hasText: /canvas/i });
  await canvasItem.waitFor({ timeout: 3_000 });
  await canvasItem.click();
  await alicePage.waitForSelector('[data-testid="canvas"]', { timeout: 10_000 });

  const canvas = alicePage.locator('[data-testid="canvas"]').first();
  const box = await canvas.boundingBox();
  if (box) {
    await alicePage.mouse.move(box.x + 60, box.y + 60);
    await alicePage.mouse.down();
    await alicePage.mouse.move(box.x + 200, box.y + 60);
    await alicePage.mouse.move(box.x + 200, box.y + 160);
    await alicePage.mouse.up();
  }
  await alicePage.waitForTimeout(300);
  await alicePage.screenshot({ path: join(SHOTS, "06-canvas.png") });

  // ── 7. Profile panel ──────────────────────────────────────────────────────

  await aliceNav.click();
  const profileItem = alicePage.locator('[role="menuitem"]').filter({ hasText: "Profile" });
  await profileItem.waitFor({ timeout: 3_000 });
  await profileItem.click();
  await alicePage.waitForSelector('[data-slot="sheet-content"]', { timeout: 5_000 });
  await alicePage.waitForTimeout(300);
  await alicePage.screenshot({ path: join(SHOTS, "07-profile.png") });

  await alicePage.keyboard.press("Escape");
  await alicePage.waitForTimeout(300);

  // ── 8. Chat list ──────────────────────────────────────────────────────────

  await aliceNav.click();
  await alicePage
    .locator('[data-slot="dropdown-menu-item"]')
    .filter({ hasText: "Chat List" })
    .waitFor({ timeout: 3_000 });
  await alicePage
    .locator('[data-slot="dropdown-menu-item"]')
    .filter({ hasText: "Chat List" })
    .click();
  await alicePage.waitForTimeout(500);
  await alicePage.screenshot({ path: join(SHOTS, "08-chat-list.png") });

  await aliceContext.close();
  await bobContext.close();
});
