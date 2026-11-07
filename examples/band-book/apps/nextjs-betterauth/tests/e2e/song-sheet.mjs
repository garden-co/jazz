// Browser receipt for the song sheet editor: `pnpm dev` in one terminal, then
// `pnpm test:e2e:sheet`. Two people edit one sheet at once: edits merge, each
// sees the other's cursor, and comments stay on their words. Set
// SCREENSHOTS=<dir> to also capture the sheet at 1280 and 375 px in light and
// dark mode.
import assert from "node:assert/strict";
import { mkdir } from "node:fs/promises";
import { join } from "node:path";
import { chromium } from "@playwright/test";

const origin = process.env.APP_ORIGIN ?? "http://127.0.0.1:3000";
const screenshots = process.env.SCREENSHOTS;
const run = Date.now().toString(36);
const timeout = 60_000;
const SHEET = "Night bus home (song sheet)";

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
const sheet = (page) => page.locator(".bb-sheet .ProseMirror");

/** Puts the caret right after the first occurrence of `text` in the sheet. */
async function caretAfter(page, text) {
  await sheet(page).evaluate((root, text) => {
    const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT);
    for (let node = walker.nextNode(); node; node = walker.nextNode()) {
      const at = node.textContent.indexOf(text);
      if (at < 0) continue;
      const range = document.createRange();
      range.setStart(node, at + text.length);
      range.collapse(true);
      const selection = window.getSelection();
      selection.removeAllRanges();
      selection.addRange(range);
      return;
    }
    throw new Error(`"${text}" is not in the sheet`);
  }, text);
  await sheet(page).focus();
}

/** Selects the first occurrence of `text` in the sheet. */
async function select(page, text) {
  await sheet(page).evaluate((root, text) => {
    const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT);
    for (let node = walker.nextNode(); node; node = walker.nextNode()) {
      const at = node.textContent.indexOf(text);
      if (at < 0) continue;
      const range = document.createRange();
      range.setStart(node, at);
      range.setEnd(node, at + text.length);
      const selection = window.getSelection();
      selection.removeAllRanges();
      selection.addRange(range);
      return;
    }
    throw new Error(`"${text}" is not in the sheet`);
  }, text);
  await sheet(page).focus();
}

/** The sheet's text without other people's name labels. */
function textOf(selector) {
  return [...document.querySelectorAll(selector)]
    .map((element) => {
      const copy = element.cloneNode(true);
      copy.querySelectorAll(".jazz-presence-caret").forEach((caret) => caret.remove());
      return copy.textContent;
    })
    .join("");
}

async function waitForText(page, text, selector = ".bb-sheet .ProseMirror") {
  await page.waitForFunction(
    ({ text, selector, textOf }) => new Function(`return ${textOf}`)()(selector).includes(text),
    { text, selector, textOf: textOf.toString() },
    { timeout },
  );
}

try {
  const ownerContext = await browser.newContext({ viewport: { width: 1280, height: 800 } });
  const owner = await signUp(ownerContext, "Ada");

  // The seeded sheet renders as rich text with its three comments in place.
  await sidebarItem(owner, "Songs").click();
  await sidebarItem(owner, SHEET).click();
  await sheet(owner).waitFor();
  await owner.locator(".bb-sheet h2", { hasText: "Chorus" }).waitFor();
  await owner.locator(".bb-sheet pre", { hasText: "C#m" }).waitFor();
  await owner.locator(".bb-sheet-comment", { hasText: "fogged and gold" }).waitFor();
  assert.equal((await owner.locator(".bb-sheet-comment").count()) >= 3, true, "comments are drawn");

  // Share the sheet with a guest editor.
  await owner.getByRole("button", { name: "Share" }).click();
  await owner.getByRole("button", { name: "Create link" }).click();
  const linkInput = owner.getByRole("dialog").getByRole("textbox").last();
  await linkInput.waitFor();
  const inviteUrl = await linkInput.inputValue();
  await owner.keyboard.press("Escape");

  const guestContext = await browser.newContext({ viewport: { width: 1280, height: 800 } });
  const guest = await signUp(guestContext, "Bo");
  await guest.goto(inviteUrl);
  await guest.waitForURL(/p=/);
  await sheet(guest).waitFor();
  await waitForText(guest, "Ride it to the end of the line");

  // Both type at once in different places: the authority merges the two
  // versions and both editors end up with both edits.
  await caretAfter(owner, "Every stop is a reason to stay");
  await caretAfter(guest, "palm muted");
  await Promise.all([owner.keyboard.type(" tonight"), guest.keyboard.type(", then a bass slide")]);
  for (const page of [owner, guest]) {
    await waitForText(page, "a reason to stay tonight");
    await waitForText(page, "palm muted, then a bass slide");
  }

  // Longer bursts from both at once, typed faster than a round trip.
  const ownerLine = " and the driver hums along to the radio, every word of it";
  const guestLine = ", then the whole band comes crashing in on the downbeat";
  await caretAfter(owner, "against the cold");
  await caretAfter(guest, "drums on the second line");
  await Promise.all([owner.keyboard.type(ownerLine), guest.keyboard.type(guestLine)]);
  for (const page of [owner, guest]) {
    await waitForText(page, `against the cold${ownerLine}`);
    await waitForText(page, `drums on the second line${guestLine}`);
  }

  // Each sees the other's cursor, labelled with their name.
  await guest.locator(".jazz-presence-caret", { hasText: "Ada" }).waitFor();
  await owner.locator(".jazz-presence-caret", { hasText: "Bo" }).waitFor();

  // A selection shows up as a highlight in the other editor.
  await select(guest, "Decide on a bridge");
  await owner.locator(".jazz-presence-selection", { hasText: "Decide on a bridge" }).waitFor();

  // The guest comments on their selection; the owner sees it straight away.
  await guest.getByRole("textbox", { name: "New comment" }).fill("I have a bridge idea in D.");
  await guest.getByRole("button", { name: "Comment" }).click();
  await owner.getByText("I have a bridge idea in D.").waitFor();
  await owner.locator(".bb-sheet-comment", { hasText: "Decide on a bridge" }).waitFor();

  // Editing commented words keeps the comment on them.
  await caretAfter(owner, "Decide on a");
  await owner.keyboard.type(" short");
  await waitForText(guest, "Decide on a short bridge");
  await waitForText(guest, "Decide on a short bridge", ".bb-sheet-comment");

  await shoot(owner, "song-sheet");
  await shoot(guest, "song-sheet-guest");
  console.log("BandBook song sheet receipt passed");
} catch (error) {
  for (const [index, context] of browser.contexts().entries()) {
    for (const page of context.pages()) {
      const text = await page
        .locator(".bb-sheet .ProseMirror")
        .innerText()
        .catch(() => "(no sheet)");
      console.error(`--- context ${index}: ${page.url()}\n${text}`);
      if (screenshots) await page.screenshot({ path: join(screenshots, `failure-${index}.png`) });
    }
  }
  throw error;
} finally {
  await browser.close();
}
