// A new browser of a user: waiting for approval, approval across a reload,
// cleared site data, and recovery by phrase or by passkey.
import { expect, test, type BrowserContext, type Page } from "@playwright/test";
import {
  addApprovedBrowser,
  createRoom,
  deviceIdOf,
  deviceRows,
  lockedBanner,
  message,
  newDevice,
  openDevices,
  openRoom,
  roomList,
  send,
  shownCode,
  signIn,
  signInHeading,
  signUp,
  T,
  unique,
  waitingBanner,
  type Device,
  reportPages,
} from "./support";

test.describe.configure({ timeout: 300_000 });
// Playwright reads fixtures from a destructured first argument; this hook needs none.
// eslint-disable-next-line no-empty-pattern
test.afterEach(async ({}, testInfo) => reportPages(testInfo));

/** Everything this origin keeps in the browser, as text. */
async function pageStorage(page: Page): Promise<string> {
  return page.evaluate(async () => {
    const text = (value: unknown): string => {
      if (value instanceof ArrayBuffer) return new TextDecoder().decode(value);
      if (ArrayBuffer.isView(value))
        return new TextDecoder().decode(
          new Uint8Array(value.buffer, value.byteOffset, value.byteLength),
        );
      if (Array.isArray(value)) return value.map(text).join("\n");
      if (value && typeof value === "object")
        return Object.entries(value)
          .map(([key, inner]) => `${key}=${text(inner)}`)
          .join("\n");
      return String(value);
    };
    const parts: string[] = [];
    for (const store of [localStorage, sessionStorage])
      for (let i = 0; i < store.length; i++) {
        const key = store.key(i)!;
        parts.push(key, store.getItem(key) ?? "");
      }
    for (const { name } of await indexedDB.databases()) {
      if (!name) continue;
      const db = await new Promise<IDBDatabase>((resolve, reject) => {
        const open = indexedDB.open(name);
        open.onsuccess = () => resolve(open.result);
        open.onerror = () => reject(open.error);
      });
      for (const storeName of Array.from(db.objectStoreNames)) {
        const all = await new Promise<unknown[]>((resolve, reject) => {
          const request = db.transaction(storeName).objectStore(storeName).getAll();
          request.onsuccess = () => resolve(request.result);
          request.onerror = () => reject(request.error);
        });
        parts.push(name, storeName, text(all));
      }
      db.close();
    }
    return parts.join("\n");
  });
}

/** Clears everything the browser keeps for the app's origin: storage and cookies. */
async function clearSiteData(device: Device): Promise<void> {
  const origin = new URL(device.page.url()).origin;
  // Off the app first, so no open page reacts to its storage going away.
  // The same tab keeps a virtual authenticator attached to it.
  const page = device.page;
  await page.goto("about:blank");
  const cdp = await device.context.newCDPSession(page);
  await cdp.send("Storage.clearDataForOrigin", { origin, storageTypes: "all" });
  await device.context.clearCookies();
  await page.goto("/");
  await expect(signInHeading(page)).toBeVisible({ timeout: T });
}

/** The user declines to create a passkey whenever the page asks. */
async function declinePasskeys(context: BrowserContext): Promise<void> {
  await context.addInitScript(() => {
    navigator.credentials.create = () =>
      Promise.reject(new DOMException("The user declined.", "NotAllowedError"));
  });
}

test("a browser waiting for approval lists its rooms by newest message, messages locked", async ({
  browser,
}) => {
  const name = unique("Ada");
  const a = await newDevice(browser);
  await signUp(a.page, name);
  const first = unique("First room");
  const second = unique("Second room");
  const third = unique("Third room");
  await createRoom(a.page, first);
  await createRoom(a.page, second);
  await createRoom(a.page, third);
  // Newest message first: the first room, then the second; the third has none.
  await openRoom(a.page, second);
  await send(a.page, unique("In the second room"));
  await openRoom(a.page, first);
  await send(a.page, unique("In the first room"));

  const b = await newDevice(browser);
  await signIn(b.page, name);
  await expect(waitingBanner(b.page).first()).toBeVisible({ timeout: T });
  const names = roomList(b.page).getByText(/^(First|Second|Third) room-/);
  await expect(names).toHaveText([first, second, third], { timeout: T });
  await roomList(b.page).getByText(first, { exact: true }).click();
  await expect(lockedBanner(b.page)).toBeVisible({ timeout: T });
  await expect(b.page.getByText("No messages yet")).toHaveCount(0);
});

test("approval survives a reload of the waiting browser after it answered", async ({ browser }) => {
  const name = unique("Ada");
  const a = await newDevice(browser);
  await signUp(a.page, name);
  const roomId = await createRoom(a.page, "Rehearsal");
  const text = unique("Soundcheck at six");
  await send(a.page, text);

  const b = await newDevice(browser);
  await signIn(b.page, name, `/?join=${roomId}`);
  await expect(waitingBanner(b.page).first()).toBeVisible({ timeout: T });
  const dialog = await openDevices(a.page);
  const row = deviceRows(dialog).filter({ hasText: "Waiting for approval" }).first();
  await row.getByRole("button", { name: "Approve" }).click();
  const code = await shownCode(b.page);
  // B reloads halfway: it has answered, A has not typed the code yet.
  await b.page.reload();
  await expect(waitingBanner(b.page).first()).toBeVisible({ timeout: T });
  await row.getByLabel("Code shown on the new browser").fill(code);
  await row.getByRole("button", { name: "Approve this code" }).click();
  await expect(waitingBanner(b.page)).toHaveCount(0, { timeout: T });
  await expect(message(b.page, text)).toBeVisible({ timeout: T });
});

test("cleared site data makes the browser a new device waiting for approval", async ({
  browser,
}) => {
  const name = unique("Ada");
  const a = await newDevice(browser);
  await signUp(a.page, name);
  const roomId = await createRoom(a.page, "Rehearsal");
  await send(a.page, unique("Soundcheck at six"));
  const b = await addApprovedBrowser(browser, a.page, name);
  const before = await deviceIdOf(b.page);

  await clearSiteData(b);
  await signIn(b.page, name, `/?join=${roomId}`);
  await expect(waitingBanner(b.page).first()).toBeVisible({ timeout: T });
  await expect(lockedBanner(b.page)).toBeVisible({ timeout: T });
  const panel = await openDevices(a.page);
  await expect(deviceRows(panel).filter({ hasText: "Waiting for approval" })).toHaveCount(1, {
    timeout: T,
  });
  await expect(deviceRows(panel).and(panel.locator(`[data-device-id="${before}"]`))).toHaveCount(1);
});

test("recovers by phrase on a new browser with no other device online; the phrase is kept nowhere", async ({
  browser,
}) => {
  const name = unique("Ada");
  const a = await newDevice(browser);
  await declinePasskeys(a.context);
  await signUp(a.page, name);
  const roomId = await createRoom(a.page, "Rehearsal");
  const text = unique("Soundcheck at six");
  await send(a.page, text);

  const panel = await openDevices(a.page);
  await panel.getByRole("button", { name: "Set up recovery" }).click();
  const shown = panel.locator("[data-recovery-phrase]");
  await expect(shown).toBeVisible({ timeout: T });
  const phrase = (await shown.textContent())!.trim();
  expect(phrase.split(/\s+/)).toHaveLength(24);
  await panel.getByRole("button", { name: "I've written it down" }).click();
  await expect(panel.locator("[data-recovery-phrase]")).toHaveCount(0);
  await expect(panel.getByText("Recovery is set up for your account.")).toBeVisible({
    timeout: T,
  });
  const words = phrase.split(/\s+/).slice(0, 6).join(" ");
  expect(await pageStorage(a.page)).not.toContain(words);
  expect(a.wire.saw(words)).toBe(false);
  // The only active device goes away.
  await a.context.close();

  const b = await newDevice(browser);
  await signIn(b.page, name, `/?join=${roomId}`);
  await expect(waitingBanner(b.page).first()).toBeVisible({ timeout: T });
  const restore = await openDevices(b.page);
  await restore.getByLabel("Recovery phrase").fill(phrase);
  await restore.getByRole("button", { name: "Restore with phrase" }).click();
  await expect(waitingBanner(b.page)).toHaveCount(0, { timeout: T });
  await expect(restore.getByLabel("Recovery phrase")).toHaveCount(0);
  await b.page.keyboard.press("Escape");
  await expect(message(b.page, text)).toBeVisible({ timeout: T });

  expect(await pageStorage(b.page)).not.toContain(words);
  expect(b.wire.saw(words)).toBe(false);
});

test("recovers by passkey after the only browser's site data is cleared", async ({ browser }) => {
  const name = unique("Ada");
  const a = await newDevice(browser);
  const cdp = await a.context.newCDPSession(a.page);
  await cdp.send("WebAuthn.enable");
  await cdp.send("WebAuthn.addVirtualAuthenticator", {
    options: {
      protocol: "ctap2",
      transport: "internal",
      hasResidentKey: true,
      hasUserVerification: true,
      isUserVerified: true,
      automaticPresenceSimulation: true,
    },
  });
  await signUp(a.page, name);
  const roomId = await createRoom(a.page, "Rehearsal");
  const text = unique("Soundcheck at six");
  await send(a.page, text);

  const panel = await openDevices(a.page);
  await panel.getByRole("button", { name: "Set up recovery" }).click();
  await expect(panel.getByText(/Your recovery key is saved in a passkey/)).toBeVisible({
    timeout: T,
  });
  await expect(panel.locator("[data-recovery-phrase]")).toHaveCount(0);

  await clearSiteData(a);
  await signIn(a.page, name, `/?join=${roomId}`);
  await expect(waitingBanner(a.page).first()).toBeVisible({ timeout: T });
  await expect(lockedBanner(a.page)).toBeVisible({ timeout: T });
  const restore = await openDevices(a.page);
  await restore.getByRole("button", { name: "Restore with passkey" }).click();
  await expect(waitingBanner(a.page)).toHaveCount(0, { timeout: T });
  await a.page.keyboard.press("Escape");
  await expect(message(a.page, text)).toBeVisible({ timeout: T });
});
