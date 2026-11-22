// One user's browsers: removing a lost device, signing out and back in,
// leaving a browser. Each browser has its own storage profile, so each is
// its own device with its own keys.
import { expect, test } from "@playwright/test";
import {
  addApprovedBrowser,
  closeDialog,
  createRoom,
  deviceIdOf,
  deviceRows,
  hasSession,
  lockedBanner,
  message,
  messageId,
  newDevice,
  openDevices,
  openRoom,
  reopen,
  send,
  signIn,
  signInHeading,
  signOut,
  signUp,
  T,
  thisBrowserRow,
  unique,
  waitingBanner,
  reportPages,
} from "./support";

test.describe.configure({ timeout: 300_000 });
// Playwright reads fixtures from a destructured first argument; this hook needs none.
// eslint-disable-next-line no-empty-pattern
test.afterEach(async ({}, testInfo) => reportPages(testInfo));

test("removes a lost device: every other sign-in ends first, the kept browser needs no approval", async ({
  browser,
}) => {
  const name = unique("Ada");
  const a = await newDevice(browser);
  await signUp(a.page, name);
  const roomId = await createRoom(a.page, "Rehearsal");
  const before = unique("Before the removal");
  await send(a.page, before);

  const b = await addApprovedBrowser(browser, a.page, name);
  const c = await addApprovedBrowser(browser, a.page, name);
  for (const device of [b, c]) {
    await openRoom(device.page, "Rehearsal");
    await expect(message(device.page, before)).toBeVisible({ timeout: T });
  }
  const bId = await deviceIdOf(b.page);
  const cId = await deviceIdOf(c.page);

  // The server step must have closed every connection of B and C before
  // A's page hears back from it, and so before `devices.remove` runs.
  let routeCalls = 0;
  let closedBeforeAnswer = false;
  await a.page.route("**/api/end-other-sign-ins", async (route) => {
    routeCalls++;
    const response = await route.fetch();
    await expect
      .poll(() => b.wire.openSockets() + c.wire.openSockets(), { timeout: T })
      .toBe(0)
      .then(() => (closedBeforeAnswer = true))
      .catch(() => undefined);
    await route.fulfill({ response });
  });
  expect(b.wire.openSockets()).toBeGreaterThan(0);
  expect(c.wire.openSockets()).toBeGreaterThan(0);

  const dialog = await openDevices(a.page);
  const target = deviceRows(dialog).and(dialog.locator(`[data-device-id="${bId}"]`));
  await target.getByRole("button", { name: "Remove lost device" }).click();
  const confirm = dialog.getByRole("alertdialog");
  // Before anything happens, the screen says what it costs.
  await expect(confirm).toContainText(/is signed out first and has to sign in again/);
  await expect(confirm).toContainText(/keep their keys and need no new approval/);
  await expect(confirm).toContainText(/keeps the messages it already has/);
  await confirm.getByRole("button", { name: "Sign out other browsers and remove" }).click();
  await expect(dialog.locator(`[data-device-id="${bId}"]`)).toHaveCount(0, { timeout: T });
  expect(routeCalls).toBe(1);
  expect(closedBeforeAnswer).toBe(true);
  // C stays listed: it keeps its keys and its device record.
  await expect(dialog.locator(`[data-device-id="${cId}"]`)).toHaveCount(1);
  await closeDialog(a.page);

  // B and C land on the sign-in form by themselves, with no session left.
  for (const device of [b, c]) {
    await expect(signInHeading(device.page)).toBeVisible({ timeout: T });
    expect(await hasSession(device)).toBe(false);
  }

  // A's own connection came back on its cookie, with no new sign-in.
  const after = unique("After the removal");
  await send(a.page, after);
  const afterId = await messageId(a.page, after);
  // The ID travels in plain bytes (A's own traffic carries it); none reached B.
  await expect.poll(() => a.wire.sawId(afterId), { timeout: T }).toBe(true);
  expect(b.wire.sawId(afterId)).toBe(false);

  // C signs in again: the same device, active at once, reading everything.
  await signIn(c.page, name, `/?join=${roomId}`);
  await expect(message(c.page, before)).toBeVisible({ timeout: T });
  await expect(message(c.page, after)).toBeVisible({ timeout: T });
  await expect(waitingBanner(c.page)).toHaveCount(0);
  expect(await deviceIdOf(c.page)).toBe(cId);

  // B signs in again. Its runtime enrols it again under a new ID, waiting
  // for approval. It may still show what it could already read, but nothing
  // written after its removal.
  await signIn(b.page, name, `/?join=${roomId}`);
  await expect(waitingBanner(b.page).first()).toBeVisible({ timeout: T });
  const list = await openDevices(a.page);
  await expect(list.locator(`[data-device-id="${bId}"]`)).toHaveCount(0);
  const replacement = deviceRows(list).filter({ hasText: "Waiting for approval" });
  await expect(replacement).toHaveCount(1, { timeout: T });
  expect(await replacement.getAttribute("data-device-id")).not.toBe(bId);
  await closeDialog(a.page);
  await expect(lockedBanner(b.page)).toBeVisible({ timeout: T });
  await expect(message(b.page, after)).toHaveCount(0);

  // Once reloaded, the re-enrolled browser holds none of its old keys:
  // everything is locked, nothing is shown as absent.
  await b.page.reload();
  await expect(waitingBanner(b.page).first()).toBeVisible({ timeout: T });
  await expect(b.page.getByText(/2 messages are locked on this browser/)).toBeVisible({
    timeout: T,
  });
  await expect(message(b.page, before)).toHaveCount(0);
  await expect(message(b.page, after)).toHaveCount(0);
  await expect(b.page.getByText("No messages yet")).toHaveCount(0);
});

test("an interrupted lost-device removal is repeated, both steps again", async ({ browser }) => {
  const name = unique("Ada");
  const a = await newDevice(browser);
  await signUp(a.page, name);
  await createRoom(a.page, "Rehearsal");
  const b = await addApprovedBrowser(browser, a.page, name);
  const bId = await deviceIdOf(b.page);

  // The page goes away after the server step and before the device step.
  let routeCalls = 0;
  await a.page.route("**/api/end-other-sign-ins", async (route) => {
    routeCalls++;
    const response = await route.fetch();
    if (routeCalls === 1) await route.abort();
    else await route.fulfill({ response });
  });
  let dialog = await openDevices(a.page);
  await dialog
    .locator(`[data-device-id="${bId}"]`)
    .getByRole("button", { name: "Remove lost device" })
    .click();
  await dialog
    .getByRole("alertdialog")
    .getByRole("button", { name: "Sign out other browsers and remove" })
    .click();
  await expect(signInHeading(b.page)).toBeVisible({ timeout: T });
  await a.page.reload();

  dialog = await openDevices(a.page);
  const row = dialog.locator(`[data-device-id="${bId}"]`);
  await expect(row).toHaveCount(1, { timeout: T });
  await row.getByRole("button", { name: "Remove lost device" }).click();
  await dialog
    .getByRole("alertdialog")
    .getByRole("button", { name: "Sign out other browsers and remove" })
    .click();
  await expect(row).toHaveCount(0, { timeout: T });
  expect(routeCalls).toBe(2);
});

test("signs out and back in: the browser keeps its keys and needs no approval", async ({
  browser,
}) => {
  const name = unique("Ada");
  const a = await newDevice(browser);
  await signUp(a.page, name);
  const roomId = await createRoom(a.page, "Rehearsal");
  const earlier = unique("Earlier");
  await send(a.page, earlier);
  const b = await addApprovedBrowser(browser, a.page, name);
  const bId = await deviceIdOf(b.page);

  await signOut(b.page);
  expect(await hasSession(b)).toBe(false);
  const list = await openDevices(a.page);
  await expect(list.locator(`[data-device-id="${bId}"]`)).toHaveCount(1);
  await closeDialog(a.page);

  await signIn(b.page, name, `/?join=${roomId}`);
  await expect(message(b.page, earlier)).toBeVisible({ timeout: T });
  await expect(waitingBanner(b.page)).toHaveCount(0);
  expect(await deviceIdOf(b.page)).toBe(bId);
});

test("a sign-out the page did not see: the cold load signs in as the same device", async ({
  browser,
}) => {
  const name = unique("Ada");
  const a = await newDevice(browser);
  await signUp(a.page, name);
  const roomId = await createRoom(a.page, "Rehearsal");
  const earlier = unique("Earlier");
  await send(a.page, earlier);
  const b = await addApprovedBrowser(browser, a.page, name);
  const bId = await deviceIdOf(b.page);

  // B's tab closes; meanwhile its sign-in is ended on the server.
  await b.page.close();
  const revoked = await a.context.request.post("/api/auth/revoke-other-sessions", {
    headers: { origin: new URL(a.page.url()).origin },
    data: {},
  });
  expect(revoked.ok()).toBe(true);
  expect(await hasSession(b)).toBe(false);

  const page = await reopen(b);
  await page.goto(`/?join=${roomId}`);
  await expect(signInHeading(page)).toBeVisible({ timeout: T });
  await signIn(page, name, `/?join=${roomId}`);
  await expect(message(page, earlier)).toBeVisible({ timeout: T });
  await expect(waitingBanner(page)).toHaveCount(0);
  expect(await deviceIdOf(page)).toBe(bId);
});

test("another account on the same browser sees nothing of the first", async ({ browser }) => {
  const alice = unique("Alice");
  const bob = unique("Bob");
  const shared = await newDevice(browser);
  await signUp(shared.page, alice);
  const roomId = await createRoom(shared.page, `Alice's room ${alice}`);
  const secret = unique("Only for Alice");
  await send(shared.page, secret);
  await signOut(shared.page);

  await signUp(shared.page, bob);
  await expect(shared.page.getByText(`Alice's room ${alice}`)).toHaveCount(0);
  await shared.page.goto(`/?join=${roomId}`);
  await expect(shared.page.getByRole("button", { name: "Ask to join" })).toBeVisible({
    timeout: T,
  });
  await expect(message(shared.page, secret)).toHaveCount(0);
  await signOut(shared.page);

  await signIn(shared.page, alice, `/?join=${roomId}`);
  await expect(message(shared.page, secret)).toBeVisible({ timeout: T });
  await expect(waitingBanner(shared.page)).toHaveCount(0);
});

test("leaves this browser: another device completes it and the browser signs itself out", async ({
  browser,
}) => {
  const name = unique("Ada");
  const a = await newDevice(browser);
  await signUp(a.page, name);
  const roomId = await createRoom(a.page, "Rehearsal");
  const earlier = unique("Earlier");
  await send(a.page, earlier);
  const b = await addApprovedBrowser(browser, a.page, name);
  const bOld = await deviceIdOf(b.page);

  const panel = await openDevices(b.page);
  await thisBrowserRow(panel).getByRole("button", { name: "Leave this browser" }).click();
  await panel.getByRole("alertdialog").getByRole("button", { name: "Leave this browser" }).click();
  await expect(panel.getByText("Removal pending").first()).toBeVisible({ timeout: T });
  await closeDialog(b.page);

  // A is online and does nothing: it completes the removal by itself.
  await expect(signInHeading(b.page)).toBeVisible({ timeout: 90_000 });
  expect(await hasSession(b)).toBe(false);

  const list = await openDevices(a.page);
  await expect(list.locator(`[data-device-id="${bOld}"]`)).toHaveCount(0, { timeout: T });
  const waiting = deviceRows(list).filter({ hasText: "Waiting for approval" });
  await expect(waiting).toHaveCount(1, { timeout: T });
  await expect(waiting.getByRole("button", { name: "Remove lost device" })).toHaveCount(0);
  const replacement = await waiting.getAttribute("data-device-id");
  expect(replacement).not.toBe(bOld);
  await closeDialog(a.page);

  // Signed in again, B is that waiting device, its messages locked.
  await signIn(b.page, name, `/?join=${roomId}`);
  await expect(waitingBanner(b.page).first()).toBeVisible({ timeout: T });
  await expect(lockedBanner(b.page)).toBeVisible({ timeout: T });
  await expect(message(b.page, earlier)).toHaveCount(0);
  // A still lists one waiting device, the same one: B did not enrol again.
  const again = await openDevices(a.page);
  await expect(deviceRows(again).filter({ hasText: "Waiting for approval" })).toHaveCount(1);
  await expect(again.locator(`[data-device-id="${replacement}"]`)).toHaveCount(1);
});

test("leaves across a reload: the request is completed while the page is closed", async ({
  browser,
}) => {
  const name = unique("Ada");
  const a = await newDevice(browser);
  await signUp(a.page, name);
  await createRoom(a.page, "Rehearsal");
  const b = await addApprovedBrowser(browser, a.page, name);

  // A is closed when B asks to leave.
  await a.page.close();
  const panel = await openDevices(b.page);
  await thisBrowserRow(panel).getByRole("button", { name: "Leave this browser" }).click();
  await panel.getByRole("alertdialog").getByRole("button", { name: "Leave this browser" }).click();
  await expect(panel.getByText("Removal pending").first()).toBeVisible({ timeout: T });
  await b.page.close();

  // A opens again and completes the removal; B is still closed.
  const aPage = await reopen(a);
  await aPage.goto("/");
  await expect(aPage.getByRole("button", { name: "Account" })).toBeVisible({ timeout: T });
  const list = await openDevices(aPage);
  await expect(deviceRows(list).filter({ hasText: "Waiting for approval" })).toHaveCount(0);
  await expect(deviceRows(list)).toHaveCount(1, { timeout: 90_000 });
  await closeDialog(aPage);

  // B opens again, still signed in, and ends on the sign-in form.
  expect(await hasSession(b)).toBe(true);
  const bPage = await reopen(b);
  await bPage.goto("/");
  await expect(signInHeading(bPage)).toBeVisible({ timeout: 90_000 });
  expect(await hasSession(b)).toBe(false);
});

test("a sole device cannot leave, and its messages stay readable", async ({ browser }) => {
  const name = unique("Ada");
  const a = await newDevice(browser);
  await signUp(a.page, name);
  await createRoom(a.page, "Rehearsal");
  const text = unique("Still readable");
  await send(a.page, text);

  const panel = await openDevices(a.page);
  await thisBrowserRow(panel).getByRole("button", { name: "Leave this browser" }).click();
  await panel.getByRole("alertdialog").getByRole("button", { name: "Leave this browser" }).click();
  await expect(panel.getByText(/This is your only device, so it can't leave/)).toBeVisible({
    timeout: T,
  });
  await expect(panel.getByText("Removal pending")).toHaveCount(0);
  await closeDialog(a.page);
  await expect(message(a.page, text)).toBeVisible();
  expect(await a.page.evaluate(() => localStorage.getItem("band-chat:leaving"))).toBeNull();
});
