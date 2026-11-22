// Two people in a room: what the server and the wire see, removing a
// member, and when a read marker is written.
import { expect, test } from "@playwright/test";
import {
  admit,
  closeDialog,
  createRoom,
  deviceIdOf,
  joinThroughLink,
  lockedBanner,
  message,
  myAccountId,
  newDevice,
  reopen,
  roomList,
  send,
  signIn,
  signUp,
  T,
  unique,
  until,
  waitingBanner,
  reportPages,
} from "./support";
import { serverMarkerCount, serverRead, serverSuccessors, storedBytes } from "./server-snapshot";

test.describe.configure({ timeout: 300_000 });
// Playwright reads fixtures from a destructured first argument; this hook needs none.
// eslint-disable-next-line no-empty-pattern
test.afterEach(async ({}, testInfo) => reportPages(testInfo));

test("no message text or attachment reaches the server or the wire; a room name does", async ({
  browser,
}) => {
  const a = await newDevice(browser);
  const b = await newDevice(browser);
  const alice = unique("Alice");
  const bob = unique("Bob");
  await signUp(a.page, alice);
  const roomName = unique("Rehearsal room");
  const roomId = await createRoom(a.page, roomName);
  await signUp(b.page, bob);
  await joinThroughLink(a.page, b.page, roomId, bob);

  const fromAlice = unique("Soundcheck at six");
  const fromBob = unique("Bringing the spare amp");
  const fileName = `${unique("setlist")}.txt`;
  const fileText = unique("Opening song: the slow one");
  await send(a.page, fromAlice);
  await a.page.getByLabel("Attachment").setInputFiles({
    name: fileName,
    mimeType: "text/plain",
    buffer: Buffer.from(fileText),
  });
  await a.page.getByRole("button", { name: "Send", exact: true }).click();
  await expect(a.page.locator(".message-bubble").getByText(fileName).first()).toBeVisible({
    timeout: T,
  });

  await expect(message(b.page, fromAlice)).toBeVisible({ timeout: T });
  await expect(b.page.locator(".message-bubble").getByText(fileName).first()).toBeVisible({
    timeout: T,
  });
  await send(b.page, fromBob);
  await expect(message(a.page, fromAlice)).toBeVisible();
  await expect(message(a.page, fromBob)).toBeVisible({ timeout: T });
  // Both readers' markers are written (their stored time is encrypted).
  const aliceId = await myAccountId(a.page);
  const bobId = await myAccountId(b.page);
  await until(
    "both read markers",
    () => serverMarkerCount(roomId, aliceId),
    (n) => n === 1,
  );
  await until(
    "both read markers",
    () => serverMarkerCount(roomId, bobId),
    (n) => n === 1,
  );

  const secrets = [fromAlice, fromBob, fileName, fileText];
  // The control: a plain column is found, so a miss below means sealed bytes.
  await until(
    "the room name on disk",
    () => storedBytes(),
    (bytes) => bytes.includes(roomName),
  );
  const stored = await storedBytes();
  for (const secret of secrets) expect(stored.includes(secret), secret).toBe(false);
  for (const device of [a, b]) {
    expect(device.wire.saw(roomName)).toBe(true);
    for (const secret of secrets) expect(device.wire.saw(secret), secret).toBe(false);
  }
});

test("removing a member renews the room's key at once; they read nothing written later", async ({
  browser,
}) => {
  const a = await newDevice(browser);
  const b = await newDevice(browser);
  const alice = unique("Alice");
  const bob = unique("Bob");
  await signUp(a.page, alice);
  const roomName = unique("Rehearsal");
  const roomId = await createRoom(a.page, roomName);
  const first = unique("Before the removal");
  await send(a.page, first);
  await signUp(b.page, bob);
  await joinThroughLink(a.page, b.page, roomId, bob);
  await expect(message(b.page, first)).toBeVisible({ timeout: T });

  // Every automatic marker write has settled: nobody writes from here on.
  const aliceId = await myAccountId(a.page);
  const bobId = await myAccountId(b.page);
  await until(
    "alice's marker",
    () => serverMarkerCount(roomId, aliceId),
    (n) => n === 1,
  );
  await until(
    "bob's marker",
    () => serverMarkerCount(roomId, bobId),
    (n) => n === 1,
  );
  const before = await serverSuccessors(roomId);

  await a.page.getByRole("button", { name: /^Invite( \d+)?$/ }).click();
  const dialog = a.page.getByRole("dialog");
  await dialog
    .locator(`[data-member-author="${bobId}"]`)
    .getByRole("button", { name: "Remove" })
    .click();
  await expect(dialog.locator(`[data-member-author="${bobId}"]`)).toHaveCount(0, { timeout: T });
  await expect(dialog.getByText(/key is not renewed yet/)).toHaveCount(0);

  // With nobody writing, the server holds a new key version for the room
  // that names the old one.
  const after = await until(
    "a new key version for the room",
    () => serverSuccessors(roomId),
    (rows) => rows.length > before.length,
  );
  expect(after.length).toBe(before.length + 1);
  await expect(dialog.getByText(/key is not renewed yet/)).toHaveCount(0);
  await closeDialog(a.page);

  const later = unique("After the removal");
  await send(a.page, later);
  await until(
    "the later message on the server",
    () => serverRead(async ({ jazz, app }) => jazz.db.all(app.messages.where({ roomId }))),
    (rows) => rows.length === 2,
  );
  // The room leaves bob's list; nothing written later shows, also after a reload.
  await expect(roomList(b.page).getByText(roomName, { exact: true })).toHaveCount(0, {
    timeout: T,
  });
  await expect(message(b.page, later)).toHaveCount(0);
  await b.page.reload();
  await expect(b.page.getByRole("button", { name: "Account" })).toBeVisible({ timeout: T });
  await b.page.goto(`/?join=${roomId}`);
  await expect(b.page.getByRole("button", { name: "Ask to join" })).toBeVisible({ timeout: T });
  await expect(message(b.page, later)).toHaveCount(0);
});

test("a read marker is written only once this browser can read the room", async ({ browser }) => {
  const a = await newDevice(browser);
  const bobFirst = await newDevice(browser);
  const alice = unique("Alice");
  const bob = unique("Bob");
  await signUp(a.page, alice);
  const roomId = await createRoom(a.page, unique("Rehearsal"));
  const text = unique("Soundcheck at six");
  await send(a.page, text);

  // Bob asks to join, then closes his only browser: no browser of his
  // holds the room's key until it opens again.
  await signUp(bobFirst.page, bob);
  // His first browser is his account's one active device.
  await deviceIdOf(bobFirst.page);
  await bobFirst.page.goto(`/?join=${roomId}`);
  await bobFirst.page.getByRole("button", { name: "Ask to join" }).click();
  await expect(bobFirst.page.getByText("Waiting for the room creator")).toBeVisible({
    timeout: T,
  });
  await until(
    "bob's join request on the server",
    () => serverRead(async ({ jazz, app }) => jazz.db.all(app.joinRequests.where({ roomId }))),
    (rows) => rows.length === 1,
  );
  await bobFirst.page.close();
  await admit(a.page, bob);
  await a.page.getByRole("button", { name: /^Invite( \d+)?$/ }).click();
  const bobId = (await a.page
    .getByRole("dialog")
    .locator("[data-member-author]", { hasNotText: "(you)" })
    .getAttribute("data-member-author"))!;
  await closeDialog(a.page);

  // A second browser of Bob's, waiting for approval: the room is listed,
  // its message locked, and no marker is written or reported as failing.
  const bobSecond = await newDevice(browser);
  await signIn(bobSecond.page, bob, `/?join=${roomId}`);
  await expect(waitingBanner(bobSecond.page).first()).toBeVisible({ timeout: T });
  await expect(lockedBanner(bobSecond.page)).toBeVisible({ timeout: T });
  for (let check = 0; check < 5; check++) {
    expect(await serverMarkerCount(roomId, bobId)).toBe(0);
    await bobSecond.page.waitForTimeout(2000);
  }
  await expect(bobSecond.page.getByText(/Could not|refused/i)).toHaveCount(0);

  // His first browser opens again: the key arrives, and the room is marked read.
  const page = await reopen(bobFirst);
  await page.goto(`/?join=${roomId}`);
  await expect(message(page, text)).toBeVisible({ timeout: T });
  await until(
    "bob's marker",
    () => serverMarkerCount(roomId, bobId),
    (n) => n === 1,
  );
});
