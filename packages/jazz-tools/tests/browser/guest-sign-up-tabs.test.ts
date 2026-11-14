import { afterEach, expect, it } from "vitest";
import { deploy } from "./deploy.js";
import { openBrowserSessionCore as createJazzClient } from "../../src/session/create-jazz-session.js";
import { createDb as createLocalFirstDb } from "../../src/runtime/default-create-db.js";
import { isThinDb } from "../../src/runtime/local-first-upgrade.js";
import { resolveDefaultPersistentDbName } from "../../src/runtime/db.js";
import { vfsPages, vfsPath } from "./idb-vfs.js";
import { uniqueDbName, waitForCondition } from "./support.js";
import { openSessionTab } from "./session-tab.js";
import {
  sessionTabApp as app,
  sessionTabPermissions as permissions,
} from "./session-tab-schema.js";
import {
  getJazzServerInfo,
  jazzAppFetch,
  jazzAppVisitor,
  signOutOfJazzApp,
  signUpAtJazzApp,
  startJazzApp,
} from "./testing-server.js";

// A local-first guest with two tabs open signs up in one of them (rc.6 G2):
// the app's own sign-up, then `linkWithBackend()` and `reauthenticate()`.
// Both tabs go on as the user, with the guest's data, and keep syncing. The
// tab that signed up once opened its new client on a shared worker that
// refused it (another sign-in than the other tab's), so its link stopped, it
// read nothing, and its next write never uploaded.

const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup().catch(() => {});
});

/**
 * This page is the tab that signs up; another tab of its origin is open too.
 * Both are the device's local-first guest, local-first once loaded. The guest
 * writes `guest-1`, then signs up here while the other tab doesn't hear of it
 * yet (its backend requests are held), so it stays the guest.
 */
async function signUpWhileAnotherTabIsTheGuest(name: string, clearOnSignOut: boolean) {
  const { appId, serverUrl: coreUrl, adminSecret } = await getJazzServerInfo(uniqueDbName(name));
  await deploy({ appId, serverUrl: coreUrl, adminSecret, schema: app.wasmSchema, permissions });
  const backend = await startJazzApp(
    { appId, serverUrl: coreUrl, adminSecret },
    { schema: app.wasmSchema, permissions },
  );
  const visitorUrl = await jazzAppVisitor(backend);
  // The backend answers no CORS: send the session's own requests to it from Node.
  const pageFetch = globalThis.fetch;
  globalThis.fetch = ((input: RequestInfo | URL, init?: RequestInit) => {
    const url = input instanceof Request ? input.url : String(input);
    return url.startsWith(new URL(visitorUrl).origin)
      ? jazzAppFetch(input, init)
      : pageFetch(input, init);
  }) as typeof fetch;
  cleanups.push(async () => {
    globalThis.fetch = pageFetch;
  });

  const signingUp = await createJazzClient({
    serverUrl: visitorUrl,
    fetch: jazzAppFetch,
    initial: "device",
    clearOnSignOut,
    client: async () => ({ createClient: createLocalFirstDb }),
  });
  cleanups.push(() => signingUp.close());
  await waitForCondition(
    async () => {
      const client = signingUp.getSnapshot().client;
      return !!client && !isThinDb(client.db);
    },
    30_000,
    "The tab should upgrade to local-first",
  );
  const other = await openSessionTab("other", visitorUrl, { clearOnSignOut });
  cleanups.push(() => other.close());
  const guest = signingUp.getSnapshot().account!.id;
  expect((await other.state()).account).toBe(guest);
  await signingUp
    .getSnapshot()
    .client!.db.insert(app.tasks, { title: "guest-1" })
    .wait({ tier: "global" });

  await other.holdBackend(true);
  // The app's own sign-up, then as its sign-up handler goes on.
  await signUpAtJazzApp(backend, visitorUrl, "erin");
  await signingUp.linkWithBackend();
  await signingUp.reauthenticate();
  expect(signingUp.getSnapshot()).toMatchObject({
    status: "ready",
    account: { id: guest, identity: { subject: "erin" } },
  });
  const db = signingUp.getSnapshot().client!.db;
  const titles = async () => (await db.all(app.tasks)).map((task) => task.title).sort();
  return { backend, visitorUrl, signingUp, other, guest, db, titles };
}

/**
 * Write while the other tab still holds the store, and show the tab waits for
 * it: connecting (it once stopped for good), and its write doesn't reach the
 * server, though nothing else holds it back (the worker refused this tab).
 */
async function writeWhileRefused(
  db: Awaited<ReturnType<typeof signUpWhileAnotherTabIsTheGuest>>["db"],
): Promise<{ upload: Promise<unknown> }> {
  const written = db.insert(app.tasks, { title: "while-the-other-is-a-guest" });
  let uploaded = false;
  const upload = written.wait({ tier: "global" }).then(() => (uploaded = true));
  await new Promise((resolve) => setTimeout(resolve, 3_000));
  expect(db.getSyncStatus().link.state).toBe("connecting");
  expect(uploaded).toBe(false);
  // Wrapped: an async function returning the promise would wait for it.
  return { upload };
}

for (const clearOnSignOut of [false, true]) {
  it(`keeps both tabs syncing when a local-first guest signs up in one of them${
    clearOnSignOut ? " (clearOnSignOut)" : ""
  }`, async () => {
    const { other, guest, db, titles } = await signUpWhileAnotherTabIsTheGuest(
      "guest-sign-up-tabs",
      clearOnSignOut,
    );

    // What it reads and writes meanwhile goes on once it joins.
    const seen: string[][] = [];
    cleanups.push(async () => unsubscribe());
    const unsubscribe = db.subscribe(app.tasks, {
      onUpdate: (rows) => seen.push(rows.map((task) => task.title).sort()),
      onError: (error) => seen.push([`error: ${error.message}`]),
    });
    const { upload } = await writeWhileRefused(db);
    await other.holdBackend(false);
    await upload;
    expect(await titles()).toEqual(["guest-1", "while-the-other-is-a-guest"]);
    await waitForCondition(
      async () => seen.at(-1)?.length === 2,
      15_000,
      "The subscription made while waiting should see the tasks",
    );
    expect(seen.flat().filter((title) => title.startsWith("error"))).toEqual([]);
    await waitForCondition(
      async () => db.getSyncStatus().link.state === "connected",
      10_000,
      "The tab that signed up should connect",
    );
    // Its next write reaches the server, and the other tab.
    await db.insert(app.tasks, { title: "after-sign-up" }).wait({ tier: "global" });
    await waitForCondition(
      async () => (await other.state()).titles.length === 3,
      15_000,
      "The other tab should see the writes made after the sign-up",
    );

    // The other tab follows the sign-up to the same account, and syncs too.
    await waitForCondition(
      async () => (await other.state()).subject === "erin",
      15_000,
      "The other tab should follow the sign-up",
    );
    expect(await other.state()).toMatchObject({
      status: "ready",
      account: guest,
      titles: ["after-sign-up", "guest-1", "while-the-other-is-a-guest"],
    });
    await other.insert("from-the-other-tab");
    await waitForCondition(
      async () => (await titles()).includes("from-the-other-tab"),
      15_000,
      "The tab that signed up should see the other tab's write",
    );
  }, 120_000);
}

it("lets the tab that signed up in once the other guest tab crashes", async () => {
  const { other, db, titles } = await signUpWhileAnotherTabIsTheGuest(
    "guest-sign-up-tab-crash",
    false,
  );
  const { upload } = await writeWhileRefused(db);

  // The other tab never follows: its page goes away without closing its
  // session (a crash, a discarded tab), so it never tells the worker.
  await other.crash();
  await upload;
  expect(await titles()).toEqual(["guest-1", "while-the-other-is-a-guest"]);
  await waitForCondition(
    async () => db.getSyncStatus().link.state === "connected",
    10_000,
    "The tab that signed up should connect",
  );
}, 120_000);

// #4992, #5139: on a shared device (clearOnSignOut), the user who signed up
// in one tab signs out there before the other tab heard of the sign-up. The
// other tab still holds the guest, which is the user's account now: it must
// sign out too, wiping its replica, not stay open as them until it reloads.
// Both tabs are local-first, on one store: the sign-out here leaves it to
// the other tab, which still has it open, and the last tab out wipes it.
it("signs the other guest tab out and wipes the store when the user signs up and out in this one (clearOnSignOut)", async () => {
  const { backend, visitorUrl, signingUp, other, guest, db } =
    await signUpWhileAnotherTabIsTheGuest("guest-sign-up-sign-out-tabs", true);
  const path = await vfsPath(resolveDefaultPersistentDbName(db.config));
  await waitForCondition(
    async () => (await vfsPages(path, "guest-1")).holding > 0,
    15_000,
    "The guest's task should reach the shared replica in IndexedDB",
  );

  await signOutOfJazzApp(backend, visitorUrl);
  await signingUp.reauthenticate();
  expect(signingUp.getSnapshot()).toMatchObject({ status: "signed-out" });
  expect(signingUp.getSnapshot().error).toBeUndefined();
  // Not wiped from under the other tab, which is still the guest.
  expect(await other.state()).toMatchObject({ status: "ready", account: guest });
  expect((await vfsPages(path, "guest-1")).holding).toBeGreaterThan(0);

  // The other tab hears of it (the account store's storage event, the
  // sign-out's broadcast) once its backend requests go on, and wipes.
  await other.holdBackend(false);
  await waitForCondition(
    async () => (await other.state()).status === "signed-out",
    15_000,
    "The other tab should sign out of the account the guest became",
  );
  expect((await other.state()).account).toBeUndefined();
  await waitForCondition(
    async () => (await vfsPages(path, "guest-1")).pages === 0,
    15_000,
    "The last tab out should wipe the replica both tabs shared",
  );
  expect((await other.state()).history.errors).toEqual([]);
}, 120_000);
