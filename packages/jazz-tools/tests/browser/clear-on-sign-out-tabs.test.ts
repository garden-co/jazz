import { afterEach, expect, it, vi } from "vitest";
import { deploy } from "../../src/dev/catalogue.js";
import { openBrowserSessionCore as createJazzClient } from "../../src/session/create-jazz-session.js";
import { createDb as createLocalFirstDb } from "../../src/runtime/default-create-db.js";
import { resolveDefaultPersistentDbName } from "../../src/runtime/db.js";
import { isThinDb } from "../../src/runtime/local-first-upgrade.js";
import { uniqueDbName, waitForCondition } from "./support.js";
import { vfsPages, vfsPath } from "./idb-vfs.js";
import { openSessionTab } from "./session-tab.js";
import {
  sessionTabApp as app,
  sessionTabPermissions as permissions,
} from "./session-tab-schema.js";
import {
  getJazzServerInfo,
  jazzAppFetch,
  jazzAppUserUrl,
  jazzAppVisitor,
  signOutOfJazzApp,
  signUpAtJazzApp,
  startJazzApp,
} from "./testing-server.js";

// A shared device (`clearOnSignOut`) with two tabs of one account open. Their
// local-first replica is one store, so a sign-out's wipe must never take it
// from under a tab whose user is still signed in (adopter evals E13), and a
// tab that follows a sign-out elsewhere must not fail wiping what another tab
// wipes (E14). The last tab to close the account wipes it.

const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup().catch(() => {});
  vi.restoreAllMocks();
});

/** The app's backend, and `url`, the origin a browser of this test uses. */
async function appAt(name: string, user?: string) {
  const { appId, serverUrl: coreUrl, adminSecret } = await getJazzServerInfo(uniqueDbName(name));
  await deploy({ appId, serverUrl: coreUrl, adminSecret, schema: app.wasmSchema, permissions });
  const backend = await startJazzApp(
    { appId, serverUrl: coreUrl, adminSecret },
    { schema: app.wasmSchema, permissions },
  );
  const url = user ? await jazzAppUserUrl(backend, user) : await jazzAppVisitor(backend);
  // The backend answers no CORS: send the session's own requests to it from Node.
  const origins = new Set([new URL(url).origin]);
  const pageFetch = globalThis.fetch;
  globalThis.fetch = ((input: RequestInfo | URL, init?: RequestInit) => {
    const target = input instanceof Request ? input.url : String(input);
    return origins.has(new URL(target).origin) ? jazzAppFetch(input, init) : pageFetch(input, init);
  }) as typeof fetch;
  cleanups.push(async () => {
    globalThis.fetch = pageFetch;
  });
  /** Another browser of `userId`: a thin session reading what the server has. */
  const serverReader = async (userId: string) => {
    const at = await jazzAppUserUrl(backend, userId);
    origins.add(new URL(at).origin);
    const reader = await createJazzClient({ serverUrl: at, fetch: jazzAppFetch });
    cleanups.push(() => reader.close());
    if (reader.getSnapshot().status !== "ready") await reader.reauthenticate();
    return async () => (await reader.getSnapshot().client!.db.all(app.tasks)).length;
  };
  return { backend, url, serverReader };
}

/** This page's session at `url`, local-first once loaded. */
async function openHere(url: string, options: { initial?: "device" } = {}) {
  const session = await createJazzClient({
    serverUrl: url,
    fetch: jazzAppFetch,
    clearOnSignOut: true,
    ...options,
    client: async () => ({ createClient: createLocalFirstDb }),
  });
  cleanups.push(() => session.close());
  const statuses = [session.getSnapshot().status as string];
  session.subscribe(() => statuses.push(session.getSnapshot().status));
  if (!options.initial && session.getSnapshot().status !== "ready") await session.reauthenticate();
  await waitForCondition(
    async () => {
      const client = session.getSnapshot().client;
      return !!client && !isThinDb(client.db);
    },
    30_000,
    "This tab should upgrade to local-first",
  );
  return { session, statuses };
}

/** Both tabs signed in as alice, each with a write only this browser has. */
async function aliceInTwoTabs(name: string) {
  const { backend, url, serverReader } = await appAt(name, "alice");
  const { session: here, statuses } = await openHere(url);
  // Its id is the test's own: other files run beside this one, with tabs of their own.
  const other = await openSessionTab(name, url, { clearOnSignOut: true, signedIn: true });
  cleanups.push(() => other.close());
  expect(await other.state()).toMatchObject({ status: "ready", subject: "alice" });
  const db = here.getSnapshot().client!.db;
  await db.insert(app.tasks, { title: "synced" }).wait({ tier: "global" });
  await waitForCondition(
    async () => (await other.state()).titles.includes("synced"),
    15_000,
    "The other tab should see the synced task",
  );
  const path = await vfsPath(resolveDefaultPersistentDbName(db.config));
  await waitForCondition(
    async () => (await vfsPages(path, "synced")).holding > 0,
    15_000,
    "The synced task should reach the shared replica in IndexedDB",
  );
  // Offline: the browser's one worker stops syncing, for both tabs.
  await db.disconnect();
  await db.insert(app.tasks, { title: "here-offline" }).wait({ tier: "local" });
  await other.insertLocally("there-offline");
  await waitForCondition(
    async () => (await other.state()).titles.length === 3,
    15_000,
    "The other tab should read both offline writes from the shared store",
  );
  return { backend, url, serverReader, here, statuses, other, path };
}

it("leaves the other tab, still signed in, its data and its link when this tab signs out offline (E13)", async () => {
  const { backend, url, serverReader, here, other, path } = await aliceInTwoTabs(
    "clear-on-sign-out-tabs-e13",
  );

  // The app's own sign-out can't reach its backend offline, so it signs
  // out of Jazz only: the other tab's user is still signed in.
  await here.logout();
  expect(here.getSnapshot()).toMatchObject({ status: "signed-out" });
  await new Promise((resolve) => setTimeout(resolve, 2_000));
  expect(await other.state()).toMatchObject({
    status: "ready",
    subject: "alice",
    titles: ["here-offline", "synced", "there-offline"],
  });
  expect((await vfsPages(path, "synced")).holding).toBeGreaterThan(0);

  // Back online, the other tab uploads both offline writes and goes on.
  await other.reconnect();
  await other.insert("after-reconnect");
  expect((await other.state()).titles).toEqual([
    "after-reconnect",
    "here-offline",
    "synced",
    "there-offline",
  ]);

  // Nothing was lost: the server has all four.
  const onServer = await serverReader("alice");
  await waitForCondition(
    async () => (await onServer()) === 4,
    30_000,
    "The server should have all four tasks",
  );

  // The user signs out for real now (the app's own sign-out reaches its
  // backend, so neither tab signs back in): the last tab out wipes.
  await signOutOfJazzApp(backend, url);
  await other.signOut();
  await waitForCondition(
    async () =>
      (await other.state()).status === "signed-out" && here.getSnapshot().status === "signed-out",
    45_000,
    "Both tabs should be signed out",
  );
  await waitForCondition(
    async () => (await vfsPages(path, "synced")).pages === 0,
    15_000,
    "The last tab to sign out should wipe the replica",
  );
  expect((await other.state()).history.errors).toEqual([]);
}, 180_000);

it("wipes once, counting every tab's unsynced writes, when the backend signs the user out of both tabs (E13)", async () => {
  const { backend, url, here, statuses, other, path } = await aliceInTwoTabs(
    "clear-on-sign-out-tabs-detected",
  );
  const warnings: string[] = [];
  const warn = console.warn.bind(console);
  vi.spyOn(console, "warn").mockImplementation((...args: unknown[]) => {
    warnings.push(args.map(String).join(" "));
    warn(...args);
  });

  // Signed out on the server: each tab hears of it and signs out.
  await signOutOfJazzApp(backend, url);
  await here.reauthenticate();
  expect(here.getSnapshot().status).toBe("signed-out");
  await waitForCondition(
    async () => (await other.state()).status === "signed-out",
    45_000,
    "The other tab should follow the sign-out",
  );
  await waitForCondition(
    async () => (await vfsPages(path, "synced")).pages === 0,
    15_000,
    "The last tab to sign out should wipe the replica",
  );
  // One wipe, which names both tabs' writes the server never had.
  const wipes = [...warnings, ...(await other.state()).history.warnings].filter((line) =>
    line.includes("wiped"),
  );
  expect(wipes).toHaveLength(1);
  expect(wipes[0]).toContain("wiped 2 write(s)");
  expect(statuses).not.toContain("error");
  expect((await other.state()).history.statuses).not.toContain("error");
  expect((await other.state()).history.errors).toEqual([]);
}, 180_000);

it("lets a background tab follow a sign-out and the next sign-in without wiping under another tab or failing (E14)", async () => {
  const { backend, url } = await appAt("clear-on-sign-out-tabs-e14");
  const { session: here, statuses } = await openHere(url, { initial: "device" });
  const other = await openSessionTab("clear-on-sign-out-tabs-e14", url, { clearOnSignOut: true });
  cleanups.push(() => other.close());
  // The guest signs up here; the other tab follows.
  await signUpAtJazzApp(backend, url, "erin");
  await here.linkWithBackend();
  await here.reauthenticate();
  await here
    .getSnapshot()
    .client!.db.insert(app.tasks, { title: "erin-1" })
    .wait({ tier: "global" });
  await waitForCondition(
    async () => (await other.state()).subject === "erin",
    15_000,
    "The other tab should follow the sign-up",
  );

  // Erin signs out here, and frank signs up and in, while the other tab
  // follows in the background.
  await signOutOfJazzApp(backend, url);
  await here.reauthenticate();
  expect(here.getSnapshot().status).toBe("signed-out");
  await signUpAtJazzApp(backend, url, "frank");
  await here.reauthenticate();
  expect(here.getSnapshot()).toMatchObject({
    status: "ready",
    account: { identity: { subject: "frank" } },
  });
  await waitForCondition(
    async () => (await other.state()).subject === "frank",
    45_000,
    "The other tab should follow to the next user",
  );
  const state = await other.state();
  expect(state).toMatchObject({ status: "ready", subject: "frank" });
  expect(state.history.statuses).not.toContain("error");
  expect(state.history.errors).toEqual([]);
  expect(statuses).not.toContain("error");
  // Frank's tabs keep working.
  await other.insert("frank-1");
  await waitForCondition(
    async () =>
      (await here.getSnapshot().client!.db.all(app.tasks)).some((task) => task.title === "frank-1"),
    15_000,
    "This tab should see the other tab's write as frank",
  );
}, 180_000);
