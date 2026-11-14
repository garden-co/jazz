import { afterEach, expect, it, vi } from "vitest";
import { deploy } from "../../src/dev/catalogue.js";
import { openBrowserSessionCore as createJazzClient } from "../../src/session/create-jazz-session.js";
import { createDb as createLocalFirstDb } from "../../src/runtime/default-create-db.js";
import { resolveDefaultPersistentDbName } from "../../src/runtime/db.js";
import { isThinDb } from "../../src/runtime/local-first-upgrade.js";
import { deleteLocalFirstStore } from "../../src/runtime/delete-local-first-store.js";
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
  signOutOfJazzApp,
  startJazzApp,
} from "./testing-server.js";

// The #5250 lifecycle of a shared device's per-account store (`clearOnSignOut`),
// in a real browser with its real broker worker: a store a tab leaves behind
// is wiped only once its user is proven signed out, on the next trigger in an
// open page (nothing polls), and the worker deletes a store only while no tab
// has it attached.

const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup().catch(() => {});
  vi.restoreAllMocks();
});

async function appAt(name: string, user: string) {
  const { appId, serverUrl: coreUrl, adminSecret } = await getJazzServerInfo(uniqueDbName(name));
  await deploy({ appId, serverUrl: coreUrl, adminSecret, schema: app.wasmSchema, permissions });
  const backend = await startJazzApp(
    { appId, serverUrl: coreUrl, adminSecret },
    { schema: app.wasmSchema, permissions },
  );
  const url = await jazzAppUserUrl(backend, user);
  const origin = new URL(url).origin;
  const pageFetch = globalThis.fetch;
  globalThis.fetch = ((input: RequestInfo | URL, init?: RequestInit) => {
    const target = input instanceof Request ? input.url : String(input);
    return new URL(target).origin === origin ? jazzAppFetch(input, init) : pageFetch(input, init);
  }) as typeof fetch;
  cleanups.push(async () => {
    globalThis.fetch = pageFetch;
  });
  return { backend, url };
}

async function openHere(url: string) {
  const session = await createJazzClient({
    serverUrl: url,
    fetch: jazzAppFetch,
    clearOnSignOut: true,
    client: async () => ({ createClient: createLocalFirstDb }),
  });
  cleanups.push(() => session.close());
  if (session.getSnapshot().status !== "ready") await session.reauthenticate();
  await waitForCondition(
    async () => {
      const client = session.getSnapshot().client;
      return !!client && !isThinDb(client.db);
    },
    30_000,
    "This tab should upgrade to local-first",
  );
  return session;
}

/** What the page does when it becomes visible again. */
const becomeVisible = () => document.dispatchEvent(new Event("visibilitychange"));

it("keeps a store a crashed tab left while its user is signed in, and wipes it on the next trigger once they aren't", async () => {
  const name = "clear-on-sign-out-lifecycle-orphan";
  const { backend, url } = await appAt(name, "alice");
  const here = await openHere(url);
  const other = await openSessionTab(name, url, { clearOnSignOut: true, signedIn: true });
  cleanups.push(() => other.close());
  const db = here.getSnapshot().client!.db;
  await db.insert(app.tasks, { title: "synced" }).wait({ tier: "global" });
  const path = await vfsPath(resolveDefaultPersistentDbName(db.config));
  await waitForCondition(
    async () => (await vfsPages(path, "synced")).holding > 0,
    15_000,
    "The synced task should reach the shared store in IndexedDB",
  );

  // This tab signs out of Jazz only; the other, still alice, then crashes.
  await here.logout();
  expect(here.getSnapshot().status).toBe("signed-out");
  await other.insertLocally("there-unsynced");
  await other.crash();

  // Alice is still signed in to the app: her store, unsynced write included, stays.
  becomeVisible();
  await new Promise((resolve) => setTimeout(resolve, 5_000));
  expect((await vfsPages(path, "synced")).holding).toBeGreaterThan(0);

  // Signed out on the server: the next trigger wipes it, after two answers.
  await signOutOfJazzApp(backend, url);
  becomeVisible();
  await waitForCondition(
    async () => (await vfsPages(path, "synced")).pages === 0,
    20_000,
    "The page should wipe the store the crashed tab left, once alice is signed out",
  );
}, 180_000);

it("deletes a store through its worker only while no tab has it attached", async () => {
  const { backend, url } = await appAt("clear-on-sign-out-lifecycle-delete-store", "bob");
  const here = await openHere(url);
  const db = here.getSnapshot().client!.db;
  await db.insert(app.tasks, { title: "kept" }).wait({ tier: "global" });
  const config = db.config;
  const path = await vfsPath(resolveDefaultPersistentDbName(config));
  await waitForCondition(
    async () => (await vfsPages(path, "kept")).holding > 0,
    15_000,
    "The task should reach IndexedDB",
  );
  const target = {
    ...config,
    accountId: config.accountId!,
    accountRegistryAuthority: config.accountRegistryAuthority!,
  };
  // The session names the same store the worker opened.
  expect(resolveDefaultPersistentDbName(target)).toBe(resolveDefaultPersistentDbName(config));

  // Attached: the worker refuses, deleting nothing.
  expect(await deleteLocalFirstStore(target)).toBe("held");
  expect((await vfsPages(path, "kept")).holding).toBeGreaterThan(0);

  // Signed out (the last tab out): the session deletes it the same way.
  await signOutOfJazzApp(backend, url);
  await here.logout();
  await waitForCondition(
    async () => (await vfsPages(path, "kept")).pages === 0,
    15_000,
    "Signing out the last tab should delete the store",
  );
  // Nothing left to delete: a second delete is harmless.
  expect(await deleteLocalFirstStore(target)).toBe("deleted");
}, 120_000);
