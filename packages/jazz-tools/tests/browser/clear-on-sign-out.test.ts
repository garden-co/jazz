import { afterEach, expect, it } from "vitest";
import { schema } from "../../src/index.js";
import { deploy } from "../../src/dev/catalogue.js";
import { openBrowserSessionCore as createJazzClient } from "../../src/session/create-jazz-session.js";
import { createDb as createLocalFirstDb } from "../../src/runtime/default-create-db.js";
import { resolveDefaultPersistentDbName } from "../../src/runtime/db.js";
import { isThinDb } from "../../src/runtime/local-first-upgrade.js";
import { uniqueDbName, waitForCondition } from "./support.js";
import { vfsPages, vfsPath } from "./idb-vfs.js";
import {
  getJazzServerInfo,
  jazzAppFetch,
  jazzAppUserUrl,
  signOutOfJazzApp,
  startJazzApp,
} from "./testing-server.js";

// A shared computer (`clearOnSignOut`): a user the app's backend signs out
// while no page of theirs is open (a server-side logout, or the tab closed)
// must leave nothing of theirs in IndexedDB once the next page loads (rc.4
// R49). The replica is the local-first SQLite file in the IndexedDB VFS.

const app = schema.defineApp(
  { notes: schema.table({ text: schema.string() }, {}) },
  { migrations: "none" },
);
const permissions = schema.definePermissions(app, ({ policy }) => [
  policy.notes.allowRead.always(),
  policy.notes.allowInsert.always(),
]);

const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup().catch(() => {});
});

it("wipes a user signed out while no page was open from IndexedDB on the next load", async () => {
  const {
    appId,
    serverUrl: coreUrl,
    adminSecret,
  } = await getJazzServerInfo(uniqueDbName("clear-on-sign-out"));
  await deploy({ appId, serverUrl: coreUrl, adminSecret, schema: app.wasmSchema, permissions });
  const backend = await startJazzApp(
    { appId, serverUrl: coreUrl, adminSecret },
    { schema: app.wasmSchema, permissions },
  );
  const aliceUrl = await jazzAppUserUrl(backend, "alice");
  // The backend answers no CORS: send the session's own requests to it from Node.
  const pageFetch = globalThis.fetch;
  globalThis.fetch = ((input: RequestInfo | URL, init?: RequestInit) => {
    const url = input instanceof Request ? input.url : String(input);
    return url.startsWith(aliceUrl) ? jazzAppFetch(input, init) : pageFetch(input, init);
  }) as typeof fetch;
  cleanups.push(async () => {
    globalThis.fetch = pageFetch;
  });
  // As an app's page does: the default browser account store, the default
  // persistent store, thin first and local-first once it loads.
  const load = () =>
    createJazzClient({
      serverUrl: aliceUrl,
      fetch: jazzAppFetch,
      clearOnSignOut: true,
      client: async () => ({ createClient: createLocalFirstDb }),
    });

  const first = await load();
  cleanups.push(() => first.close());
  if (first.getSnapshot().status !== "ready") await first.reauthenticate();
  await waitForCondition(
    async () => {
      const client = first.getSnapshot().client;
      return !!client && !isThinDb(client.db);
    },
    30_000,
    "Alice's page should upgrade to local-first",
  );
  const db = first.getSnapshot().client!.db;
  const secret = `ALICE-PRIVATE-${crypto.randomUUID()}`;
  await db.insert(app.notes, { text: secret }).wait({ tier: "global" });
  const path = await vfsPath(resolveDefaultPersistentDbName(db.config));
  await waitForCondition(
    async () => (await vfsPages(path, secret)).holding > 0,
    15_000,
    "Alice's note should reach her replica in IndexedDB",
  );

  // The page goes away without signing out (a reload, a closed tab), and the
  // app's backend signs her out (a server-side logout).
  await first.close();
  await signOutOfJazzApp(backend, aliceUrl);

  const next = await load();
  cleanups.push(() => next.close());
  expect(next.getSnapshot().status).toBe("signed-out");
  await waitForCondition(
    async () => (await vfsPages(path, secret)).pages === 0,
    30_000,
    "The next load should wipe the signed-out user's replica from IndexedDB",
  );
  // And it stays wiped: nothing writes her file again.
  await new Promise((resolve) => setTimeout(resolve, 3_000));
  expect(await vfsPages(path, secret)).toEqual({ pages: 0, holding: 0 });
}, 90_000);
