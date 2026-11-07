import { afterEach, expect, it } from "vitest";
import { deploy } from "../../src/dev/catalogue.js";
import { createJazzClient } from "../../src/session/create-jazz-session.js";
import { createDb as createLocalFirstDb } from "../../src/runtime/default-create-db.js";
import { isThinDb } from "../../src/runtime/local-first-upgrade.js";
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

it("keeps both tabs syncing when a local-first guest signs up in one of them", async () => {
  const {
    appId,
    serverUrl: coreUrl,
    adminSecret,
  } = await getJazzServerInfo(uniqueDbName("guest-sign-up-tabs"));
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

  // This page is the tab that signs up; another tab of its origin is open
  // too. Both are the device's local-first guest, local-first once loaded.
  const signingUp = await createJazzClient({
    serverUrl: visitorUrl,
    fetch: jazzAppFetch,
    initial: "local-first",
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
  const other = await openSessionTab("other", visitorUrl);
  cleanups.push(() => other.close());
  const guest = signingUp.getSnapshot().account!.id;
  expect((await other.state()).account).toBe(guest);
  const titles = async () =>
    (await signingUp.getSnapshot().client!.db.all(app.tasks)).map((task) => task.title).sort();
  await signingUp
    .getSnapshot()
    .client!.db.insert(app.tasks, { title: "guest-1" })
    .wait({ tier: "global" });

  // The other tab doesn't hear of the sign-up yet: it stays the guest.
  await other.holdBackend(true);
  // The app's own sign-up, then as its sign-up handler goes on.
  await signUpAtJazzApp(backend, visitorUrl, "erin");
  await signingUp.linkWithBackend();
  await signingUp.reauthenticate();
  expect(signingUp.getSnapshot()).toMatchObject({
    status: "ready",
    account: { id: guest, identity: { subject: "erin" } },
  });

  // While the other tab is still the guest, the tab that signed up waits for
  // it to follow: it shows connecting (it once stopped for good), and what
  // it reads and writes meanwhile goes on once it joins.
  const db = signingUp.getSnapshot().client!.db;
  const seen: string[][] = [];
  cleanups.push(async () => unsubscribe());
  const unsubscribe = db.subscribe(app.tasks, {
    onUpdate: (rows) => seen.push(rows.map((task) => task.title).sort()),
    onError: (error) => seen.push([`error: ${error.message}`]),
  });
  const written = db.insert(app.tasks, { title: "while-the-other-is-a-guest" });
  await new Promise((resolve) => setTimeout(resolve, 3_000));
  expect(db.getSyncStatus().link.state).not.toBe("offline");
  await other.holdBackend(false);
  await written.wait({ tier: "global" });
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
