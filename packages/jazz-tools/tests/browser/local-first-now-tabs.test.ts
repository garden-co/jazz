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
import { getJazzServerInfo, jazzAppFetch, jazzAppVisitor, startJazzApp } from "./testing-server.js";

// `localFirst: "now"` opens the local-first client on a first visit, while
// another tab of the origin already holds the device account's store through
// the shared worker: both tabs read and write the one store at once.

const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup().catch(() => {});
});

it('opens local-first at once with localFirst: "now" while another tab holds the store', async () => {
  const {
    appId,
    serverUrl: coreUrl,
    adminSecret,
  } = await getJazzServerInfo(uniqueDbName("local-first-now-tabs"));
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

  // The other tab is first: it holds the device account's store.
  const other = await openSessionTab("other", visitorUrl, { localFirst: "now" });
  cleanups.push(() => other.close());

  const session = await createJazzClient({
    serverUrl: visitorUrl,
    fetch: jazzAppFetch,
    initial: "device",
    client: async () => ({ createClient: createLocalFirstDb }),
    localFirst: "now",
  });
  cleanups.push(() => session.close());
  // Local-first as soon as it resolved: no thin first paint to upgrade from.
  expect(isThinDb(session.db)).toBe(false);
  expect((await other.state()).account).toBe(session.getSnapshot().user!.accountId);

  await session.db.insert(app.tasks, { title: "from-this-tab" }).wait({ tier: "local" });
  await waitForCondition(
    async () => (await other.state()).titles.includes("from-this-tab"),
    15_000,
    "the other tab should read this tab's write from the shared store",
  );
  await other.insert("from-the-other-tab");
  await waitForCondition(
    async () =>
      (await session.db.all(app.tasks)).some((task) => task.title === "from-the-other-tab"),
    15_000,
    "this tab should read the other tab's write",
  );
}, 90_000);
