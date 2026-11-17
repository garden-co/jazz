import { afterEach, expect, it } from "vitest";
import { commands, server } from "vitest/browser";
import { openBrowserSessionCore as createJazzClient } from "../../src/session/create-jazz-session.js";
import { createDb as createLocalFirstDb } from "../../src/runtime/default-create-db.js";
import { isThinDb } from "../../src/runtime/local-first-upgrade.js";
import { uniqueDbName, waitForCondition } from "./support.js";
import { openSessionTab } from "./session-tab.js";
import {
  sessionTabApp as app,
  sessionTabPermissions as permissions,
} from "./session-tab-schema.js";
import {
  deploy,
  getJazzServerInfo,
  jazzAppFetch,
  jazzAppVisitor,
  startJazzApp,
} from "./testing-server.js";

// A local-first tab the user navigates away from stays in the back/forward
// cache, and "back" shows it again as it was (N3 of #4996). Chrome caches a
// page with its Web Locks still held, so to the worker the cached tab was
// still open: it lets go of its page locks (its liveness lock and its
// store's) as it leaves, and takes them again, with a new connection to the
// worker, when it comes back. Chromium only: Playwright's Firefox and WebKit
// have the cache off for good (and Firefox caches no page connected to a
// SharedWorker that holds a lock, as a local-first tab's worker always does).
// Chromium 145, this suite's, caches no page with a SharedWorker at all,
// where 159 does.

const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup().catch(() => {});
});

/** This page and another tab of its origin, both local-first guests of one backend. */
async function twoTabs(name: string) {
  const { appId, serverUrl: coreUrl, adminSecret } = await getJazzServerInfo(uniqueDbName(name));
  await deploy({
    appId,
    serverUrl: coreUrl,
    adminSecret,
    app,
    permissions: permissions,
  });
  const backend = await startJazzApp(
    { appId, serverUrl: coreUrl, adminSecret },
    { schema: app, permissions },
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

  const here = await createJazzClient({
    serverUrl: visitorUrl,
    fetch: jazzAppFetch,
    initial: "device",
    client: async () => ({ createClient: createLocalFirstDb }),
    localFirst: "now",
  });
  cleanups.push(() => here.close());
  const db = here.getSnapshot().client!.db;
  expect(isThinDb(db)).toBe(false);
  const titles = async () => (await db.all(app.tasks)).map((task) => task.title).sort();
  const other = await openSessionTab("cached", visitorUrl, { localFirst: "now" });
  cleanups.push(() => other.close());
  return { db, titles, other };
}

it.runIf(server.browser === "chromium")(
  "a tab in the back/forward cache holds no Jazz lock, stays cached while another tab writes, and syncs when it comes back",
  async () => {
    const { db, titles, other } = await twoTabs("back-forward-cache");
    await other.insert("before-leaving");
    await waitForCondition(
      async () => (await titles()).includes("before-leaving"),
      15_000,
      "This tab should see the other tab's write",
    );

    const { heldWhileAway } = await other.leave();
    expect(heldWhileAway).toEqual([]);
    // The worker would post this write to a tab it still counted, and Chrome
    // evicts a cached page its shared worker posts to.
    await db.insert(app.tasks, { title: "while-away" }).wait({ tier: "global" });

    const back = await other.goBack();
    if (!(await commands.pageCacheKeepsSharedWorkerPages())) {
      // This Chromium caches no page with a SharedWorker (as every local-first
      // tab has): "back" loads the page again, whatever the tab let go of.
      expect(back).toMatchObject({ restored: false });
      expect(back.notRestoredReasons).toContain("sharedworker");
      return;
    }
    expect(back).toEqual({ restored: true, notRestoredReasons: [] });
    await waitForCondition(
      async () => (await other.state()).titles.includes("while-away"),
      15_000,
      "The tab that came back should catch up on what was written while it was away",
    );
    await other.insert("after-coming-back");
    await waitForCondition(
      async () => (await titles()).includes("after-coming-back"),
      15_000,
      "This tab should see the write of the tab that came back",
    );
    const state = await other.state();
    expect(state.status).toBe("ready");
    expect(state.history.errors).toEqual([]);
  },
  120_000,
);

it.runIf(server.browser === "chromium")(
  "a tab whose store another tab deleted while it was in the back/forward cache reloads when it comes back",
  async () => {
    if (!(await commands.pageCacheKeepsSharedWorkerPages())) return;
    const { db, other } = await twoTabs("back-forward-cache-reset");
    await other.insert("before-the-reset");
    expect((await other.leave()).heldWhileAway).toEqual([]);
    // The worker tells the tabs attached to it; the cached tab isn't one.
    await db.deleteClientStorage();
    // Shown from the cache, it reloads itself at once: a "reload", where a
    // page the browser dropped from the cache would load as "back_forward".
    await other.goBack();
    await waitForCondition(
      async () => (await other.navigationType()) === "reload",
      15_000,
      "The tab that came back should reload rather than show the deleted store",
    );
  },
  120_000,
);
