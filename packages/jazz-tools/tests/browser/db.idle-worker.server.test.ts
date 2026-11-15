import { afterEach, expect, it } from "vitest";
import { generateAuthSecret } from "../../src/index.js";
import { app } from "./apps/local-first-server-wait/jazz.generated.js";
import permissions from "./apps/local-first-server-wait/jazz.permissions.json";
import { TestCleanup, createBrowserTestDb, uniqueDbName, waitForCondition } from "./support.js";
import { deploy, getJazzServerInfo, startJazzApp } from "./testing-server.js";
import { sharedWorkerTimerBrowserCommands } from "./browser-commands.js";

// A tab connected through its shared worker to a server, with nothing to
// sync, must let the worker go idle. The worker's server pump and its tab
// transport pump used to re-arm each other after every core tick, so an idle
// worker armed the 16ms server-pump timer ~60 times a second forever.

// The app (apps/local-first-server-wait): entries every user may read and insert.

const ctx = new TestCleanup();
afterEach(async () => {
  await ctx.cleanup();
});

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

it("lets the shared worker go idle once a connected tab has nothing to sync", async () => {
  const {
    appId,
    serverUrl: coreUrl,
    adminSecret,
  } = await getJazzServerInfo(uniqueDbName("idle-worker"));
  await deploy({ appId, serverUrl: coreUrl, adminSecret, app, permissions });
  const { serverUrl } = await startJazzApp(
    { appId, serverUrl: coreUrl, adminSecret },
    { schema: app, permissions },
  );
  const dbName = uniqueDbName("idle-worker-db");
  const db = ctx.track(
    await createBrowserTestDb({
      appId,
      serverUrl,
      secret: generateAuthSecret(),
      driver: { type: "persistent", dbName },
    }),
  );
  const seen: number[] = [];
  ctx.trackSubscription(db.subscribe(app.entries, (rows) => seen.push(rows.length)));
  await db.insert(app.entries, { title: "first" }).wait({ tier: "global" });
  await waitForCondition(async () => seen.at(-1) === 1, 10_000, "tab sees the first row");

  const probe = sharedWorkerTimerBrowserCommands();
  const expectIdle = async (when: string) => {
    await probe.sharedWorkerTimerCounts(dbName);
    await sleep(1_000); // let the last write's follow-up traffic settle
    const before = await probe.sharedWorkerTimerCounts(dbName);
    await sleep(3_000);
    const after = await probe.sharedWorkerTimerCounts(dbName);
    const idle = {
      serverPumpTimeouts: after.serverPumpTimeouts - before.serverPumpTimeouts,
      timeouts: after.timeouts - before.timeouts,
      microtasks: after.microtasks - before.microtasks,
      posts: after.posts - before.posts,
    };
    // Before the fix: ~180 server-pump timers and ~530 microtasks in 3 s.
    expect(idle.serverPumpTimeouts, `${when}: ${JSON.stringify(idle)}`).toBeLessThanOrEqual(3);
  };

  await expectIdle("after the first write settled");

  // Real work still reaches the server and the tab, and the worker goes idle again.
  await db.insert(app.entries, { title: "second" }).wait({ tier: "global" });
  await waitForCondition(async () => seen.at(-1) === 2, 10_000, "tab sees the second row");
  await expectIdle("after the second write settled");
});
