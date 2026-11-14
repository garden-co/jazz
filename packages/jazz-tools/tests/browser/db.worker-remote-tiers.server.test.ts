import { afterEach, expect, it } from "vitest";
import { generateAuthSecret, schema, ReadTier } from "../../src/index.js";
import { deploy } from "./deploy.js";
import { TestCleanup, createBrowserTestDb, uniqueDbName, waitForCondition } from "./support.js";
import { openBrowserSessionCore as createJazzClient } from "../../src/session/create-jazz-session.js";
import { createDb as createLocalFirstDb } from "../../src/runtime/default-create-db.js";
import { isThinDb } from "../../src/runtime/local-first-upgrade.js";
import {
  createJazzServerTransportControl,
  getJazzServerInfo,
  jazzAppFetch,
  jazzAppUserUrl,
  startJazzApp,
} from "./testing-server.js";

// A tab over its shared worker reads with the same tiers as a direct client
// (INV-API-32): a remote read waits for the server, a Global wait for the
// authority, and a first-load wait reaches the worker that holds the store.
// The same rules are pinned in Rust (tests/binding_host.rs), for NAPI, and for
// React Native foregrounds over the native relay.

const app = schema.defineApp(
  { entries: schema.table({ title: schema.string() }, {}) },
  { migrations: "none" },
);
const permissions = schema.definePermissions(app, ({ policy }) => [
  policy.entries.allowRead.always(),
  policy.entries.allowInsert.always(),
]);

const ctx = new TestCleanup();
afterEach(async () => {
  await ctx.cleanup();
});

it("keeps a tab's remote reads and Global waits pending while its worker cannot reach the server", async () => {
  const db = ctx.track(
    await createBrowserTestDb({
      appId: uniqueDbName("worker-remote-tiers-offline"),
      secret: generateAuthSecret(),
      driver: { type: "persistent", dbName: uniqueDbName("worker-remote-tiers-offline-db") },
    }),
  );
  const write = db.insert(app.entries, { title: "local" });
  const row = await write.wait({ tier: "local" });
  const settled: string[] = [];
  void write.wait({ tier: "global" }).then(
    () => settled.push("global"),
    () => settled.push("global:closed"),
  );
  void db.all(app.entries, { tier: ReadTier.Remote }).then(
    () => settled.push("remote"),
    () => settled.push("remote:closed"),
  );
  const strict: unknown[][] = [];
  ctx.trackSubscription(
    db.subscribe(app.entries, (rows) => strict.push(rows), { tier: ReadTier.Remote }),
  );
  expect(await db.all(app.entries, { tier: ReadTier.LocalFirst })).toEqual([row]);
  await new Promise((resolve) => setTimeout(resolve, 500));
  expect(settled).toEqual([]);
  expect(strict).toEqual([]);
}, 30_000);

it("waits for a tab's firstLoadRemoteWaitMs in its worker, then shows the worker's store", async () => {
  const {
    appId,
    serverUrl: coreUrl,
    adminSecret,
  } = await getJazzServerInfo(uniqueDbName("worker-first-load-wait"));
  await deploy({ appId, serverUrl: coreUrl, adminSecret, schema: app.wasmSchema, permissions });
  const { serverUrl } = await startJazzApp(
    { appId, serverUrl: coreUrl, adminSecret },
    { schema: app.wasmSchema, permissions },
  );
  const transport = await createJazzServerTransportControl(serverUrl);
  try {
    const writer = ctx.track(
      await createBrowserTestDb({
        appId,
        serverUrl,
        secret: generateAuthSecret(),
        driver: { type: "memory" },
      }),
    );
    await writer.insert(app.entries, { title: "Only on the server" }).wait({ tier: "global" });

    const reader = ctx.track(
      await createBrowserTestDb({
        appId,
        serverUrl: transport.url,
        secret: generateAuthSecret(),
        driver: { type: "persistent", dbName: uniqueDbName("worker-first-load-wait-db") },
      }),
    );
    await reader.insert(app.entries, { title: "Written by the reader" }).wait({ tier: "global" });

    await transport.blockInbound();
    const waitMs = 1_500;
    const started = Date.now();
    const deliveries: string[][] = [];
    ctx.trackSubscription(
      reader.subscribe(
        app.entries,
        (rows) => deliveries.push(rows.map((row) => row.title).sort()),
        { tier: ReadTier.LocalFirst, firstLoadRemoteWaitMs: waitMs },
      ),
    );
    await waitForCondition(
      async () => deliveries.length > 0,
      10_000,
      "local-first opening at the deadline",
    );
    expect(Date.now() - started).toBeGreaterThanOrEqual(waitMs - 50);
    expect(deliveries[0]).toEqual(["Written by the reader"]);

    await transport.unblock();
    await waitForCondition(
      async () =>
        JSON.stringify(deliveries.at(-1)) ===
        JSON.stringify(["Only on the server", "Written by the reader"]),
      10_000,
      "the late server answer as a change",
    );
  } finally {
    await transport.stop();
  }
}, 60_000);

// A write made once the worker has published its outage (or had its sign-in
// refused, which something renews) is queued like any other: its Global wait
// holds until the worker reconnects, as without a worker (#3923). A tab must
// not ask its worker for a live server link before it waits.
it("holds a tab's Global wait for a write made after its worker published an outage", async () => {
  const {
    appId,
    serverUrl: coreUrl,
    adminSecret,
  } = await getJazzServerInfo(uniqueDbName("worker-global-wait-outage"));
  await deploy({ appId, serverUrl: coreUrl, adminSecret, schema: app.wasmSchema, permissions });
  const { serverUrl } = await startJazzApp(
    { appId, serverUrl: coreUrl, adminSecret },
    { schema: app.wasmSchema, permissions },
  );
  const transport = await createJazzServerTransportControl(serverUrl);
  try {
    const db = ctx.track(
      await createBrowserTestDb({
        appId,
        serverUrl: transport.url,
        secret: generateAuthSecret(),
        driver: { type: "persistent", dbName: uniqueDbName("worker-global-wait-outage-db") },
      }),
    );
    await db.insert(app.entries, { title: "before the outage" }).wait({ tier: "global" });

    // A blocked transport keeps its sockets, so the link never fails: take
    // the server down, so it retries until it publishes the outage (7.5 s).
    await transport.down();
    await waitForCondition(
      async () => {
        const link = db.getSyncStatus().link;
        return link.state === "offline" && link.reason === "network";
      },
      30_000,
      "the worker's published outage",
    );
    let outcome = "pending";
    void db
      .insert(app.entries, { title: "during the outage" })
      .wait({ tier: "global" })
      .then(
        () => (outcome = "global"),
        (error: unknown) => (outcome = `rejected: ${String(error)}`),
      );
    await new Promise((resolve) => setTimeout(resolve, 1_000));
    expect(outcome).toBe("pending");

    await transport.up();
    await waitForCondition(async () => outcome !== "pending", 30_000, "the reconnect");
    expect(outcome).toBe("global");
  } finally {
    await transport.stop();
  }
}, 90_000);

// A write made as the server goes away, before the worker publishes the
// outage, keeps its Global wait through the publication (#4848; rc.4 R22).
it("holds a tab's Global wait armed before its worker published an outage", async () => {
  const {
    appId,
    serverUrl: coreUrl,
    adminSecret,
  } = await getJazzServerInfo(uniqueDbName("worker-global-wait-early-outage"));
  await deploy({
    appId,
    serverUrl: coreUrl,
    adminSecret,
    schema: app.wasmSchema,
    permissions,
  });
  const { serverUrl } = await startJazzApp(
    { appId, serverUrl: coreUrl, adminSecret },
    { schema: app.wasmSchema, permissions },
  );
  const transport = await createJazzServerTransportControl(serverUrl);
  try {
    const db = ctx.track(
      await createBrowserTestDb({
        appId,
        serverUrl: transport.url,
        secret: generateAuthSecret(),
        driver: {
          type: "persistent",
          dbName: uniqueDbName("worker-global-wait-early-outage-db"),
        },
      }),
    );
    await db.insert(app.entries, { title: "before the outage" }).wait({ tier: "global" });

    await transport.down();
    await new Promise((resolve) => setTimeout(resolve, 1_000));
    let outcome = "pending";
    void db
      .insert(app.entries, { title: "as the server goes away" })
      .wait({ tier: "global" })
      .then(
        () => (outcome = "global"),
        (error: unknown) => (outcome = `rejected: ${String(error)}`),
      );
    await waitForCondition(
      async () => {
        const link = db.getSyncStatus().link;
        return link.state === "offline" && link.reason === "network";
      },
      30_000,
      "the worker's published outage",
    );
    await new Promise((resolve) => setTimeout(resolve, 2_000));
    expect(outcome).toBe("pending");

    await transport.up();
    await waitForCondition(async () => outcome !== "pending", 30_000, "the reconnect");
    expect(outcome).toBe("global");
  } finally {
    await transport.stop();
  }
}, 90_000);

// The same through an app's session: a user signed in through the app's
// backend, whose session upgraded to local-first and then reopened (a reload,
// so it opens local-first), writes while the server is gone but not yet
// published as an outage (rc.4 R22).
it("holds a session's Global wait through its worker's outage after a reload", async () => {
  const {
    appId,
    serverUrl: coreUrl,
    adminSecret,
  } = await getJazzServerInfo(uniqueDbName("worker-global-wait-session"));
  await deploy({
    appId,
    serverUrl: coreUrl,
    adminSecret,
    schema: app.wasmSchema,
    permissions,
  });
  const backend = await startJazzApp(
    { appId, serverUrl: coreUrl, adminSecret },
    { schema: app.wasmSchema, permissions },
  );
  const userUrl = await jazzAppUserUrl(backend, "alice");
  const transport = await createJazzServerTransportControl(userUrl);
  const dbName = uniqueDbName("worker-global-wait-session-db");
  const stored: { value: string | null } = { value: null };
  const store = {
    async read() {
      return stored.value;
    },
    async update(transform: (value: string | null) => string | null) {
      stored.value = transform(stored.value);
    },
  };
  // The backend answers no CORS: send the session's own requests to it from Node.
  const pageFetch = globalThis.fetch;
  globalThis.fetch = ((input: RequestInfo | URL, init?: RequestInit) => {
    const url = input instanceof Request ? input.url : String(input);
    return url.startsWith(transport.url) || url.startsWith(transport.url.replace("ws", "http"))
      ? jazzAppFetch(input, init)
      : pageFetch(input, init);
  }) as typeof fetch;
  const openSession = async () => {
    const session = await createJazzClient({
      scope: appId,
      serverUrl: transport.url,
      store,
      fetch: jazzAppFetch,
      driver: { type: "persistent", dbName },
      client: async () => ({ createClient: createLocalFirstDb }),
    });
    if (session.getSnapshot().status !== "ready") await session.reauthenticate();
    return session;
  };
  const sessionDb = (session: Awaited<ReturnType<typeof openSession>>) => {
    const client = session.getSnapshot().client;
    if (!client) throw new Error(`session is ${session.getSnapshot().status}`);
    return client.db;
  };
  try {
    const first = await openSession();
    await sessionDb(first).insert(app.entries, { title: "thin" }).wait({ tier: "global" });
    await sessionDb(first).upgradeToLocalFirst(async () => ({
      createClient: createLocalFirstDb,
    }));
    expect(isThinDb(sessionDb(first))).toBe(false);
    await first.close();

    const session = await openSession();
    try {
      const db = sessionDb(session);
      expect(isThinDb(db)).toBe(false);
      await db.all(app.entries, { tier: "remote" });

      await transport.down();
      await new Promise((resolve) => setTimeout(resolve, 1_000));
      const outcomes: Record<string, string> = {
        write: "pending",
        sync: "pending",
      };
      const write = db.insert(app.entries, { title: "during the outage" });
      void write.wait({ tier: "global" }).then(
        () => (outcomes.write = "global"),
        (error: unknown) => (outcomes.write = `rejected: ${String(error)}`),
      );
      void db.waitForSync().then(
        () => (outcomes.sync = "synced"),
        (error: unknown) => (outcomes.sync = `rejected: ${String(error)}`),
      );
      await waitForCondition(
        async () => {
          const link = db.getSyncStatus().link;
          return link.state === "offline" && link.reason === "network";
        },
        30_000,
        "the worker's published outage",
      );
      await new Promise((resolve) => setTimeout(resolve, 3_000));
      expect(outcomes).toEqual({ write: "pending", sync: "pending" });

      await transport.up();
      await waitForCondition(async () => outcomes.write !== "pending", 30_000, "the reconnect");
      expect(outcomes.write).toBe("global");
      await waitForCondition(async () => outcomes.sync !== "pending", 30_000, "the sync");
      expect(outcomes.sync).toBe("synced");
    } finally {
      await session.close();
    }
  } finally {
    globalThis.fetch = pageFetch;
    await transport.stop();
  }
}, 120_000);
