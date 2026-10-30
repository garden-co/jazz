import { afterEach, expect, it } from "vitest";
import { generateAuthSecret, schema, ReadTier } from "../../src/index.js";
import { deploy } from "../../src/dev/catalogue.js";
import { TestCleanup, createBrowserTestDb, uniqueDbName, waitForCondition } from "./support.js";
import { getJazzServerInfo, createJazzServerTransportControl } from "./testing-server.js";

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
  const { appId, serverUrl, adminSecret } = await getJazzServerInfo(
    uniqueDbName("worker-first-load-wait"),
  );
  await deploy({ appId, serverUrl, adminSecret, schema: app.wasmSchema, permissions });
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
