import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { ReadTier } from "./client.js";
import { createDb } from "./default-create-db.js";
import { localAccountConfig } from "./testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { startHoldingProxy } from "./testing/holding-proxy.js";

const app = s.defineApp({ entries: s.table({ title: s.string() }, {}) });
const permissions = definePermissions(app, ({ policy }) => {
  policy.entries.allowRead.always();
  policy.entries.allowInsert.always();
});

it("opens a client with local rows on the server's answer when it arrives in time", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const dbs: Awaited<ReturnType<typeof createDb>>[] = [];
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    const writer = await createDb(await localAccountConfig(server.appId, server.url));
    dbs.push(writer);
    const written = writer.insert(app.entries, { title: "Already on the server" });
    await written.wait({ tier: "global" });

    const reader = await createDb(await localAccountConfig(server.appId, server.url));
    dbs.push(reader);
    reader.insert(app.entries, { title: "Written locally" });
    const deliveries: string[][] = [];
    const unsubscribe = reader.subscribe(
      app.entries,
      (rows) => deliveries.push(rows.map((row) => row.title).sort()),
      { tier: ReadTier.LocalFirst, firstLoadRemoteWaitMs: 30_000 },
    );
    await expect.poll(() => deliveries.length, { timeout: 10_000 }).toBeGreaterThan(0);
    // The local row alone is not shown first: the opening waited for the server.
    expect(deliveries[0]).toEqual(["Already on the server", "Written locally"]);
    unsubscribe();

    const fresh = await createDb(await localAccountConfig(server.appId, server.url));
    dbs.push(fresh);
    expect(
      (
        await fresh.all(app.entries, { tier: ReadTier.LocalFirst, firstLoadRemoteWaitMs: 30_000 })
      ).map((row) => row.title),
    ).toContain("Already on the server");
  } finally {
    for (const db of dbs) await db.shutdown();
    await server.stop();
  }
}, 60_000);

it("does not wait for a server that is unreachable", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  let db: Awaited<ReturnType<typeof createDb>> | undefined;
  let serverStopped = false;
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    const account = await localAccountConfig(server.appId, server.url);
    await server.stop();
    serverStopped = true;

    db = await createDb(account);
    const started = Date.now();
    const deliveries: string[][] = [];
    const unsubscribe = db.subscribe(
      app.entries,
      (rows) => deliveries.push(rows.map((row) => row.title)),
      { tier: ReadTier.LocalFirst, firstLoadRemoteWaitMs: 60_000 },
    );
    await expect.poll(() => deliveries.length, { timeout: 10_000 }).toBeGreaterThan(0);
    expect(deliveries[0]).toEqual([]);
    expect(
      await db.all(app.entries, { tier: ReadTier.LocalFirst, firstLoadRemoteWaitMs: 60_000 }),
    ).toEqual([]);
    // Bounded by the first-connection wait, never by the timeout.
    expect(Date.now() - started).toBeLessThan(8_000);

    // Local writes still appear immediately.
    db.insert(app.entries, { title: "Written offline" });
    await expect.poll(() => deliveries.at(-1)).toEqual(["Written offline"]);
    unsubscribe();
  } finally {
    await db?.shutdown();
    if (!serverStopped) await server.stop();
  }
}, 60_000);

it("gives an empty client the server's rows as its first delivery", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const dbs: Awaited<ReturnType<typeof createDb>>[] = [];
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    const writer = await createDb(await localAccountConfig(server.appId, server.url));
    dbs.push(writer);
    const written = writer.insert(app.entries, { title: "Already on the server" });
    await written.wait({ tier: "global" });

    const reader = await createDb(await localAccountConfig(server.appId, server.url));
    dbs.push(reader);
    const deliveries: string[][] = [];
    const unsubscribe = reader.subscribe(
      app.entries,
      (rows) => deliveries.push(rows.map((row) => row.title)),
      { tier: ReadTier.LocalFirst, firstLoadRemoteWaitMs: 30_000 },
    );
    await expect.poll(() => deliveries.length, { timeout: 10_000 }).toBeGreaterThan(0);
    // No empty flash: the opening waited for the server's answer.
    expect(deliveries[0]).toEqual(["Already on the server"]);
    unsubscribe();

    const fresh = await createDb(await localAccountConfig(server.appId, server.url));
    dbs.push(fresh);
    expect(
      (
        await fresh.all(app.entries, { tier: ReadTier.LocalFirst, firstLoadRemoteWaitMs: 30_000 })
      ).map((row) => row.title),
    ).toEqual(["Already on the server"]);
  } finally {
    for (const db of dbs) await db.shutdown();
    await server.stop();
  }
}, 60_000);

it("shows local rows at the deadline when a live server has not answered", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const proxy = await startHoldingProxy(server.url);
  const dbs: Awaited<ReturnType<typeof createDb>>[] = [];
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    const writer = await createDb(await localAccountConfig(server.appId, server.url));
    dbs.push(writer);
    await writer.insert(app.entries, { title: "Only on the server" }).wait({ tier: "global" });

    const reader = await createDb(await localAccountConfig(server.appId, proxy.url));
    dbs.push(reader);
    // The reader's own write reaching the server proves its link is live.
    await reader.insert(app.entries, { title: "Written by the reader" }).wait({ tier: "global" });

    proxy.hold();
    const waitMs = 1_500;
    const started = Date.now();
    const deliveries: string[][] = [];
    const unsubscribe = reader.subscribe(
      app.entries,
      (rows) => deliveries.push(rows.map((row) => row.title).sort()),
      { tier: ReadTier.LocalFirst, firstLoadRemoteWaitMs: waitMs },
    );
    await expect.poll(() => deliveries.length, { timeout: 10_000 }).toBeGreaterThan(0);
    // The opening waited for the whole timeout, then showed the local rows.
    expect(Date.now() - started).toBeGreaterThanOrEqual(waitMs - 50);
    expect(deliveries[0]).toEqual(["Written by the reader"]);

    const oneShotStarted = Date.now();
    expect(
      (
        await reader.all(app.entries, { tier: ReadTier.LocalFirst, firstLoadRemoteWaitMs: waitMs })
      ).map((row) => row.title),
    ).toEqual(["Written by the reader"]);
    expect(Date.now() - oneShotStarted).toBeGreaterThanOrEqual(waitMs - 50);

    // The late answer arrives as an ordinary change.
    proxy.release();
    await expect
      .poll(() => deliveries.at(-1), { timeout: 10_000 })
      .toEqual(["Only on the server", "Written by the reader"]);
    unsubscribe();
  } finally {
    proxy.release();
    for (const db of dbs) await db.shutdown();
    await proxy.stop();
    await server.stop();
  }
}, 60_000);
