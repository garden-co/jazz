import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { ReadTier } from "./client.js";
import { createDb } from "./default-create-db.js";
import { localAccountConfig } from "./testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";

const app = s.defineApp({ entries: s.table({ title: s.string() }, {}) });
const permissions = definePermissions(app, ({ policy }) => {
  policy.entries.allowRead.always();
  policy.entries.allowInsert.always();
});

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
      { tier: ReadTier.LocalFirstUnlessEmpty },
    );
    await expect.poll(() => deliveries.length, { timeout: 10_000 }).toBeGreaterThan(0);
    // No empty flash: the opening waited for the first remote view.
    expect(deliveries[0]).toEqual(["Already on the server"]);
    unsubscribe();

    const fresh = await createDb(await localAccountConfig(server.appId, server.url));
    dbs.push(fresh);
    expect(
      (await fresh.all(app.entries, { tier: ReadTier.LocalFirstUnlessEmpty })).map(
        (row) => row.title,
      ),
    ).toEqual(["Already on the server"]);
  } finally {
    for (const db of dbs) await db.shutdown();
    await server.stop();
  }
}, 60_000);

it("does not hang an empty client whose server is unreachable", async () => {
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
      { tier: ReadTier.LocalFirstUnlessEmpty },
    );
    await expect.poll(() => deliveries.length, { timeout: 10_000 }).toBeGreaterThan(0);
    expect(deliveries[0]).toEqual([]);
    expect(await db.all(app.entries, { tier: ReadTier.LocalFirstUnlessEmpty })).toEqual([]);
    // Bounded by the first-connection wait, never by the unreachable server.
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
