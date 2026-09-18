import { expect, it } from "vitest";
import { schema as s } from "../../src/schema-namespace.js";
import { definePermissions } from "../../src/permissions/index.js";
import { createDb } from "../../src/runtime/default-create-db.js";
import { deploy } from "../../src/dev/catalogue.js";
import { acquireBrowserTestAccount } from "./account-fixtures.js";
import { getJazzServerInfo, stopJazzServer } from "./testing-server.js";

it("observes both accepted rows after persistent reopening without encryption", async () => {
  const server = await getJazzServerInfo(`accepted-observation-${crypto.randomUUID()}`);
  const app = s.defineApp({
    __e2ee_snapshot_roots: s.table({ title: s.string() }, {}),
    __e2ee_snapshot_refs: s.table(
      { rootId: s.uuid() },
      { root: s.rel("__e2ee_snapshot_roots", "rootId") },
    ),
  });
  const permissions = definePermissions(app, ({ policy }) => {
    policy.__e2ee_snapshot_roots.allowRead.always();
    policy.__e2ee_snapshot_roots.allowInsert.always();
    policy.__e2ee_snapshot_refs.allowRead.always();
    policy.__e2ee_snapshot_refs.allowInsert.always();
  });
  let db: Awaited<ReturnType<typeof createDb>> | undefined;
  let stopped = false;
  try {
    await deploy({ ...server, schema: app, permissions });
    const config = {
      appId: server.appId,
      serverUrl: server.serverUrl,
      account: await acquireBrowserTestAccount(server),
      driver: {
        type: "persistent" as const,
        dbName: `accepted-observation-${crypto.randomUUID()}`,
      },
    };
    db = await createDb(config);
    await db.all(app.__e2ee_snapshot_roots.where({}), { tier: "local" });
    const tx = db.beginExclusiveTransaction();
    const root = tx.insert(app.__e2ee_snapshot_roots, { title: "Root" });
    const ref = tx.insert(app.__e2ee_snapshot_refs, { rootId: root.id });
    await tx.commit().wait({ tier: "global" });
    await db.shutdown();
    db = undefined;
    await stopJazzServer(server.serverUrl);
    stopped = true;
    db = await createDb(config);
    await db.disconnect();
    const roots = app.__e2ee_snapshot_roots.where({ id: root.id });
    const refs = app.__e2ee_snapshot_refs.where({ rootId: root.id });
    const observed = await db.observeE2eeHistory([roots, refs]);
    expect(observed.map(({ rows }) => rows)).toEqual([[root], [ref]]);
    expect(await db.all(refs, { tier: "local" })).toEqual([ref]);
  } finally {
    await db?.shutdown();
    if (!stopped) await stopJazzServer(server.serverUrl);
  }
}, 60_000);
