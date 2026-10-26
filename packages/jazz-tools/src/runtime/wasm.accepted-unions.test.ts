import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { createDb } from "./default-create-db.js";
import { localAccountConfig } from "./testing/account-fixtures.js";
import type { Db } from "./db.js";

// Alice's ordinary authenticated WASM client receives a cold UNION page from
// the real authority. Its retained local observations preserve the duplicate
// content receipts after disconnect and exclude later pending local writes.
it("preserves accepted UNION occurrences in ordinary WASM reads and offline observations", async () => {
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const app = s.defineApp({
    __e2ee_roots: s.table({ title: s.string() }, {}),
    __e2ee_links: s.table({ rootId: s.uuid() }, { root: s.rel("__e2ee_roots", "rootId") }),
  });
  const permissions = s.definePermissions(app, ({ policy }) => {
    policy.__e2ee_roots.allowRead.always();
    policy.__e2ee_roots.allowInsert.always();
    policy.__e2ee_roots.allowUpdate.always();
    policy.__e2ee_links.allowRead.always();
    policy.__e2ee_links.allowInsert.always();
  });
  const databases: Db[] = [];
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    const config = await localAccountConfig(server.appId, server.url);
    const writer = await createDb(config);
    databases.push(writer);
    const predecessor = writer.insert(app.__e2ee_roots, { title: "aardvark" });
    await predecessor.wait({ tier: "global" });
    const alpha = writer.insert(app.__e2ee_roots, { title: "alpha" });
    const row = await alpha.wait({ tier: "global" });
    await writer.insert(app.__e2ee_roots, { title: "gamma" }).wait({ tier: "global" });
    await writer.insert(app.__e2ee_links, { rootId: row.id }).wait({ tier: "global" });
    const reader = await createDb(config);
    databases.push(reader);
    // Bind the schema through an empty point read; none of the page's rows or
    // join witnesses are warmed before its first accepted read.
    expect(
      await reader.all(app.__e2ee_roots.where({ id: "00000000-0000-0000-0000-000000000001" }), {
        tier: "local-first",
      }),
    ).toEqual([]);
    const query = app
      .union({
        all: app.__e2ee_roots.select("title"),
        linked: app.__e2ee_links.hopTo("root").select("title"),
      })
      .orderBy("title");
    const page = await (
      await reader.serializableTransaction((tx) => tx.allSettledForE2ee(query.offset(1).limit(2)))
    ).wait({ tier: "global" });
    expect(page.rows).toEqual([row, row]);
    expect(page.settlements).toHaveLength(2);
    expect(page.settlements[0]).toEqual(page.settlements[1]);
    expect(page.settlements[0]).toEqual({
      rowId: row.id,
      transactionId: await alpha.txId,
      position: expect.stringMatching(/^\d+$/),
    });
    const complete = await (
      await reader.serializableTransaction((tx) => tx.allSettledForE2ee(query))
    ).wait({ tier: "global" });
    expect(complete.rows.map((row) => row.title)).toEqual(["aardvark", "alpha", "alpha", "gamma"]);
    await reader.disconnect();
    expect(await reader.observeE2eeHistory([query])).toEqual([complete]);
    await reader.insert(app.__e2ee_roots, { title: "pending" }).wait({ tier: "local" });
    await reader
      .update(app.__e2ee_roots, row.id, { title: "pending alpha" })
      .wait({ tier: "local" });
    expect(await reader.observeE2eeHistory([query])).toEqual([complete]);
    expect(await reader.all(app.__e2ee_roots, { tier: "local-first" })).toEqual(
      expect.arrayContaining([
        { id: row.id, title: "pending alpha" },
        { id: expect.any(String), title: "pending" },
      ]),
    );
  } finally {
    for (const db of databases.reverse()) await db.shutdown();
    await server.stop();
  }
}, 30_000);
