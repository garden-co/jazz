import { expect, it } from "vitest";
import { schema as s, generateAuthSecret } from "../../src/index.js";
import { createBrowserTestDb } from "./support.js";
import { deploy } from "../../src/dev/catalogue.js";
import { getJazzServerInfo, stopJazzServer } from "./testing-server.js";

const app = s.defineApp({
  parents: s.table({
    name: s.string(),
    state: s.enum("draft", "open", "closed"),
    summaryId: s.ref("summaries").optional(),
  }),
  summaries: s.table({ metadata: s.json() }),
  children: s.table({ parentId: s.ref("parents"), metadata: s.json() }),
});

it("reads JSON forward and reverse includes offline before and after persistent reopen", async () => {
  const server = await getJazzServerInfo(crypto.randomUUID());
  const permissions = s.definePermissions(app, ({ policy }) => [
    policy.parents.allowRead.always(),
    policy.parents.allowInsert.always(),
    policy.summaries.allowRead.always(),
    policy.summaries.allowInsert.always(),
    policy.children.allowRead.always(),
    policy.children.allowInsert.always(),
  ]);
  await deploy({ ...server, schema: app, permissions });
  const config = {
    appId: server.appId,
    serverUrl: server.serverUrl,
    secret: generateAuthSecret(),
    driver: { type: "persistent" as const, dbName: `json-includes-${crypto.randomUUID()}` },
  };
  let db = await createBrowserTestDb(config);
  let serverStopped = false;
  try {
    const summary = await db
      .insert(app.summaries, { metadata: { total: 1 } })
      .wait({ tier: "global" });
    const parent = await db
      .insert(app.parents, { name: "Alice", state: "draft", summaryId: summary.id })
      .wait({ tier: "global" });
    const child = await db
      .insert(app.children, { parentId: parent.id, metadata: { title: "Note" } })
      .wait({ tier: "global" });
    await db.disconnect();
    await stopJazzServer(server.serverUrl);
    serverStopped = true;
    for (let cycle = 0; cycle < 2; cycle++) {
      const q = app.parents.where({ id: parent.id });
      expect(await db.one(q, { tier: "local" })).toMatchObject({ id: parent.id, state: "draft" });
      expect(await db.one(q.include({ summary: true }), { tier: "local" })).toMatchObject({
        summary: { id: summary.id, metadata: { total: 1 } },
      });
      expect(await db.one(q.include({ childrenViaParent: true }), { tier: "local" })).toMatchObject(
        { childrenViaParent: [{ id: child.id, metadata: { title: "Note" } }] },
      );
      expect(
        await db.one(
          q.select("id").include({
            summary: app.summaries.select("id"),
            childrenViaParent: app.children.select("id"),
          }),
          { tier: "local" },
        ),
      ).toEqual({
        id: parent.id,
        summary: { id: summary.id },
        childrenViaParent: [{ id: child.id }],
      });
      if (cycle === 0) {
        await db.shutdown();
        db = await createBrowserTestDb(config);
      }
    }
  } finally {
    await db.shutdown();
    if (!serverStopped) await stopJazzServer(server.serverUrl);
  }
}, 60_000);
