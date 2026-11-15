import { randomUUID } from "node:crypto";
import { describe, expect, it } from "vitest";
import {
  deploy,
  startLocalJazzServer,
  startTestJazz,
  testApp,
  type TestJazz,
} from "../testing/index.js";

describe("JSON parent includes through the native backend", () => {
  for (const inMemory of [true, false]) {
    it(`hydrates projected and full includes with ${inMemory ? "memory" : "persistent"} server storage`, async () => {
      const app = testApp({
        tables: {
          parents: {
            columns: { name: "string", metadata: { json: true } },
            relations: { childrenViaParent: { reverse: "children", relation: "parent" } },
          },
          children: {
            columns: { parentId: "uuid", name: "string" },
            relations: { parent: { rel: "parents", column: "parentId" } },
          },
        },
      });
      const server = await startLocalJazzServer({ appId: randomUUID(), inMemory });
      let owner: TestJazz | undefined;
      try {
        await deploy({
          serverUrl: server.url,
          appId: server.appId,
          adminSecret: server.adminSecret,
          app,
          permissions: { format: 1, tables: {} },
        });
        owner = await startTestJazz({ app, permissions: { format: 1, tables: {} }, core: server });
        const db = owner.jazz.db;
        const parent = await db
          .insert(app.parents, { name: "Alice", metadata: {} })
          .wait({ tier: "global" });
        const child = await db
          .insert(app.children, { parentId: parent.id, name: "Note" })
          .wait({ tier: "global" });
        expect(await db.all(app.parents, { tier: "remote" })).toMatchObject([
          { id: parent.id, metadata: {} },
        ]);
        expect(await db.all(app.children, { tier: "remote" })).toMatchObject([{ id: child.id }]);
        expect(
          await db.all(
            app.parents.select("id").include({ childrenViaParent: app.children.select("id") }),
            { tier: "remote" },
          ),
        ).toEqual([{ id: parent.id, childrenViaParent: [{ id: child.id }] }]);
        expect(
          await db.all(app.parents.include({ childrenViaParent: true }), { tier: "remote" }),
        ).toMatchObject([{ id: parent.id, metadata: {}, childrenViaParent: [{ id: child.id }] }]);
        const metadata = { body: "x".repeat(100_000) };
        await db.update(app.parents, parent.id, { metadata }).wait({ tier: "global" });
        expect(
          await db.all(app.parents.include({ childrenViaParent: true }), { tier: "remote" }),
        ).toMatchObject([{ id: parent.id, metadata, childrenViaParent: [{ id: child.id }] }]);
      } finally {
        await owner?.stop();
        await server.stop();
      }
    }, 30_000);
  }
});
