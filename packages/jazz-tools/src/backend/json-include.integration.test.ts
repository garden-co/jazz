import { randomUUID } from "node:crypto";
import { describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { createJazzSession } from "./index.js";

describe("JSON parent includes through the native backend", () => {
  for (const inMemory of [true, false]) {
    it(`hydrates projected and full includes with ${inMemory ? "memory" : "persistent"} server storage`, async () => {
      const app = s.defineApp({
        parents: s.table({ name: s.string(), metadata: s.json() }),
        children: s.table({ parentId: s.ref("parents"), name: s.string() }),
      });
      const server = await startLocalJazzServer({ appId: randomUUID(), inMemory });
      let owner: Awaited<ReturnType<typeof createJazzSession>> | undefined;
      try {
        await deploy({
          serverUrl: server.url,
          appId: server.appId,
          adminSecret: server.adminSecret,
          schema: app,
          permissions: {},
        });
        owner = await createJazzSession({
          appId: server.appId,
          serverUrl: server.url,
          app,
          permissions: {},
          driver: { type: "memory" },
          initial: { backendSecret: server.backendSecret },
        });
        const db = owner.getSnapshot().client!.db;
        const parent = await db
          .insert(app.parents, { name: "Alice", metadata: {} })
          .wait({ tier: "global" });
        const child = await db
          .insert(app.children, { parentId: parent.id, name: "Note" })
          .wait({ tier: "global" });
        expect(await db.all(app.parents, { tier: "edge" })).toMatchObject([
          { id: parent.id, metadata: {} },
        ]);
        expect(await db.all(app.children, { tier: "edge" })).toMatchObject([{ id: child.id }]);
        expect(
          await db.all(
            app.parents.select("id").include({ childrenViaParent: app.children.select("id") }),
            { tier: "edge" },
          ),
        ).toEqual([{ id: parent.id, childrenViaParent: [{ id: child.id }] }]);
        expect(
          await db.all(app.parents.include({ childrenViaParent: true }), { tier: "edge" }),
        ).toMatchObject([{ id: parent.id, metadata: {}, childrenViaParent: [{ id: child.id }] }]);
        const metadata = { body: "x".repeat(100_000) };
        await db.update(app.parents, parent.id, { metadata }).wait({ tier: "global" });
        expect(
          await db.all(app.parents.include({ childrenViaParent: true }), { tier: "edge" }),
        ).toMatchObject([{ id: parent.id, metadata, childrenViaParent: [{ id: child.id }] }]);
      } finally {
        await owner?.close();
        await server.stop();
      }
    }, 30_000);
  }
});
