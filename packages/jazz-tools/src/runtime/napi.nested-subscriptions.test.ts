import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { createJazzSession } from "../backend/create-jazz-session.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";

// Alice writes through one native replica; Bob subscribes through another.
// Descendant changes must reach the SDK even when SQLite replaces the parent
// result instead of sending Groove's per-child terminal edit operations.
it.each([true, false])(
  "delivers nested native subscription edits with memory server=%s",
  async (inMemory) => {
    const app = s.defineApp({
      parents: s.table(
        { name: s.string(), metadata: s.json() },
        { childrenViaParent: s.reverse("children", "parent") },
      ),
      children: s.table(
        { parentId: s.uuid(), name: s.string() },
        {
          parent: s.rel("parents", "parentId"),
          entriesViaChild: s.reverse("entries", "child"),
        },
      ),
      entries: s.table(
        { childId: s.uuid(), name: s.string() },
        { child: s.rel("children", "childId") },
      ),
    });
    const server = await startLocalJazzServer({ inMemory });
    const owners: Awaited<ReturnType<typeof createJazzSession>>[] = [];
    let stop: (() => void) | undefined;
    try {
      await deploy({
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
        schema: app,
        permissions: {},
      });
      const open = async () => {
        const owner = await createJazzSession({
          appId: server.appId,
          serverUrl: server.url,
          app,
          permissions: {},
          driver: { type: "memory" },
          initial: { backendSecret: server.backendSecret },
        });
        owners.push(owner);
        return owner.getSnapshot().client!.db;
      };
      const alice = await open();
      const parent = await alice
        .insert(app.parents, { name: "Parent", metadata: { version: 1 } })
        .wait({ tier: "global" });
      const bravo = await alice
        .insert(app.children, { parentId: parent.id, name: "Bravo" })
        .wait({ tier: "global" });
      const charlie = await alice
        .insert(app.children, { parentId: parent.id, name: "Charlie" })
        .wait({ tier: "global" });
      const bob = await open();
      const query = app.parents.where({ id: parent.id }).include({
        childrenViaParent: app.children.orderBy("name").include({ entriesViaChild: true }),
      });
      let latest: unknown;
      let failure: unknown;
      stop = bob.subscribe(
        query,
        {
          onUpdate: (rows) => {
            latest = rows;
          },
          onError: (error) => {
            failure = error;
          },
        },
        { tier: "remote" },
      );
      const expectChildren = async (children: unknown[]) => {
        await expect
          .poll(() => failure ?? latest, { timeout: 10_000 })
          .toEqual([{ ...parent, childrenViaParent: children }]);
        expect(failure).toBeUndefined();
      };
      const bravoRow = { ...bravo, entriesViaChild: [] };
      const charlieRow = { ...charlie, entriesViaChild: [] };
      await expectChildren([bravoRow, charlieRow]);
      const alpha = await alice
        .insert(app.children, { parentId: parent.id, name: "Alpha" })
        .wait({ tier: "global" });
      const alphaRow = { ...alpha, entriesViaChild: [] };
      await expectChildren([alphaRow, bravoRow, charlieRow]);
      await alice.update(app.children, bravo.id, { name: "Zulu" }).wait({ tier: "global" });
      const zuluRow = { ...bravoRow, name: "Zulu" };
      await expectChildren([alphaRow, charlieRow, zuluRow]);
      const entry = await alice
        .insert(app.entries, { childId: charlie.id, name: "Nested" })
        .wait({ tier: "global" });
      await expectChildren([alphaRow, { ...charlieRow, entriesViaChild: [entry] }, zuluRow]);
      await alice.update(app.entries, entry.id, { name: "Edited" }).wait({ tier: "global" });
      await expectChildren([
        alphaRow,
        { ...charlieRow, entriesViaChild: [{ ...entry, name: "Edited" }] },
        zuluRow,
      ]);
      await alice.delete(app.entries, entry.id).wait({ tier: "global" });
      await expectChildren([alphaRow, charlieRow, zuluRow]);
      await alice.delete(app.children, alpha.id).wait({ tier: "global" });
      await expectChildren([charlieRow, zuluRow]);
    } finally {
      stop?.();
      for (const owner of owners) await owner.close();
      await server.stop();
    }
  },
  30_000,
);
