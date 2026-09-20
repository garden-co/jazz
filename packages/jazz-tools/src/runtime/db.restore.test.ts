import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "./default-create-db.js";
import type { Db } from "./db.js";
import { localAccountConfig } from "./testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";

it.each(["standalone", "transaction"])(
  "can delete a restored plaintext row (%s)",
  async (mode) => {
    const app = s.defineApp({ notes: s.table({ title: s.string() }, {}) });
    const permissions = definePermissions(app, ({ policy }) => {
      policy.notes.allowRead.always();
      policy.notes.allowInsert.always();
      policy.notes.allowUpdate.always();
      policy.notes.allowDelete.always();
    });
    const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    let db: Db | undefined;
    try {
      await deploy({
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
        schema: app,
        permissions,
      });
      db = await createDb(await localAccountConfig(server.appId, server.url));
      const note = await db.insert(app.notes, { title: "Original" }).wait({ tier: "global" });
      await db.delete(app.notes, note.id).wait({ tier: "global" });
      if (mode === "transaction") {
        const tx = db.beginTransaction();
        await tx.all(app.notes.includeDeleted().where({ id: note.id }), { tier: "global" });
        tx.restore(app.notes, note.id, { title: "Restored" });
        await tx.commit().wait({ tier: "global" });
      } else {
        await db.restore(app.notes, note.id, { title: "Restored" }).wait({ tier: "global" });
      }
      expect(await db.one(app.notes.where({ id: note.id }), { tier: "global" })).toEqual({
        id: note.id,
        title: "Restored",
      });
      await db.delete(app.notes, note.id).wait({ tier: "global" });
      expect(await db.one(app.notes.where({ id: note.id }), { tier: "global" })).toBeNull();
    } finally {
      await db?.shutdown();
      await server.stop();
    }
  },
  60_000,
);
