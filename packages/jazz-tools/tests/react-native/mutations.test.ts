import { describe, expect, it } from "vitest";
import { schema } from "../../src/index.js";
import { withNativeRelayFixture } from "./fixture.js";

const app = schema.defineApp({
  documents: schema.table({ title: schema.string(), done: schema.boolean() }),
});

describe("React Native public mutations through the real foreground C ABI", () => {
  it("keeps omitted cells, rejects tombstone upserts, and restores replacement content", async () => {
    await withNativeRelayFixture(app, async (fixture) => {
      const db = await fixture.createDb();
      const row = await db
        .insert(app.documents, { title: "original", done: false })
        .wait({ tier: "local" });
      await db.update(app.documents, row.id, { title: "patched" }).wait({ tier: "local" });
      expect(await db.one(app.documents.where({ id: row.id }))).toEqual({
        ...row,
        title: "patched",
      });
      await db.upsert(app.documents, row.id, { done: true }).wait({ tier: "local" });
      expect(await db.one(app.documents.where({ id: row.id }))).toEqual({
        ...row,
        title: "patched",
        done: true,
      });
      await db.delete(app.documents, row.id).wait({ tier: "local" });
      expect(await db.all(app.documents)).toEqual([]);
      await expect(
        db
          .upsert(app.documents, row.id, { title: "must stay hidden", done: false })
          .wait({ tier: "local" }),
      ).rejects.toMatchObject({
        name: "PersistedWriteRejectedError",
        code: "write_rejected",
      });
      expect(await db.all(app.documents)).toEqual([]);
      await db
        .restore(app.documents, row.id, { title: "restored", done: false })
        .wait({ tier: "local" });
      expect(await db.all(app.documents)).toEqual([{ ...row, title: "restored" }]);
    });
  });

  it("preserves caller timestamps across insert, update, upsert, and restore", async () => {
    await withNativeRelayFixture(app, async (fixture) => {
      const db = await fixture.createDb();
      const start = 1_704_067_200_123;
      const row = await db
        .insert(app.documents, { title: "insert", done: false }, { updatedAt: start })
        .wait({ tier: "local" });
      const read = () => db.one(app.documents.select("title", "$updatedAt").where({ id: row.id }));
      expect(await read()).toMatchObject({ title: "insert", $updatedAt: new Date(start) });
      await db
        .update(app.documents, row.id, { title: "update" }, { updatedAt: start + 1 })
        .wait({ tier: "local" });
      expect(await read()).toMatchObject({ title: "update", $updatedAt: new Date(start + 1) });
      await db
        .upsert(app.documents, row.id, { title: "upsert" }, { updatedAt: start + 2 })
        .wait({ tier: "local" });
      expect(await read()).toMatchObject({ title: "upsert", $updatedAt: new Date(start + 2) });
      await db.delete(app.documents, row.id).wait({ tier: "local" });
      await db
        .restore(app.documents, row.id, { title: "restore", done: true }, { updatedAt: start + 3 })
        .wait({ tier: "local" });
      expect(await read()).toMatchObject({ title: "restore", $updatedAt: new Date(start + 3) });
    });
  });
});
