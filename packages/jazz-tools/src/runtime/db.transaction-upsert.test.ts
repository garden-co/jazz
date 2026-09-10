import { afterEach, describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import { createDb } from "./default-create-db.js";
import type { Db } from "./db.js";
import { localAccountConfig } from "./testing/account-fixtures.js";

const app = s.defineApp({
  items: s.table({
    title: s.string(),
    note: s.string().default("default note"),
    tags: s.array(s.string()).default([]),
  }),
});

const requiredArrayApp = s.defineApp({
  items: s.table({
    title: s.string(),
    tags: s.array(s.string()),
  }),
});

let db: Db | undefined;
afterEach(async () => {
  await db?.shutdown();
  db = undefined;
});

describe.each(["mergeable", "exclusive"] as const)("%s transaction upserts", (kind) => {
  it("applies insert defaults and preserves omitted fields across staged patches", async () => {
    db = await createDb(await localAccountConfig("transaction-upsert-defaults"));
    await db.all(app.items);
    const tx = kind === "mergeable" ? db.beginTransaction() : db.beginExclusiveTransaction();
    const id = "00000000-0000-4000-8000-000000000101";
    try {
      tx.upsert(app.items, id, { title: "created" });
      await expect(tx.one(app.items.where({ id }))).resolves.toEqual({
        id,
        title: "created",
        note: "default note",
        tags: [],
      });
      tx.update(app.items, id, { tags: ["kept"] });
      tx.upsert(app.items, id, { title: "updated" });
      await expect(tx.one(app.items.where({ id }))).resolves.toEqual({
        id,
        title: "updated",
        note: "default note",
        tags: ["kept"],
      });
      await tx.commit().wait({ tier: "local" });
    } catch (error) {
      await tx.rollback().catch(() => {});
      throw error;
    }
    await expect(db.one(app.items.where({ id }))).resolves.toEqual({
      id,
      title: "updated",
      note: "default note",
      tags: ["kept"],
    });
  });

  it("rejects an incomplete insert through transaction completion", async () => {
    db = await createDb(await localAccountConfig("transaction-upsert-required"));
    await db.all(app.items);
    const tx = kind === "mergeable" ? db.beginTransaction() : db.beginExclusiveTransaction();
    const id = "00000000-0000-4000-8000-000000000102";
    // A partial upsert is valid for an existing row, but cannot create one
    // without its required title. Rust must decide against transaction state.
    tx.upsert(app.items, id, { note: "missing title" });
    await expect(tx.commit().wait({ tier: "local" })).rejects.toThrow("missing required field");
    await expect(db.all(app.items)).resolves.toEqual([]);
  });
});

it("requires arrays without defaults on insert instead of synthesizing an empty array", async () => {
  db = await createDb(await localAccountConfig("insert-required-array"));
  expect(() =>
    // @ts-expect-error - deliberately omit a required field to test runtime validation
    db!.insert(requiredArrayApp.items, { title: "missing tags" }),
  ).toThrow("missing required column tags");
  await db
    .insert(requiredArrayApp.items, { title: "explicit empty", tags: [] })
    .wait({ tier: "local" });
  await expect(db.all(requiredArrayApp.items)).resolves.toMatchObject([
    { title: "explicit empty", tags: [] },
  ]);
});

describe.each(["direct", "mergeable", "exclusive"] as const)(
  "%s required-array upserts",
  (kind) => {
    it("rejects omission on creation but preserves the array when patching an existing row", async () => {
      db = await createDb(await localAccountConfig("upsert-required-array"));
      await db.all(requiredArrayApp.items);
      const id = "00000000-0000-4000-8000-000000000103";
      if (kind === "direct") {
        await expect(
          db.upsert(requiredArrayApp.items, id, { title: "missing tags" }).wait({ tier: "local" }),
        ).rejects.toThrow("missing required field");
      } else {
        const tx = kind === "mergeable" ? db.beginTransaction() : db.beginExclusiveTransaction();
        tx.upsert(requiredArrayApp.items, id, { title: "missing tags" });
        await expect(tx.commit().wait({ tier: "local" })).rejects.toThrow("missing required field");
      }
      await expect(db.all(requiredArrayApp.items)).resolves.toEqual([]);

      const inserted = db.insert(requiredArrayApp.items, { title: "original", tags: ["keep"] });
      await inserted.wait({ tier: "local" });
      if (kind === "direct") {
        await db
          .upsert(requiredArrayApp.items, inserted.value.id, { title: "patched" })
          .wait({ tier: "local" });
      } else {
        const tx = kind === "mergeable" ? db.beginTransaction() : db.beginExclusiveTransaction();
        tx.upsert(requiredArrayApp.items, inserted.value.id, { title: "patched" });
        await tx.commit().wait({ tier: "local" });
      }
      await expect(
        db.one(requiredArrayApp.items.where({ id: inserted.value.id })),
      ).resolves.toEqual({
        id: inserted.value.id,
        title: "patched",
        tags: ["keep"],
      });
    });
  },
);
