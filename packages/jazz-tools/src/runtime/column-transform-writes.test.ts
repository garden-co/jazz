import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { testApp } from "../testing/test-app.js";
import type { Db } from "./db.js";
import { createDb } from "./default-create-db.js";
import { localAccountConfig } from "./testing/account-fixtures.js";

// Writes through a transformed column, other than a plain insert or update:
// upsert (both branches), restore, and transactions. The app sees titles in
// upper case; the store keeps them in lower case. `from` records what it was
// given, so a test can check the stored form as well as the app's.
const behaviour = { stored: [] as string[], failTo: undefined as undefined | (() => never) };

const app = testApp(
  {
    tables: { notes: { columns: { title: { type: "string", transform: "lowercase" } } } },
    transforms: { lowercase: { stored: "string", view: "string" } },
  },
  {
    transforms: {
      lowercase: {
        from: (value: string) => {
          behaviour.stored.push(value);
          return value.toUpperCase();
        },
        to: (value: string) => {
          behaviour.failTo?.();
          return value.toLowerCase();
        },
      },
    },
  },
);

const ID = "00000000-0000-4000-8000-000000005668";

/** What `write` throws, so a falsy throw is told apart from none. */
function thrownBy(write: () => unknown): unknown {
  try {
    write();
  } catch (error) {
    return error;
  }
  return "nothing thrown";
}

describe("writes through a transformed column", () => {
  let db: Db;

  beforeEach(async () => {
    behaviour.stored = [];
    behaviour.failTo = undefined;
    db = await createDb({
      ...(await localAccountConfig(`transform-writes-${crypto.randomUUID()}`)),
      driver: { type: "memory" },
    });
  });

  afterEach(async () => {
    behaviour.failTo = undefined;
    await db.shutdown();
  });

  /** The row as the app reads it, and the title as it is stored. */
  async function read(id: string): Promise<{ row: unknown; stored: string | undefined }> {
    behaviour.stored = [];
    const row = await db.one(app.notes.where({ id }));
    return { row, stored: behaviour.stored.at(-1) };
  }

  it("upserts through the transform when it creates the row and when it updates it", async () => {
    const created = db.upsert(app.notes, ID, { title: "Hello" });
    await created.wait({ tier: "local" });
    await expect(read(ID)).resolves.toEqual({
      row: { id: ID, title: "HELLO" },
      stored: "hello",
    });

    await db.upsert(app.notes, ID, { title: "World" }).wait({ tier: "local" });
    await expect(read(ID)).resolves.toEqual({
      row: { id: ID, title: "WORLD" },
      stored: "world",
    });
    await expect(db.all(app.notes)).resolves.toHaveLength(1);
  });

  it("restores a deleted row through the transform", async () => {
    const { value: note } = db.insert(app.notes, { title: "Before" });
    await db.delete(app.notes, note.id).wait({ tier: "local" });
    await expect(db.all(app.notes)).resolves.toEqual([]);

    const restored = db.restore(app.notes, note.id, { title: "Back" });
    expect(restored.value).toEqual({ id: note.id, title: "BACK" });
    await restored.wait({ tier: "local" });
    await expect(read(note.id)).resolves.toEqual({
      row: { id: note.id, title: "BACK" },
      stored: "back",
    });
  });

  it("commits transformed writes from a transaction, and a rollback writes none", async () => {
    const { value: kept } = db.insert(app.notes, { title: "Kept" });

    const tx = db.beginTransaction();
    const inserted = tx.insert(app.notes, { title: "In Tx" });
    expect(inserted).toEqual({ id: expect.any(String), title: "IN TX" });
    tx.update(app.notes, kept.id, { title: "Changed" });
    tx.upsert(app.notes, ID, { title: "Upserted" });
    await tx.commit().wait({ tier: "local" });

    await expect(read(inserted.id)).resolves.toEqual({
      row: { id: inserted.id, title: "IN TX" },
      stored: "in tx",
    });
    await expect(read(kept.id)).resolves.toEqual({
      row: { id: kept.id, title: "CHANGED" },
      stored: "changed",
    });
    await expect(read(ID)).resolves.toEqual({
      row: { id: ID, title: "UPSERTED" },
      stored: "upserted",
    });

    const rolledBack = db.beginTransaction();
    rolledBack.insert(app.notes, { title: "Never" });
    rolledBack.update(app.notes, kept.id, { title: "Never" });
    await rolledBack.rollback();
    await expect(db.all(app.notes)).resolves.toHaveLength(3);
    await expect(read(kept.id)).resolves.toEqual({
      row: { id: kept.id, title: "CHANGED" },
      stored: "changed",
    });
  });

  it.each([
    ["an error", new Error("to failed")],
    ["a falsy value", undefined],
  ])("fails a transaction whose `to` throws %s, and writes none of it", async (_, thrown) => {
    const { value: note } = db.insert(app.notes, { title: "Hello" });

    const tx = db.beginTransaction();
    tx.insert(app.notes, { title: "Staged first" });
    behaviour.failTo = () => {
      throw thrown;
    };
    expect(thrownBy(() => tx.update(app.notes, note.id, { title: "Changed" }))).toBe(thrown);
    // The transaction stays failed: a later write in it, even one whose `to`
    // succeeds, throws the same, and so does its commit.
    behaviour.failTo = undefined;
    expect(thrownBy(() => tx.update(app.notes, note.id, { title: "After" }))).toBe(thrown);
    const committed = await tx
      .commit()
      .wait({ tier: "local" })
      .then(
        () => "committed",
        (error: unknown) => error,
      );
    expect(committed).not.toBe("committed");
    // A thrown Error is passed on as it is; a falsy throw still fails it.
    if (thrown) expect(committed).toBe(thrown);
    await tx.rollback();

    // Nothing of the failed transaction is written, and the client goes on.
    await expect(read(note.id)).resolves.toEqual({
      row: { id: note.id, title: "HELLO" },
      stored: "hello",
    });
    await expect(db.all(app.notes)).resolves.toHaveLength(1);
    await db.update(app.notes, note.id, { title: "Later" }).wait({ tier: "local" });
    await expect(read(note.id)).resolves.toEqual({
      row: { id: note.id, title: "LATER" },
      stored: "later",
    });
  });
});
