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

it("streams inserts, updates, and upserts through the core upload sink", async () => {
  await withNativeRelayFixture(app, async (fixture) => {
    const db = await fixture.createDb();
    const inserted = await db.insertStreaming(app.documents, {
      title: (async function* () {
        yield "native ";
        yield "\ud83e";
        yield "\udea9";
      })(),
      done: false,
    });
    await inserted.wait({ tier: "local" });
    expect(await db.one(app.documents.where({ id: inserted.value.id }))).toMatchObject({
      title: "native 🪩",
      done: false,
    });
    const updated = await db.updateStreaming(app.documents, inserted.value.id, {
      title: (async function* () {
        yield "updated ";
        yield new TextEncoder().encode("stream");
      })(),
      done: true,
    });
    await updated.wait({ tier: "local" });
    const upserted = await db.upsertStreaming(app.documents, inserted.value.id, {
      title: (async function* () {
        yield "x".repeat(150_000);
      })(),
    });
    await upserted.wait({ tier: "local" });
    expect(await db.one(app.documents.where({ id: inserted.value.id }))).toEqual({
      id: inserted.value.id,
      title: "x".repeat(150_000),
      done: true,
    });
  });
});

it("aborts a failed stream without publishing partial rows and allows the next upload", async () => {
  await withNativeRelayFixture(app, async (fixture) => {
    const db = await fixture.createDb();
    await expect(
      db.insertStreaming(app.documents, {
        title: (async function* () {
          yield "x".repeat(70_000);
          throw new Error("source failed");
        })(),
        done: false,
      }),
    ).rejects.toThrow("source failed");
    expect(await db.all(app.documents)).toEqual([]);
    const next = await db.insertStreaming(app.documents, {
      title: (async function* () {
        yield "after abort";
      })(),
      done: false,
    });
    await next.wait({ tier: "local" });
    expect(await db.all(app.documents)).toEqual([
      { id: next.value.id, title: "after abort", done: false },
    ]);
  });
});

it("applies typed text, bytes, and JSON diffs atomically with replacement cells", async () => {
  const rich = schema.defineApp({
    documents: schema.table({
      body: schema.string(),
      payload: schema.bytes(),
      metadata: schema.json(),
      done: schema.boolean(),
    }),
  });
  await withNativeRelayFixture(rich, async (fixture) => {
    const db = await fixture.createDb();
    const prefix = "a".repeat(70_000);
    const payload = new Uint8Array(70_006).fill(7);
    payload.set([0, 1, 2, 3, 4, 5], 70_000);
    const row = await db
      .insert(rich.documents, {
        body: `${prefix}A😀BC`,
        payload,
        metadata: { padding: prefix, nested: { answer: 42 } },
        done: false,
      })
      .wait({ tier: "local" });
    await db
      .update(
        rich.documents,
        row.id,
        { done: true },
        {
          applyDiffs: {
            body: {
              within: { from: 70_001, to: 70_003 },
              splices: [{ at: 0, delete: 2, insert: "🪩" }],
            },
            payload: {
              within: { from: 70_001, to: 70_005 },
              splices: [{ at: 1, delete: 2, insert: new Uint8Array([9, 8]) }],
            },
            metadata: { edits: [{ op: "set", at: "/nested/answer", value: 43 }] },
          },
        },
      )
      .wait({ tier: "local" });
    const expectedPayload = payload.slice();
    expectedPayload.set([9, 8], 70_002);
    expect(await db.one(rich.documents.where({ id: row.id }))).toEqual({
      ...row,
      body: `${prefix}A🪩BC`,
      payload: expectedPayload,
      metadata: { padding: prefix, nested: { answer: 43 } },
      done: true,
    });
    await expect(async () =>
      db
        .update(
          rich.documents,
          row.id,
          { done: false },
          {
            applyDiffs: {
              body: {
                within: { from: 70_001, to: 70_003 },
                splices: [{ at: 99, delete: 1, insert: "invalid" }],
              },
            },
          },
        )
        .wait({ tier: "local" }),
    ).rejects.toThrow();
    expect(await db.one(rich.documents.where({ id: row.id }))).toMatchObject({
      done: true,
      body: `${prefix}A🪩BC`,
    });
  });
});

it("settles an empty standalone update without changing the row", async () => {
  await withNativeRelayFixture(app, async (fixture) => {
    const db = await fixture.createDb();
    const row = await db
      .insert(app.documents, { title: "unchanged", done: false })
      .wait({ tier: "local" });
    const write = db.update(app.documents, row.id, {});
    await expect(write.wait({ tier: "local" })).resolves.toBeUndefined();
    expect(await db.one(app.documents.where({ id: row.id }))).toEqual(row);
  });
});
