import { localAccountConfig } from "./testing/account-fixtures.js";
import { afterEach, describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import { type Db } from "./db.js";
import { createDb } from "./default-create-db.js";

const schema = {
  notes: s.table(
    {
      title: s.string(),
      done: s.boolean(),
    },
    {},
  ),
};

type AppSchema = s.Schema<typeof schema>;
const app: s.App<AppSchema> = s.defineApp(schema);
type Note = s.RowOf<typeof app.notes>;

const largeValueSchema = {
  documents: s.table(
    {
      payload: s.bytes(),
      body: s.string(),
      metadata: s.json(),
      done: s.boolean(),
    },
    {},
  ),
};
type LargeValueAppSchema = s.Schema<typeof largeValueSchema>;
const largeValues: s.App<LargeValueAppSchema> = s.defineApp(largeValueSchema);

const optionalJsonSchema = {
  jobs: s.table(
    {
      title: s.string(),
      meta: s.json().optional(),
    },
    {},
  ),
};
type OptionalJsonAppSchema = s.Schema<typeof optionalJsonSchema>;
const optionalJson: s.App<OptionalJsonAppSchema> = s.defineApp(optionalJsonSchema);

describe("createDb in-memory driver", () => {
  let db: Db | undefined;

  afterEach(async () => {
    await db?.shutdown();
    db = undefined;
  });

  it("opens a native Db with the current ordinary-column schema layout", async () => {
    db = await createDb({
      ...(await localAccountConfig("in-memory-current-column-layout-test")),
      driver: { type: "memory" },
    });

    // Opening the native client decodes and compiles the TypeScript-authored
    // source schema before this query can run.
    await expect(db.all(app.notes)).resolves.toEqual([]);
  });

  it("can read and write data without connecting to a server", async () => {
    db = await createDb({
      ...(await localAccountConfig("in-memory-db-test")),
      driver: { type: "memory" },
    });

    const { value: inserted } = db.insert(app.notes, {
      title: "Draft test",
      done: false,
    });

    await db.update(app.notes, inserted.id, { done: true }).wait({ tier: "local" });

    const updated = await db.one<Note>(app.notes.where({ id: { eq: inserted.id } }));
    expect(updated).toEqual({
      id: inserted.id,
      title: "Draft test",
      done: true,
    });

    const rows = await db.all<Note>(app.notes.where({ done: true }));
    expect(rows).toEqual([updated]);
  });

  it("writes present values to an optional JSON column", async () => {
    db = await createDb({
      ...(await localAccountConfig("in-memory-optional-json-test")),
      driver: { type: "memory" },
    });

    const { value: withObject } = db.insert(optionalJson.jobs, {
      title: "object",
      meta: { a: 1 },
    });
    const { value: unset } = db.insert(optionalJson.jobs, { title: "unset" });
    await db.update(optionalJson.jobs, unset.id, { meta: [1, "two"] }).wait({ tier: "local" });
    const { value: exclusive } = await db.exclusiveTransaction((tx) =>
      tx.insert(optionalJson.jobs, { title: "exclusive", meta: { b: 2 } }),
    );

    const rows = await db.all(optionalJson.jobs);
    expect(rows.find((row) => row.id === withObject.id)?.meta).toEqual({ a: 1 });
    expect(rows.find((row) => row.id === unset.id)?.meta).toEqual([1, "two"]);
    expect(rows.find((row) => row.id === exclusive.id)?.meta).toEqual({ b: 2 });
  });

  it("executes typed partial selects and page-relative diffs end to end", async () => {
    db = await createDb({
      ...(await localAccountConfig("in-memory-large-value-dsl-test")),
      driver: { type: "memory" },
    });

    const payloadOffset = 70_000;
    const payload = new Uint8Array(payloadOffset + 6).fill(7);
    payload.set([0, 1, 2, 3, 4, 5], payloadOffset);
    const textPrefix = "a".repeat(70_000);
    const body = `${textPrefix}A😀BC`;
    const { value: inserted } = db.insert(largeValues.documents, {
      payload,
      body,
      metadata: { padding: "p".repeat(70_000), nested: { answer: 42 } },
      done: false,
    });
    const [page] = await db.all(
      largeValues.documents.where({ id: inserted.id }).select({
        payload: { from: payloadOffset + 1, to: payloadOffset + 5 },
        body: { from: textPrefix.length + 1, to: textPrefix.length + 3 },
        metadata: { at: "/nested/answer" },
      }),
    );
    expect(page).toEqual({
      id: inserted.id,
      payload: new Uint8Array([1, 2, 3, 4]),
      body: "😀",
      metadata: 42,
    });

    const [utf8Page] = await db.all(
      largeValues.documents.where({ id: inserted.id }).select({
        body: { fromUtf8: textPrefix.length + 1, toUtf8: textPrefix.length + 5 },
      }),
    );
    expect(utf8Page).toEqual({ id: inserted.id, body: "😀" });

    await db
      .update(
        largeValues.documents,
        inserted.id,
        { done: true },
        {
          applyDiffs: {
            payload: {
              within: { from: payloadOffset + 1, to: payloadOffset + 5 },
              splices: [{ at: 1, delete: 2, insert: new Uint8Array([9, 8]) }],
            },
            body: {
              within: { from: textPrefix.length + 1, to: textPrefix.length + 3 },
              splices: [{ at: 0, delete: 2, insert: "🪩" }],
            },
            metadata: { edits: [{ op: "set", at: "/nested/answer", value: 43 }] },
          },
        },
      )
      .wait({ tier: "local" });

    await db
      .update(
        largeValues.documents,
        inserted.id,
        {},
        {
          applyDiffs: {
            body: {
              within: { fromUtf8: textPrefix.length + 1, toUtf8: textPrefix.length + 5 },
              splices: [{ atUtf8: 0, deleteUtf8: 4, insert: "🚀" }],
            },
          },
        },
      )
      .wait({ tier: "local" });

    const [updated] = await db.all(
      largeValues.documents.where({ id: inserted.id }).select({
        payload: { from: payloadOffset, to: payloadOffset + 6 },
        body: { from: textPrefix.length + 1, to: textPrefix.length + 3 },
        metadata: { at: "/nested/answer" },
      }),
    );
    expect(updated).toEqual({
      id: inserted.id,
      payload: new Uint8Array([0, 1, 9, 8, 4, 5]),
      body: "🚀",
      metadata: 43,
    });
  });

  it("composes page-relative diffs inside mergeable and exclusive transactions", async () => {
    db = await createDb({
      ...(await localAccountConfig("in-memory-large-value-transaction-diff-test")),
      driver: { type: "memory" },
    });

    const prefix = "a".repeat(70_000);
    const { value: inserted } = db.insert(largeValues.documents, {
      payload: new Uint8Array(),
      body: prefix,
      metadata: { nested: { answer: 42 } },
      done: false,
    });
    const tail = (length: number) =>
      largeValues.documents
        .where({ id: inserted.id })
        .select({ body: { from: prefix.length, to: prefix.length + length } });

    const mergeable = await db.transaction(async (tx) => {
      tx.update(
        largeValues.documents,
        inserted.id,
        {},
        {
          applyDiffs: {
            body: {
              within: { from: prefix.length, to: prefix.length },
              splices: [{ at: 0, delete: 0, insert: "one" }],
            },
          },
        },
      );
      // The second append addresses the transaction's own first append.
      tx.update(
        largeValues.documents,
        inserted.id,
        { done: true },
        {
          applyDiffs: {
            body: {
              within: { from: prefix.length + 3, to: prefix.length + 3 },
              splices: [{ at: 0, delete: 0, insert: "two" }],
            },
            metadata: { edits: [{ op: "set", at: "/nested/answer", value: 43 }] },
          },
        },
      );
      const [seen] = await tx.all(tail(6));
      const [outside] = await db.all(tail(6));
      return { seen: seen?.body, outside: outside?.body };
    });
    expect(mergeable.value).toEqual({ seen: "onetwo", outside: "" });
    await mergeable.wait({ tier: "local" });
    await expect(db.all(tail(6))).resolves.toEqual([{ id: inserted.id, body: "onetwo" }]);
    await expect(db.one(largeValues.documents.where({ id: inserted.id }))).resolves.toMatchObject({
      done: true,
    });

    await db.exclusiveTransaction((tx) => {
      tx.update(
        largeValues.documents,
        inserted.id,
        {},
        {
          applyDiffs: {
            body: {
              within: { from: prefix.length + 6, to: prefix.length + 6 },
              splices: [{ at: 0, delete: 0, insert: "three" }],
            },
          },
        },
      );
    });
    await expect(db.all(tail(11))).resolves.toEqual([{ id: inserted.id, body: "onetwothree" }]);
    const [metadata] = await db.all(
      largeValues.documents
        .where({ id: inserted.id })
        .select({ metadata: { at: "/nested/answer" } }),
    );
    expect(metadata).toEqual({ id: inserted.id, metadata: 43 });
  });

  it("keeps descriptor-shaped JSON values ordinary for direct and transactional upserts", async () => {
    db = await createDb({
      ...(await localAccountConfig("in-memory-large-value-json-upsert-shape-test")),
      driver: { type: "memory" },
    });

    const { value: inserted } = db.insert(largeValues.documents, {
      payload: new Uint8Array(),
      body: "body",
      metadata: {},
      done: false,
    });
    const direct = { edits: [{ op: "set", at: "/ordinary", value: "direct" }] };
    await db
      .upsert(largeValues.documents, inserted.id, { metadata: direct })
      .wait({ tier: "local" });
    await expect(
      db.one(largeValues.documents.where({ id: inserted.id }), { tier: "local" }),
    ).resolves.toMatchObject({ metadata: direct });

    const transactional = { edits: [{ op: "set", at: "/ordinary", value: "transaction" }] };
    const committed = await db.transaction((tx) => {
      tx.upsert(largeValues.documents, inserted.id, { metadata: transactional });
    });
    await committed.wait({ tier: "local" });
    await expect(
      db.one(largeValues.documents.where({ id: inserted.id }), { tier: "local" }),
    ).resolves.toMatchObject({ metadata: transactional });
  });
});
