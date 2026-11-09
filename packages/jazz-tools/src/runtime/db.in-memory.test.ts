import { localAccountConfig } from "./testing/account-fixtures.js";
import { afterEach, describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import { type Db, type DbSubscriptionUpdate } from "./db.js";
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
const app: s.App<AppSchema> = s.defineApp(schema, { migrations: "none" });
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
const largeValues: s.App<LargeValueAppSchema> = s.defineApp(largeValueSchema, {
  migrations: "none",
});

const textSchema = {
  pages: s.table({ body: s.string().merge("markdown") }, {}),
};
type TextAppSchema = s.Schema<typeof textSchema>;
const texts: s.App<TextAppSchema> = s.defineApp(textSchema, { migrations: "none" });

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
const optionalJson: s.App<OptionalJsonAppSchema> = s.defineApp(optionalJsonSchema, {
  migrations: "none",
});

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

  it("merges a text edit over a value its editor had not shown yet", async () => {
    db = await createDb({
      ...(await localAccountConfig("in-memory-text-edit-test")),
      driver: { type: "memory" },
    });
    const { value: page } = db.insert(texts.pages, { body: "Hello world" });
    // A merged value arrives in the store before the editor shows it...
    await db.update(texts.pages, page.id, { body: "Hello there world" }).wait({ tier: "local" });
    // ...and the editor, still showing "Hello world", types "!".
    await db
      .update(
        texts.pages,
        page.id,
        { body: "Hello world!" },
        { editedFrom: { body: "Hello world" } },
      )
      .wait({ tier: "local" });
    const [read] = await db.all(texts.pages.where({ id: page.id }));
    expect(read?.body).toBe("Hello there world!");

    // An edit of the value the column holds is written as it is.
    await db
      .update(
        texts.pages,
        page.id,
        { body: "Hi there world!" },
        { editedFrom: { body: "Hello there world!" } },
      )
      .wait({ tier: "local" });
    const [again] = await db.all(texts.pages.where({ id: page.id }));
    expect(again?.body).toBe("Hi there world!");
  });

  it("merges a transaction's text edit over a value its editor had not shown yet", async () => {
    db = await createDb({
      ...(await localAccountConfig("in-memory-text-edit-transaction-test")),
      driver: { type: "memory" },
    });
    const { value: page } = db.insert(texts.pages, { body: "Hello world" });
    await db.update(texts.pages, page.id, { body: "Hello there world" }).wait({ tier: "local" });
    const committed = await db.transaction((tx) => {
      tx.update(
        texts.pages,
        page.id,
        { body: "Hello world!" },
        { editedFrom: { body: "Hello world" } },
      );
    });
    await committed.wait({ tier: "local" });
    const [read] = await db.all(texts.pages.where({ id: page.id }));
    expect(read?.body).toBe("Hello there world!");
  });

  it("writes a column left undefined in editedFrom as usual", async () => {
    db = await createDb({
      ...(await localAccountConfig("in-memory-text-edit-undefined-test")),
      driver: { type: "memory" },
    });
    const { value: page } = db.insert(texts.pages, { body: "Hello world" });
    await db.update(texts.pages, page.id, { body: "Hello there" }).wait({ tier: "local" });
    await db
      .update(texts.pages, page.id, { body: "Bye" }, { editedFrom: { body: undefined } })
      .wait({ tier: "local" });
    const [read] = await db.all(texts.pages.where({ id: page.id }));
    expect(read?.body).toBe("Bye");
  });

  it("refuses editedFrom it cannot honour", async () => {
    const transformedSchema = {
      pages: s.table(
        {
          body: s
            .string()
            .transform({ from: (value: string) => value, to: (value: string) => value })
            .merge("markdown"),
        },
        {},
      ),
    };
    const transformed: s.App<s.Schema<typeof transformedSchema>> = s.defineApp(transformedSchema, {
      migrations: "none",
    });
    // A Db holds one schema: each app below gets its own.
    const open = async (name: string) => {
      await db?.shutdown();
      db = await createDb({
        ...(await localAccountConfig(`in-memory-text-edit-refusals-${name}`)),
        driver: { type: "memory" },
      });
      return db;
    };
    await open("texts");
    const { value: page } = db!.insert(texts.pages, { body: "Hello" });
    // A value to edit from, but no text to write.
    expect(() => db!.update(texts.pages, page.id, {}, { editedFrom: { body: "Hello" } })).toThrow(
      'a string replacement for "body"',
    );
    // A column that does not merge text.
    await open("plain");
    const { value: note } = db!.insert(largeValues.documents, {
      payload: new Uint8Array(),
      body: "Hello",
      metadata: {},
      done: false,
    });
    await expect(
      (async () =>
        db!
          .update(
            largeValues.documents,
            note.id,
            { body: "Hello!" },
            { editedFrom: { body: "Hello" } },
          )
          .wait({ tier: "local" }))(),
    ).rejects.toThrow();
    // A column with a transform.
    await open("transformed");
    const { value: other } = db!.insert(transformed.pages, { body: "Hello" });
    expect(() =>
      db!.update(
        transformed.pages,
        other.id,
        { body: "Hello!" },
        { editedFrom: { body: "Hello" } },
      ),
    ).toThrow("which has a transform");
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
    const { value: serializable } = await db.serializableTransaction((tx) =>
      tx.insert(optionalJson.jobs, { title: "serializable", meta: { b: 2 } }),
    );

    const rows = await db.all(optionalJson.jobs);
    expect(rows.find((row) => row.id === withObject.id)?.meta).toEqual({ a: 1 });
    expect(rows.find((row) => row.id === unset.id)?.meta).toEqual([1, "two"]);
    expect(rows.find((row) => row.id === serializable.id)?.meta).toEqual({ b: 2 });
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

  it("composes page-relative diffs inside mergeable and serializable transactions", async () => {
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
      // Outside the transaction the committed body is still just the prefix.
      const outside = await db!.one(largeValues.documents.where({ id: inserted.id }));
      return { seen: seen?.body, outsideLength: outside?.body.length };
    });
    expect(mergeable.value).toEqual({ seen: "onetwo", outsideLength: prefix.length });
    await mergeable.wait({ tier: "local" });
    await expect(db.all(tail(6))).resolves.toEqual([{ id: inserted.id, body: "onetwo" }]);
    await expect(db.one(largeValues.documents.where({ id: inserted.id }))).resolves.toMatchObject({
      done: true,
    });

    await db.serializableTransaction((tx) => {
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

  it("delivers opted-in columns' changes as splices, page-relative on a range select", async () => {
    db = await createDb({
      ...(await localAccountConfig("in-memory-large-value-subscription-splices-test")),
      driver: { type: "memory" },
    });

    const prefix = "a".repeat(70_000);
    const { value: inserted } = db.insert(largeValues.documents, {
      payload: new Uint8Array(70_000),
      body: `${prefix}hello world`,
      metadata: {},
      done: false,
    });
    await Promise.resolve();
    const whole: DbSubscriptionUpdate[] = [];
    const page: Array<{ rows: unknown[]; update: DbSubscriptionUpdate }> = [];
    const stopWhole = db.subscribe(
      largeValues.documents.where({ id: inserted.id }),
      { onUpdate: (_rows, update) => whole.push(update) },
      { splices: ["body", "payload"] },
    );
    const stopPage = db.subscribe(
      largeValues.documents
        .where({ id: inserted.id })
        .select({ body: { from: prefix.length + 6, to: prefix.length + 11 } }),
      { onUpdate: (rows, update) => page.push({ rows, update }) },
      { splices: ["body"] },
    );
    await expect.poll(() => whole.length > 0 && page.length > 0).toBe(true);

    await db
      .update(
        largeValues.documents,
        inserted.id,
        {},
        {
          applyDiffs: {
            body: {
              within: { from: prefix.length + 6, to: prefix.length + 11 },
              splices: [{ at: 0, delete: 5, insert: "🪩 wide" }],
            },
            payload: {
              within: { from: 10, to: 12 },
              splices: [{ at: 0, delete: 2, insert: new Uint8Array([7]) }],
            },
          },
        },
      )
      .wait({ tier: "local" });

    await expect.poll(() => whole.at(-1)?.splices.get(inserted.id)).toBeDefined();
    expect(whole.at(-1)!.splices.get(inserted.id)).toEqual({
      body: [{ at: prefix.length + 6, delete: 5, insert: "🪩 wide" }],
      payload: [{ at: 10, delete: 2, insert: new Uint8Array([7]) }],
    });
    // The window now covers "🪩 wi"; its splice turns "world" into exactly that.
    await expect.poll(() => page.at(-1)?.update.splices.get(inserted.id)).toBeDefined();
    const last = page.at(-1)!;
    expect(last.rows).toEqual([{ id: inserted.id, body: "🪩 wi" }]);
    const splices = last.update.splices.get(inserted.id)!.body!;
    let text = "world";
    for (const splice of splices) {
      text =
        text.slice(0, splice.at) +
        (splice.insert as string) +
        text.slice(splice.at + splice.delete);
    }
    expect(text).toBe("🪩 wi");

    stopWhole();
    stopPage();
    expect(() =>
      db!.subscribe(largeValues.documents, { onUpdate: () => {} }, { splices: ["metadata"] }),
    ).toThrow(/not supported yet/);
  });

  it("refuses splices on a range-selected column that a computed column makes it read whole", async () => {
    const computedApp = s.defineApp(
      {
        documents: s.table(
          { body: s.string() },
          { preview: s.computed((row: { body: string }) => row.body.slice(0, 3)) },
        ),
      },
      { migrations: "none" },
    );
    db = await createDb({
      ...(await localAccountConfig("in-memory-splices-whole-read-test")),
      driver: { type: "memory" },
    });
    const { value: inserted } = db.insert(computedApp.documents, { body: "abcdefgh" });
    await Promise.resolve();
    const errors: unknown[] = [];
    let threw: unknown;
    try {
      const stop = db.subscribe(
        computedApp.documents
          .where({ id: inserted.id })
          .select({ body: { from: 2, to: 5 } }, "preview"),
        { onUpdate: () => {}, onError: (error) => errors.push(error) },
        { splices: ["body"] },
      );
      await expect.poll(() => errors.length).toBeGreaterThan(0);
      stop();
    } catch (error) {
      threw = error;
    }
    expect(String(threw ?? errors[0])).toMatch(/read whole; splices are not supported yet/);
  });

  it("delivers splices for a hopped query's output rows", async () => {
    const hopApp = s.defineApp(
      {
        documents: s.table({ body: s.string() }, {}),
        links: s.table({ documentId: s.uuid() }, { document: s.rel("documents", "documentId") }),
      },
      { migrations: "none" },
    );
    db = await createDb({
      ...(await localAccountConfig("in-memory-splices-hop-test")),
      driver: { type: "memory" },
    });
    const prefix = "a".repeat(70_000);
    const { value: doc } = db.insert(hopApp.documents, { body: `${prefix}hello` });
    const { value: link } = db.insert(hopApp.links, { documentId: doc.id });
    await Promise.resolve();
    const updates: DbSubscriptionUpdate[] = [];
    const stop = db.subscribe(
      hopApp.links.where({ id: link.id }).hopTo("document"),
      { onUpdate: (_rows, update) => updates.push(update) },
      { splices: ["body"] },
    );
    await expect.poll(() => updates.length).toBeGreaterThan(0);
    await db
      .update(
        hopApp.documents,
        doc.id,
        {},
        {
          applyDiffs: {
            body: {
              within: { from: prefix.length, to: prefix.length + 5 },
              splices: [{ at: 0, delete: 5, insert: "world" }],
            },
          },
        },
      )
      .wait({ tier: "local" });
    await expect.poll(() => updates.at(-1)?.splices.get(doc.id)).toBeDefined();
    expect(updates.at(-1)!.splices.get(doc.id)).toEqual({
      body: [{ at: prefix.length, delete: 5, insert: "world" }],
    });
    stop();
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
      db.one(largeValues.documents.where({ id: inserted.id }), { tier: "local-first" }),
    ).resolves.toMatchObject({ metadata: direct });

    const transactional = { edits: [{ op: "set", at: "/ordinary", value: "transaction" }] };
    const committed = await db.transaction((tx) => {
      tx.upsert(largeValues.documents, inserted.id, { metadata: transactional });
    });
    await committed.wait({ tier: "local" });
    await expect(
      db.one(largeValues.documents.where({ id: inserted.id }), { tier: "local-first" }),
    ).resolves.toMatchObject({ metadata: transactional });
  });
});
