import { localAccountConfig } from "./testing/account-fixtures.js";
import { afterEach, describe, expect, it } from "vitest";
import { testApp } from "../testing/test-app.js";
import type { RowOf } from "../typed-app.js";
import { type Db, type DbSubscriptionUpdate } from "./db.js";
import { createDb } from "./default-create-db.js";
import { nativeCoreErrorCode } from "./native-runtime/native-error-code.js";

const app = testApp({ tables: { notes: { columns: { title: "string", done: "boolean" } } } });
type Note = RowOf<typeof app.notes>;

const largeValues = testApp({
  tables: {
    documents: {
      columns: { payload: "bytes", body: "string", metadata: { json: true }, done: "boolean" },
    },
  },
});

const texts = testApp({
  tables: { pages: { columns: { body: { type: "string", merge: "markdown" } } } },
});

const optionalJson = testApp({
  tables: { jobs: { columns: { title: "string", meta: { json: true, optional: true } } } },
});

describe("createDb in-memory driver", () => {
  let db: Db | undefined;

  afterEach(async () => {
    await db?.shutdown();
    db = undefined;
  });

  it("leaves operator and orderBy checks to the engine, which refuses them by code", async () => {
    db = await createDb({
      ...(await localAccountConfig("in-memory-operator-checks-test")),
      driver: { type: "memory" },
    });
    for (const query of [
      largeValues.documents.where({ metadata: { gt: 1 } } as never),
      largeValues.documents.orderBy("payload" as never),
    ]) {
      const refused = await db.all(query).then(
        () => undefined,
        (error: unknown) => error,
      );
      expect(nativeCoreErrorCode(refused), String(refused)).toBe("query");
    }
    await expect(db.all(largeValues.documents.orderBy("done"))).resolves.toEqual([]);
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
    const transformed = testApp(
      {
        tables: {
          pages: {
            columns: { body: { type: "string", merge: "markdown", transform: "identity" } },
          },
        },
        transforms: { identity: { stored: "string", view: "string" } },
      },
      {
        transforms: {
          identity: { from: (value: string) => value, to: (value: string) => value },
        },
      },
    );
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

  it("mixes column names and partial selections in any order (#5192)", async () => {
    db = await createDb({
      ...(await localAccountConfig("in-memory-select-mix-test")),
      driver: { type: "memory" },
    });
    const { value: inserted } = db.insert(largeValues.documents, {
      payload: new Uint8Array([0, 1, 2, 3, 4, 5]),
      body: "abcdefgh",
      metadata: { nested: { answer: 42 }, other: "x" },
      done: true,
    });
    const documents = largeValues.documents.where({ id: inserted.id });

    const [rangeFirst] = await db.all(documents.select({ body: { from: 2, to: 5 } }, "done"));
    expect(rangeFirst).toEqual({ id: inserted.id, body: "cde", done: true });

    const [namesFirst] = await db.all(documents.select("done", { body: { from: 2, to: 5 } }));
    expect(namesFirst).toEqual({ id: inserted.id, done: true, body: "cde" });

    const [interleaved] = await db.all(
      documents.select(
        { payload: { from: 1, to: 3 } },
        "done",
        { metadata: { at: "/nested/answer" } },
        { body: { from: 0, to: 2 } },
      ),
    );
    expect(interleaved).toEqual({
      id: inserted.id,
      payload: new Uint8Array([1, 2]),
      done: true,
      metadata: 42,
      body: "ab",
    });

    const [everythingBodyInPart] = await db.all(
      documents.select("*", { body: { from: 6, to: 8 } }),
    );
    expect(everythingBodyInPart).toMatchObject({
      id: inserted.id,
      payload: new Uint8Array([0, 1, 2, 3, 4, 5]),
      body: "gh",
      metadata: { nested: { answer: 42 }, other: "x" },
      done: true,
    });
  });

  it("refuses a select that names a column both whole and in part (#5192)", () => {
    const documents = largeValues.documents;
    expect(() => documents.select({ body: { from: 0, to: 2 } }, "body")).toThrow(
      'select(...): column "body" is selected both whole and in part',
    );
    expect(() => documents.select("body", { body: { from: 0, to: 2 } })).toThrow(
      'select(...): column "body" is selected both whole and in part',
    );
    expect(() =>
      documents.select({ body: { from: 0, to: 2 } }, { body: { from: 4, to: 6 } }),
    ).toThrow('select(...): column "body" has more than one partial selection');
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

  it("keeps a range subscription alive as the text shrinks below the range and grows back (#5502)", async () => {
    db = await createDb({
      ...(await localAccountConfig("in-memory-range-shrink-test")),
      driver: { type: "memory" },
    });
    const { value: note } = db.insert(texts.pages, { body: "abcdefgh" });
    await Promise.resolve();
    const pages = texts.pages.where({ id: note.id });
    const seen = (from: number, to: number) => {
      const bodies: string[] = [];
      const errors: unknown[] = [];
      const stop = db!.subscribe(pages.select({ body: { from, to } }), {
        onUpdate: (rows) => bodies.push(...rows.map((row) => row.body)),
        onError: (error) => errors.push(error),
      });
      return { bodies, errors, stop };
    };
    const whole = seen(0, 8);
    const middle = seen(2, 6);
    const tail = seen(4, 8);
    const settled = async (body: string, wants: Array<[typeof whole, string]>) => {
      await db!.update(texts.pages, note.id, { body }).wait({ tier: "local" });
      for (const [subscription, want] of wants) {
        await expect.poll(() => subscription.bodies.at(-1)).toBe(want);
        expect(subscription.errors).toEqual([]);
      }
    };
    await expect.poll(() => tail.bodies.at(-1)).toBe("efgh");

    // Shrunk below the ranges: clamped, empty where the range starts past the end.
    await settled("ab", [
      [whole, "ab"],
      [middle, ""],
      [tail, ""],
    ]);
    // A one-shot read of the same selection clamps alike.
    expect(await db.all(pages.select({ body: { from: 4, to: 8 } }))).toEqual([
      { id: note.id, body: "" },
    ]);
    // Partly overlapping, with a surrogate pair inside the value.
    await settled("abcd🚀", [
      [whole, "abcd🚀"],
      [middle, "cd🚀"],
      [tail, "🚀"],
    ]);
    // Grown back: the windows show again.
    await settled("abcdefgh", [
      [whole, "abcdefgh"],
      [middle, "cdef"],
      [tail, "efgh"],
    ]);
    whole.stop();
    middle.stop();
    tail.stop();
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

  it("clamps a range on a column a computed column makes it read whole (#5502)", async () => {
    // The engine reads body and payload whole for `size`, so the selection
    // is taken from the whole value in TypeScript; it clamps like the engine.
    const computedApp = testApp(
      {
        tables: {
          documents: {
            columns: { body: "string", payload: "bytes" },
            computed: { size: { type: "int", from: ["body", "payload"] } },
          },
        },
      },
      {
        computed: {
          documents: {
            size: (row: { body: string; payload: Uint8Array }) =>
              row.body.length + row.payload.length,
          },
        },
      },
    );
    db = await createDb({
      ...(await localAccountConfig("in-memory-whole-read-range-clamp-test")),
      driver: { type: "memory" },
    });
    const bytes = (text: string) => new TextEncoder().encode(text);
    const { value: inserted } = db.insert(computedApp.documents, {
      body: "abcdefgh",
      payload: bytes("abcdefgh"),
    });
    await Promise.resolve();
    const documents = computedApp.documents.where({ id: inserted.id });
    const queries = {
      utf16: documents.select({ body: { from: 4, to: 8 } }, "size"),
      utf8: documents.select({ body: { fromUtf8: 2, toUtf8: 8 } }, "size"),
      bytes: documents.select({ payload: { from: 2, to: 6 } }, "size"),
    };
    const shown = { utf16: [] as unknown[], utf8: [] as unknown[], bytes: [] as unknown[] };
    const errors: unknown[] = [];
    const stops = (Object.keys(queries) as Array<keyof typeof queries>).map((key) =>
      db!.subscribe(queries[key] as typeof queries.utf16, {
        onUpdate: (rows) => shown[key].push(...rows),
        onError: (error) => errors.push(error),
      }),
    );
    const settled = async (
      body: string,
      want: { utf16: string; utf8: string; bytes: Uint8Array; size: number },
    ) => {
      await db!
        .update(computedApp.documents, inserted.id, { body, payload: bytes(body) })
        .wait({ tier: "local" });
      await expect
        .poll(() => shown.utf16.at(-1))
        .toEqual({ id: inserted.id, body: want.utf16, size: want.size });
      await expect
        .poll(() => shown.utf8.at(-1))
        .toEqual({ id: inserted.id, body: want.utf8, size: want.size });
      await expect
        .poll(() => shown.bytes.at(-1))
        .toEqual({ id: inserted.id, payload: want.bytes, size: want.size });
      expect(errors).toEqual([]);
    };

    await settled("ab", { utf16: "", utf8: "", bytes: bytes(""), size: 4 });
    expect(await db.all(queries.utf16)).toEqual([{ id: inserted.id, body: "", size: 4 }]);
    // "abcd🚀" is 6 UTF-16 units and 8 UTF-8 bytes; a byte range may end
    // inside the emoji, a text range may not.
    await settled("abcd🚀", {
      utf16: "🚀",
      utf8: "cd🚀",
      bytes: bytes("abcd🚀").slice(2, 6),
      size: 14,
    });
    await settled("abcdefgh", { utf16: "efgh", utf8: "cdefgh", bytes: bytes("cdef"), size: 16 });
    for (const stop of stops) stop();

    // Inverted ranges still fail, with the reason.
    await expect(async () =>
      db!.all(documents.select({ body: { from: 3, to: 1 } }, "size")),
    ).rejects.toThrow(/ends before it starts/);
  });

  it("refuses splices on a range-selected column that a computed column makes it read whole", async () => {
    const computedApp = testApp(
      {
        tables: {
          documents: {
            columns: { body: "string" },
            computed: { preview: { type: "string", from: ["body"] } },
          },
        },
      },
      { computed: { documents: { preview: (row: { body: string }) => row.body.slice(0, 3) } } },
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
    const hopApp = testApp({
      tables: {
        documents: { columns: { body: "string" } },
        links: {
          columns: { documentId: "uuid" },
          relations: { document: { rel: "documents", column: "documentId" } },
        },
      },
    });
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
