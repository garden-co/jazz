import { PROVENANCE_MAGIC_COLUMNS, type RowAuthor } from "../../src/magic-columns.js";
import type { TxId } from "../../src/runtime/client.js";
import { afterEach, beforeEach, describe, expect, expectTypeOf, it } from "vitest";
import { schema as s, migration as m } from "../../src/index.js";
import { computeSchemaHash } from "../../src/dev/catalogue.js";
import { deploy, startLocalJazzServer } from "../../src/testing/index.js";
import { createDb } from "../../src/runtime/default-create-db.js";
import { localAccountConfig } from "../../src/runtime/testing/account-fixtures.js";
import type { Db, QueryBuilder } from "../../src/runtime/db.js";

const app = s.defineApp({ notes: s.table({ title: s.string(), done: s.boolean() }, {}) });
const permissions = s.definePermissions(app, ({ policy }) => {
  policy.notes.allowRead.where({ done: false });
  policy.notes.allowInsert.always();
  policy.notes.allowUpdate.always();
  policy.notes.allowDelete.always();
});

describe("TS Query API history", () => {
  let db: Db;
  let subscriptions: (() => void)[];
  let server: Awaited<ReturnType<typeof startLocalJazzServer>>;

  beforeEach(async () => {
    subscriptions = [];
    server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    db = await createDb({
      ...(await localAccountConfig(server.appId, server.url)),
      driver: { type: "persistent" },
    });
  });

  afterEach(async () => {
    try {
      for (const stop of subscriptions) stop();
      await db?.shutdown();
    } finally {
      await server?.stop();
    }
  });

  function insertNote(title = "first") {
    return db.insert(app.notes, { title, done: false }).wait({ tier: "global" });
  }

  function watch<T extends { id: string }>(query: QueryBuilder<T>) {
    let latest: T[] | undefined;
    let failure: Error | undefined;
    subscriptions.push(
      db.subscribe(query, {
        onUpdate: (rows) => {
          latest = rows;
        },
        onError: (error) => {
          failure = error;
        },
      }),
    );
    return () => {
      if (failure) throw failure;
      return latest;
    };
  }

  it("reads each selected row's versions once even when IDs repeat", async () => {
    const a = await insertNote("a1");
    const b = await insertNote("b1");
    await insertNote("unrelated");
    await db.update(app.notes, a.id, { title: "a2" }).wait({ tier: "global" });
    const rows = await db.all(
      app.$history.notes
        .where({ id: { in: [a.id, b.id, a.id] } })
        .orderBy("title")
        .select("title"),
    );
    expect(rows).toEqual([
      { id: a.id, title: "a1" },
      { id: a.id, title: "a2" },
      { id: b.id, title: "b1" },
    ]);
  });

  it("returns an empty query and subscription for an empty ID list", async () => {
    await insertNote();
    const query = app.$history.notes.where({ id: { in: [] } });
    expect(await db.all(query)).toEqual([]);
    const latest = watch(query);
    await expect.poll(latest).toEqual([]);
  });

  it("orders and paginates across the combined histories", async () => {
    const a = await insertNote("a");
    const b = await insertNote("b");
    await db.update(app.notes, a.id, { title: "d" }).wait({ tier: "global" });
    await db.update(app.notes, b.id, { title: "c" }).wait({ tier: "global" });
    expect(
      await db.all(
        app.$history.notes
          .where({ id: { in: [a.id, b.id] } })
          .orderBy("title")
          .offset(1)
          .limit(2)
          .select("title"),
      ),
    ).toEqual([
      { id: b.id, title: "b" },
      { id: b.id, title: "c" },
    ]);
  });

  it("subscribes to new versions of every selected row", async () => {
    const a = await insertNote("a1");
    const b = await insertNote("b1");
    const latest = watch(
      app.$history.notes
        .where({ id: { in: [a.id, b.id] } })
        .orderBy("title")
        .select("title"),
    );
    await expect.poll(latest).toEqual([
      { id: a.id, title: "a1" },
      { id: b.id, title: "b1" },
    ]);
    await db.update(app.notes, a.id, { title: "a2" }).wait({ tier: "global" });
    await db.update(app.notes, b.id, { title: "b2" }).wait({ tier: "global" });
    await expect.poll(latest).toEqual([
      { id: a.id, title: "a1" },
      { id: a.id, title: "a2" },
      { id: b.id, title: "b1" },
      { id: b.id, title: "b2" },
    ]);
  });

  it("observes a selected row that did not exist when subscribed", async () => {
    const a = await insertNote("a");
    const futureId = "00000000-0000-4000-8000-000000000001";
    const latest = watch(
      app.$history.notes
        .where({ id: { in: [a.id, futureId] } })
        .orderBy("title")
        .select("title"),
    );
    await expect.poll(latest).toEqual([{ id: a.id, title: "a" }]);
    await db.upsert(app.notes, futureId, { title: "b", done: false }).wait({ tier: "global" });
    await expect.poll(latest).toEqual([
      { id: a.id, title: "a" },
      { id: futureId, title: "b" },
    ]);
  });

  it("returns typed content and transaction metadata for an accepted version", async () => {
    const startedAt = Date.now();
    const note = await insertNote();
    const rows = await db.all(app.$history.notes.where({ id: note.id }));
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({ id: note.id, title: "first", done: false });
    expectTypeOf(rows[0]!.$acceptedAt).toEqualTypeOf<Date>();
    expectTypeOf(rows[0]!.$transactionId).toEqualTypeOf<TxId>();
    expect(rows[0]!.$acceptedAt).toBeInstanceOf(Date);
    expect(rows[0]!.$acceptedAt.getTime()).toBeGreaterThanOrEqual(startedAt);
    expect(rows[0]!.$acceptedAt.getTime()).toBeLessThanOrEqual(Date.now());
    const latest = watch(app.$history.notes.where({ id: note.id }).select("$acceptedAt"));
    await expect.poll(latest).toEqual([{ id: note.id, $acceptedAt: rows[0]!.$acceptedAt }]);
    expect(typeof rows[0]!.$transactionId).toBe("string");
  });

  it("projects selected fields while preserving the logical row id", async () => {
    const note = await insertNote();
    const rows = await db.all(app.$history.notes.where({ id: note.id }).select("title"));
    expectTypeOf(rows).toEqualTypeOf<{ id: string; title: string }[]>();
    expect(rows).toEqual([{ id: note.id, title: "first" }]);
  });

  it("orders versions by acceptance sequence", async () => {
    const note = await insertNote();
    await db.update(app.notes, note.id, { title: "second" }).wait({ tier: "global" });
    const query = app.$history.notes.where({ id: note.id });
    const rows = await db.all(query.orderBy("$acceptedAt"));
    expect(rows.map((row) => row.title)).toEqual(["first", "second"]);
    expect(rows[1]!.$acceptedAt.getTime()).toBeGreaterThanOrEqual(rows[0]!.$acceptedAt.getTime());
    expect(rows[1]!.$transactionId).not.toEqual(rows[0]!.$transactionId);
    expect(await db.all(query.orderBy("$acceptedAt", "desc"))).toEqual([...rows].reverse());
  });

  it("applies offset and limit to ordered versions", async () => {
    const note = await insertNote();
    await db.update(app.notes, note.id, { title: "second" }).wait({ tier: "global" });
    await db.update(app.notes, note.id, { title: "third" }).wait({ tier: "global" });
    const page = app.$history.notes
      .where({ id: note.id })
      .orderBy("$acceptedAt")
      .offset(1)
      .limit(1)
      .select("title");
    expect(await db.all(page)).toEqual([{ id: note.id, title: "second" }]);
  });

  it("keeps equal projections as separate versions in subscriptions", async () => {
    const note = await insertNote("same");
    const query = app.$history.notes.where({ id: note.id }).select("title");
    const latest = watch(query);
    await expect.poll(latest).toEqual([{ id: note.id, title: "same" }]);
    await db.update(app.notes, note.id, { title: "same" }).wait({ tier: "global" });
    const expected = [
      { id: note.id, title: "same" },
      { id: note.id, title: "same" },
    ];
    await expect.poll(latest).toEqual(expected);
    expect(await db.all(query)).toEqual(expected);
  });

  it("retracts every retained version when the row is deleted", async () => {
    const note = await insertNote();
    await db.update(app.notes, note.id, { title: "second" }).wait({ tier: "global" });
    const query = app.$history.notes.where({ id: note.id });
    const latest = watch(query);
    await expect.poll(() => latest()?.length).toBe(2);
    await db.delete(app.notes, note.id).wait({ tier: "global" });
    await expect.poll(latest).toEqual([]);
    expect(await db.all(query)).toEqual([]);
  });

  it("restores old versions and adds the replacement content version", async () => {
    const note = await insertNote();
    const query = app.$history.notes.where({ id: note.id }).orderBy("$acceptedAt");
    const original = await db.all(query);
    await db.delete(app.notes, note.id).wait({ tier: "global" });
    const latest = watch(query);
    await expect.poll(latest).toEqual([]);
    await db
      .restore(app.notes, note.id, { title: "restored", done: false })
      .wait({ tier: "global" });
    await expect.poll(() => latest()?.map((row) => row.title)).toEqual(["first", "restored"]);
    const restored = await db.all(query);
    expect(restored).toHaveLength(2);
    expect(restored[0]).toEqual(original[0]);
    expect(restored[1]).toMatchObject({ id: note.id, title: "restored", done: false });
  });

  it("local-first history retracts cached versions when upstream access is revoked", async () => {
    const note = await insertNote();
    const query = app.$history.notes.where({ id: note.id }).orderBy("$acceptedAt");
    const latest = watch(query);
    await expect.poll(() => latest()?.length).toBe(1);
    // The deployed read policy denies done=true; writes remain allowed.
    await db.update(app.notes, note.id, { done: true }).wait({ tier: "global" });
    await expect.poll(latest).toEqual([]);
    expect(await db.all(query, { tier: "remote" })).toEqual([]);
  });

  it("includeDeleted returns content versions of a deleted row", async () => {
    const note = await insertNote("first");
    await db.update(app.notes, note.id, { title: "second" }).wait({ tier: "global" });
    const query = app.$history.notes.where({ id: note.id }).orderBy("$acceptedAt");
    const versions = await db.all(query);
    await db.delete(app.notes, note.id).wait({ tier: "global" });

    expect(await db.all(query)).toEqual([]);
    const retained = await db.all(query.includeDeleted());
    expectTypeOf(retained).toEqualTypeOf<typeof versions>();
    expect(retained).toEqual(versions);
  });

  it("includeDeleted supports filtering and projecting deleted history", async () => {
    const note = await insertNote("first");
    await db.update(app.notes, note.id, { title: "second" }).wait({ tier: "global" });
    await db.delete(app.notes, note.id).wait({ tier: "global" });

    const rows = await db.all(
      app.$history.notes.includeDeleted().where({ id: note.id, title: "first" }).select("title"),
    );
    expectTypeOf(rows).toEqualTypeOf<{ id: string; title: string }[]>();
    expect(rows).toEqual([{ id: note.id, title: "first" }]);
  });

  it("includeDeleted retains old versions after content restoration", async () => {
    const note = await insertNote();
    const query = app.$history.notes.where({ id: note.id }).includeDeleted().orderBy("$acceptedAt");
    const versions = await db.all(query);
    await db.delete(app.notes, note.id).wait({ tier: "global" });
    await db
      .restore(app.notes, note.id, { title: "restored", done: false })
      .wait({ tier: "global" });
    const restored = await db.all(query);
    expect(restored).toHaveLength(2);
    expect(restored[0]).toEqual(versions[0]);
    expect(restored[1]).toMatchObject({ id: note.id, title: "restored" });
  });

  it("remote history reads return content and version metadata", async () => {
    const note = await insertNote();
    const rows = await db.all(app.$history.notes.where({ id: note.id }), { tier: "remote" });
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({ id: note.id, title: "first", done: false });
    expect(typeof rows[0]!.$transactionId).toBe("string");
    expect(rows[0]!.$acceptedAt).toBeInstanceOf(Date);
    expect(Number.isFinite(rows[0]!.$acceptedAt.getTime())).toBe(true);
  });

  it("rejects history reads without a bounded row id constraint", async () => {
    await expect(db.all(app.$history.notes)).rejects.toMatchObject({
      message:
        "Query: unsupported relation query: Reading history without an id equality or IN constraint is not supported in history queries yet",
      code: "query",
    });
  });

  it("rejects insertion into a history query at type and runtime boundaries", () => {
    expect(() => {
      // @ts-expect-error History is not a mutation target.
      db.insert(app.$history.notes, { title: "wrong", done: false });
    }).toThrow(new Error('History table "history.notes" is read-only'));
  });

  it("rejects deletion through a history query without deleting the current row", async () => {
    const note = await insertNote();
    expect(() => {
      // @ts-expect-error History is not a mutation target.
      db.delete(app.$history.notes, note.id);
    }).toThrow(new Error('History table "history.notes" is read-only'));
  });

  it("filters historical values", async () => {
    const note = await insertNote("before");
    await db.update(app.notes, note.id, { title: "after" }).wait({ tier: "global" });
    const rows = await db.all(
      app.$history.notes.where({ id: note.id, title: "before" }).select("title"),
    );
    expect(rows).toEqual([{ id: note.id, title: "before" }]);
  });

  it("combines chained application predicates with an exact id", async () => {
    const note = await insertNote("a");
    await db.update(app.notes, note.id, { title: "b" }).wait({ tier: "global" });
    await db.update(app.notes, note.id, { title: "c" }).wait({ tier: "global" });
    const query = app.$history.notes
      .where({ id: note.id })
      .where({ title: { in: ["b", "c"], ne: "c" }, done: false })
      .select("title");
    expect(await db.all(query)).toEqual([{ id: note.id, title: "b" }]);
  });

  it("orders and paginates by an unselected application column", async () => {
    const note = await insertNote("b");
    await db.update(app.notes, note.id, { title: "c" }).wait({ tier: "global" });
    await db.update(app.notes, note.id, { title: "a" }).wait({ tier: "global" });
    const query = app.$history.notes.where({ id: note.id });
    const byAcceptance = await db.all(query.orderBy("$acceptedAt"));
    expect(
      await db.all(query.orderBy("title").offset(1).limit(1).select("$transactionId")),
    ).toEqual([{ id: note.id, $transactionId: byAcceptance[0]!.$transactionId }]);
    expect(
      (await db.all(query.orderBy("title", "desc").select("title"))).map((row) => row.title),
    ).toEqual(["c", "b", "a"]);
  });

  it("keeps matching old versions when a new version fails the subscription filter", async () => {
    const note = await insertNote("keep");
    const query = app.$history.notes.where({ id: note.id, title: "keep" }).select("title");
    const latest = watch(query);
    const expected = { id: note.id, title: "keep" };
    await expect.poll(latest).toEqual([expected]);
    await db.update(app.notes, note.id, { title: "other" }).wait({ tier: "global" });
    expect(await db.all(query)).toEqual([expected]);
    await db.update(app.notes, note.id, { title: "keep" }).wait({ tier: "global" });
    await expect.poll(latest).toEqual([expected, expected]);
  });

  it("updates an application-ordered subscription window", async () => {
    const note = await insertNote("z");
    const latest = watch(
      app.$history.notes.where({ id: note.id }).orderBy("title").limit(1).select("title"),
    );
    await expect.poll(latest).toEqual([{ id: note.id, title: "z" }]);
    await db.update(app.notes, note.id, { title: "a" }).wait({ tier: "global" });
    await expect.poll(latest).toEqual([{ id: note.id, title: "a" }]);
  });

  it.each(["eq", "ne", "gt", "gte", "lt", "lte", "in", "notIn"] as const)(
    "applies millisecond semantics to $acceptedAt %s predicates",
    async (op) => {
      const note = await insertNote("first");
      await db.update(app.notes, note.id, { title: "second" }).wait({ tier: "global" });
      const query = app.$history.notes.where({ id: note.id }).orderBy("$acceptedAt");
      const rows = await db.all(query);
      const date = rows[0]!.$acceptedAt;
      const matches = (value: Date) => {
        const left = value.getTime(),
          right = date.getTime();
        switch (op) {
          case "eq":
          case "in":
            return left === right;
          case "ne":
          case "notIn":
            return left !== right;
          case "gt":
            return left > right;
          case "gte":
            return left >= right;
          case "lt":
            return left < right;
          case "lte":
            return left <= right;
        }
      };
      const predicate = op === "in" || op === "notIn" ? { [op]: [date] } : { [op]: date };
      const filtered = query.where({ $acceptedAt: predicate });
      const expected = rows.filter((row) => matches(row.$acceptedAt));
      expect(await db.all(filtered)).toEqual(expected);
      await expect.poll(watch(filtered)).toEqual(expected);
    },
  );

  it("handles $acceptedAt dates outside the HLC range", async () => {
    const note = await insertNote();
    const query = app.$history.notes.where({ id: note.id });
    const rows = await db.all(query);
    expect(await db.all(query.where({ $acceptedAt: { gt: new Date(-1) } }))).toEqual(rows);
    expect(await db.all(query.where({ $acceptedAt: { lt: new Date(-1) } }))).toEqual([]);
    expect(await db.all(query.where({ $acceptedAt: { lt: new Date(8e15) } }))).toEqual(rows);
    expect(await db.all(query.where({ $acceptedAt: { gte: new Date(8e15) } }))).toEqual([]);
  });

  it("rejects application filters without a bounded id constraint", async () => {
    await expect(db.all(app.$history.notes.where({ title: "first" }))).rejects.toMatchObject({
      message:
        "Query: unsupported relation query: Reading history without an id equality or IN constraint is not supported in history queries yet",
      code: "query",
    });
  });

  it("rejects user-provided transaction ID predicates", async () => {
    const note = await insertNote();
    const query = app.$history.notes.where({
      id: note.id,
      // @ts-expect-error Transaction IDs can be selected but not supplied as predicates.
      $transactionId: "0123456789abcdef0123456789abcdef",
    });
    await expect(db.all(query)).rejects.toThrow(
      new Error("History $transactionId predicates do not accept user-provided values"),
    );
  });

  it("projects each version's own creation and update timestamps as Dates", async () => {
    const note = await insertNote();
    const query = app.$history.notes
      .where({ id: note.id })
      .orderBy("$acceptedAt")
      .select("title", "$createdAt", "$updatedAt");
    const first = (await db.all(query))[0]!;
    expectTypeOf(first.$createdAt).toEqualTypeOf<Date>();
    expectTypeOf(first.$updatedAt).toEqualTypeOf<Date>();
    expect(first.$createdAt).toBeInstanceOf(Date);
    expect(first.$updatedAt).toBeInstanceOf(Date);
    const updatedAt = first.$updatedAt.getTime() + 1000;
    await db
      .update(app.notes, note.id, { title: "second" }, { updatedAt })
      .wait({ tier: "global" });
    expect(await db.all(query)).toEqual([
      first,
      {
        id: note.id,
        title: "second",
        $createdAt: first.$createdAt,
        $updatedAt: new Date(updatedAt),
      },
    ]);
  });

  it("filters and orders versions by their authored timestamps", async () => {
    const note = await insertNote();
    const query = app.$history.notes
      .where({ id: note.id })
      .select("title", "$createdAt", "$updatedAt");
    const first = (await db.all(query))[0]!;
    const earlier = first.$updatedAt.getTime() - 1000;
    await db
      .update(app.notes, note.id, { title: "backdated" }, { updatedAt: earlier })
      .wait({ tier: "global" });
    expect((await db.all(query.orderBy("$updatedAt"))).map((row) => row.title)).toEqual([
      "backdated",
      "first",
    ]);
    expect(
      await db.all(
        query.where({ $createdAt: first.$createdAt, $updatedAt: { gte: first.$updatedAt } }),
      ),
    ).toEqual([first]);
  });

  it("subscribes to projected historical timestamps without overwriting older versions", async () => {
    const note = await insertNote();
    const query = app.$history.notes
      .where({ id: note.id })
      .orderBy("$updatedAt")
      .select("$createdAt", "$updatedAt");
    const first = (await db.all(query))[0]!;
    const latest = watch(
      query.where({ $createdAt: first.$createdAt, $updatedAt: { gte: first.$updatedAt } }),
    );
    await expect.poll(latest).toEqual([first]);
    const updatedAt = first.$updatedAt.getTime() + 1000;
    await db
      .update(app.notes, note.id, { title: "second" }, { updatedAt })
      .wait({ tier: "global" });
    await expect
      .poll(latest)
      .toEqual([
        first,
        { id: note.id, $createdAt: first.$createdAt, $updatedAt: new Date(updatedAt) },
      ]);
  });

  it("projects all provenance columns with the same values and types as ordinary rows", async () => {
    const note = await insertNote();
    const current = (
      await db.all(app.notes.where({ id: note.id }).select(...PROVENANCE_MAGIC_COLUMNS))
    )[0]!;
    const query = app.$history.notes.where({ id: note.id }).select(...PROVENANCE_MAGIC_COLUMNS);
    const rows = await db.all(query);
    expectTypeOf(rows[0]!.$createdBy).toEqualTypeOf<RowAuthor>();
    expectTypeOf(rows[0]!.$updatedBy).toEqualTypeOf<RowAuthor>();
    expectTypeOf(rows[0]!["$createdBy.identity"]).toEqualTypeOf<RowAuthor["identity"]>();
    expectTypeOf(rows[0]!["$updatedBy.identity.subject"]).toEqualTypeOf<string>();
    expect(rows).toEqual([current]);
    await expect.poll(watch(query)).toEqual([current]);
  });

  it.each(
    PROVENANCE_MAGIC_COLUMNS.filter((column) => column !== "$createdAt" && column !== "$updatedAt"),
  )("filters historical provenance by %s", async (column) => {
    const note = await insertNote();
    const query = app.$history.notes.where({ id: note.id }).select(column);
    const rows = await db.all(query);
    const value = rows[0]![column];
    expect(value).toBeDefined();
    expect(await db.all(query.where({ [column]: { eq: value } }))).toEqual(rows);
    expect(await db.all(query.where({ [column]: { ne: value } }))).toEqual([]);
  });

  it("orders history by author subfields with a version tie-breaker", async () => {
    const note = await insertNote();
    await db.update(app.notes, note.id, { title: "second" }).wait({ tier: "global" });
    const query = app.$history.notes
      .where({ id: note.id })
      .select("title", "$updatedBy.identity.subject")
      .orderBy("$updatedBy.identity.subject")
      .orderBy("$acceptedAt", "desc");
    const rows = await db.all(query);
    expect(rows.map((row) => row.title)).toEqual(["second", "first"]);
    await expect.poll(watch(query)).toEqual(rows);
  });

  it("keeps provenance opt-in when selecting a wildcard", async () => {
    const note = await insertNote();
    const query = app.$history.notes.where({ id: note.id });
    const plain = (await db.all(query.select("*")))[0]!;
    for (const column of PROVENANCE_MAGIC_COLUMNS) expect(plain).not.toHaveProperty(column);
    const selected = (await db.all(query.select("*", "$createdBy")))[0]!;
    expectTypeOf(selected.$createdBy).toEqualTypeOf<RowAuthor>();
    const { $createdBy, ...content } = selected;
    expect($createdBy).toBeDefined();
    expect(content).toEqual(plain);
  });

  it("rejects relationship includes on history queries", async () => {
    const note = await insertNote();
    // @ts-expect-error History joins/includes are deferred.
    const query = app.$history.notes.where({ id: note.id }).include({ owner: true });
    await expect(db.all(query)).rejects.toThrow(
      new Error('Unknown relation "owner" on table "history.notes"'),
    );
  });
});

describe("TS Query API history schema migrations", () => {
  const schemaV1 = {
    notes: s.table({ title: s.string(), legacy: s.string() }, {}),
  };
  const schemaV2 = {
    notes: s.table({ heading: s.string(), category: s.string() }, {}),
  };
  const v1 = s.defineApp(schemaV1);
  const v2 = s.defineApp(schemaV2);
  const permissionsV1 = s.definePermissions(v1, ({ policy }) => {
    policy.notes.allowRead.always();
    policy.notes.allowInsert.always();
    policy.notes.allowUpdate.always();
  });
  const permissionsV2 = s.definePermissions(v2, ({ policy }) => {
    policy.notes.allowRead.always();
    policy.notes.allowInsert.always();
    policy.notes.allowUpdate.always();
  });
  let db: Db;
  let writer: Db;
  let server: Awaited<ReturnType<typeof startLocalJazzServer>>;
  let id: string;
  let stop: (() => void) | undefined;

  beforeEach(async () => {
    server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    const deployment = {
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
    };
    const deployed = await deploy({ ...deployment, schema: v1, permissions: permissionsV1 });
    await deploy({
      ...deployment,
      schema: v2,
      permissions: permissionsV2,
      migration: m.defineMigration({
        fromHash: deployed.schema.hash,
        toHash: await computeSchemaHash(v2.wasmSchema),
        from: schemaV1,
        to: schemaV2,
        migrate: {
          notes: {
            heading: m.renameFrom("title"),
            category: m.add.string({ default: "uncategorized" }),
            legacy: m.drop.string({ backwardsDefault: "" }),
          },
        },
      }),
    });
    writer = await createDb({
      ...(await localAccountConfig(server.appId, server.url)),
      driver: { type: "persistent" },
    });
    db = await createDb({
      ...(await localAccountConfig(server.appId, server.url)),
      driver: { type: "persistent" },
    });
    // Keep ordinary current-row coverage open so this partial reader retains
    // both old-schema versions through normal sync before history is queried.
    let current: { id: string; heading: string; category: string }[] | undefined;
    let failure: Error | undefined;
    stop = db.subscribe(v2.notes, {
      onUpdate: (rows) => {
        current = rows;
      },
      onError: (error) => {
        failure = error;
      },
    });
    const observed = () => {
      if (failure) throw failure;
      return current;
    };
    await expect.poll(observed).toEqual([]);
    const note = await writer
      .insert(v1.notes, { title: "first", legacy: "old" })
      .wait({ tier: "global" });
    id = note.id;
    await expect.poll(observed).toEqual([{ id, heading: "first", category: "uncategorized" }]);
    await writer.update(v1.notes, id, { title: "second" }).wait({ tier: "global" });
    await expect.poll(observed).toEqual([{ id, heading: "second", category: "uncategorized" }]);
    stop();
    stop = undefined;
  });

  afterEach(async () => {
    stop?.();
    stop = undefined;
    try {
      await db?.shutdown();
      await writer?.shutdown();
    } finally {
      await server?.stop();
    }
  });

  it("reads and filters older versions under a renamed column", async () => {
    const query = v2.$history.notes.where({ id }).orderBy("$acceptedAt").select("heading");
    const rows = await db.all(query);
    expectTypeOf(rows).toEqualTypeOf<{ id: string; heading: string }[]>();
    expect(rows).toEqual([
      { id, heading: "first" },
      { id, heading: "second" },
    ]);
    expect(await db.all(query.where({ heading: "first" }))).toEqual([{ id, heading: "first" }]);
  });

  it("uses the added column's lens default in older versions", async () => {
    expect(await db.all(v2.$history.notes.where({ id }).select("category"))).toEqual([
      { id, category: "uncategorized" },
      { id, category: "uncategorized" },
    ]);
  });

  it("omits dropped columns from historical wildcard results", async () => {
    const rows = await db.all(v2.$history.notes.where({ id }).orderBy("$acceptedAt").select("*"));
    expect(rows).toHaveLength(2);
    expect(rows.map(({ $transactionId, $acceptedAt, ...content }) => content)).toEqual([
      { id, heading: "first", category: "uncategorized" },
      { id, heading: "second", category: "uncategorized" },
    ]);
  });

  it("subscribes to old and new versions using the reader's schema", async () => {
    const query = v2.$history.notes
      .where({ id })
      .orderBy("$acceptedAt")
      .select("heading", "category");
    let latest: { id: string; heading: string; category: string }[] | undefined;
    let failure: Error | undefined;
    stop = db.subscribe(query, {
      onUpdate: (rows) => {
        latest = rows;
      },
      onError: (error) => {
        failure = error;
      },
    });
    const observed = () => {
      if (failure) throw failure;
      return latest;
    };
    const old = [
      { id, heading: "first", category: "uncategorized" },
      { id, heading: "second", category: "uncategorized" },
    ];
    await expect.poll(observed).toEqual(old);
    await db.update(v2.notes, id, { heading: "third", category: "new" }).wait({ tier: "global" });
    await expect.poll(observed).toEqual([...old, { id, heading: "third", category: "new" }]);
    expect(await db.all(query)).toEqual(latest);
  });
});

describe("TS Query API upstream history", () => {
  let writer: Db;
  let reader: Db;
  let server: Awaited<ReturnType<typeof startLocalJazzServer>>;
  let id: string;
  let stop: (() => void) | undefined;

  beforeEach(async () => {
    server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    writer = await createDb({
      ...(await localAccountConfig(server.appId, server.url)),
      driver: { type: "persistent" },
    });
    const row = await writer
      .insert(app.notes, { title: "first", done: false })
      .wait({ tier: "global" });
    id = row.id;
    await writer.update(app.notes, id, { title: "second" }).wait({ tier: "global" });
    reader = await createDb({
      ...(await localAccountConfig(server.appId, server.url)),
      driver: { type: "persistent" },
    });
  });

  afterEach(async () => {
    stop?.();
    stop = undefined;
    try {
      await reader?.shutdown();
      await writer?.shutdown();
    } finally {
      await server?.stop();
    }
  });

  function query() {
    return app.$history.notes.where({ id }).orderBy("$acceptedAt").select("title");
  }

  it("remote reads fetch missing versions on a fresh client", async () => {
    expect(await reader.all(query(), { tier: "local-first", firstLoadRemoteWaitMs: 0 })).toEqual(
      [],
    );
    expect(await reader.all(query(), { tier: "remote" })).toEqual([
      { id, title: "first" },
      { id, title: "second" },
    ]);
    expect(await reader.all(query(), { tier: "local-first", firstLoadRemoteWaitMs: 0 })).toEqual([
      { id, title: "first" },
      { id, title: "second" },
    ]);
  });

  it("remote reads settle when the requested row has no history", async () => {
    expect(
      await reader.all(app.$history.notes.where({ id: crypto.randomUUID() }), { tier: "remote" }),
    ).toEqual([]);
  });

  it("remote reads apply ordering and pagination across fetched versions", async () => {
    expect(await reader.all(query().offset(1).limit(1), { tier: "remote" })).toEqual([
      { id, title: "second" },
    ]);
  });

  it("local-first reads can wait for missing upstream history", async () => {
    expect(
      await reader.all(query(), { tier: "local-first", firstLoadRemoteWaitMs: 5_000 }),
    ).toEqual([
      { id, title: "first" },
      { id, title: "second" },
    ]);
  });

  it("remote filters exclude other versions already cached by the client", async () => {
    // Fetch all history versions into the local DB
    await reader.all(query(), { tier: "remote" });
    // Fetch only first history version form remote DB
    const filtered = app.$history.notes.where({ id, title: "first" }).select("title");
    expect(await reader.all(filtered, { tier: "remote" })).toEqual([{ id, title: "first" }]);
  });

  it("remote reads recover versions accepted while the client was disconnected", async () => {
    await reader.all(query(), { tier: "remote" });
    await reader.disconnect();
    await writer.update(app.notes, id, { title: "third" }).wait({ tier: "global" });
    await writer.update(app.notes, id, { title: "fourth" }).wait({ tier: "global" });
    await reader.reconnect();
    expect(await reader.all(query(), { tier: "remote" })).toEqual([
      { id, title: "first" },
      { id, title: "second" },
      { id, title: "third" },
      { id, title: "fourth" },
    ]);
  });

  it("local-first subscriptions fetch missing versions after their local result", async () => {
    let latest: { id: string; title: string }[] | undefined;
    let failure: Error | undefined;
    const snapshots: { id: string; title: string }[][] = [];
    stop = reader.subscribe(
      query(),
      {
        onUpdate: (rows) => {
          latest = rows;
          snapshots.push(rows);
        },
        onError: (error) => {
          failure = error;
        },
      },
      { tier: "local-first" },
    );
    await expect
      .poll(() => {
        if (failure) throw failure;
        return latest;
      })
      .toEqual([
        { id, title: "first" },
        { id, title: "second" },
      ]);
    expect(snapshots[0]).toEqual([]);
  });

  it("remote subscriptions retract history when current permission is revoked", async () => {
    let latest: { id: string; title: string }[] | undefined;
    let failure: Error | undefined;
    stop = reader.subscribe(
      query(),
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
    const observed = () => {
      if (failure) throw failure;
      return latest;
    };
    await expect.poll(observed).toEqual([
      { id, title: "first" },
      { id, title: "second" },
    ]);
    await writer.update(app.notes, id, { done: true }).wait({ tier: "global" });
    await expect.poll(observed).toEqual([]);
    expect(await reader.all(query(), { tier: "remote" })).toEqual([]);
  });
});
