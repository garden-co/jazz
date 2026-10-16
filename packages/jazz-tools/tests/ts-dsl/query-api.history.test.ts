import type { TxId } from "../../src/runtime/client.js";
import { afterEach, beforeEach, describe, expect, expectTypeOf, it } from "vitest";
import { schema as s } from "../../src/index.js";
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

describe("TS Query API history (connected client)", () => {
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

  it("returns typed content and transaction metadata for an accepted version", async () => {
    const startedAt = Date.now();
    const note = await insertNote();
    const rows = await db.all(app.notes.history.where({ id: note.id }));
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({ id: note.id, title: "first", done: false });
    expectTypeOf(rows[0]!.$acceptedAt).toEqualTypeOf<Date>();
    expectTypeOf(rows[0]!.$transactionId).toEqualTypeOf<TxId>();
    expect(rows[0]!.$acceptedAt).toBeInstanceOf(Date);
    expect(rows[0]!.$acceptedAt.getTime()).toBeGreaterThanOrEqual(startedAt);
    expect(rows[0]!.$acceptedAt.getTime()).toBeLessThanOrEqual(Date.now());
    const latest = watch(app.notes.history.where({ id: note.id }).select("$acceptedAt"));
    await expect.poll(latest).toEqual([{ id: note.id, $acceptedAt: rows[0]!.$acceptedAt }]);
    expect(typeof rows[0]!.$transactionId).toBe("string");
  });

  it("projects selected fields while preserving the logical row id", async () => {
    const note = await insertNote();
    const rows = await db.all(app.notes.history.where({ id: note.id }).select("title"));
    expectTypeOf(rows).toEqualTypeOf<{ id: string; title: string }[]>();
    expect(rows).toEqual([{ id: note.id, title: "first" }]);
  });

  it("orders versions by acceptance sequence", async () => {
    const note = await insertNote();
    await db.update(app.notes, note.id, { title: "second" }).wait({ tier: "global" });
    const query = app.notes.history.where({ id: note.id });
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
    const page = app.notes.history
      .where({ id: note.id })
      .orderBy("$acceptedAt")
      .offset(1)
      .limit(1)
      .select("title");
    expect(await db.all(page)).toEqual([{ id: note.id, title: "second" }]);
  });

  it("keeps equal projections as separate versions in subscriptions", async () => {
    const note = await insertNote("same");
    const query = app.notes.history.where({ id: note.id }).select("title");
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
    const query = app.notes.history.where({ id: note.id });
    const latest = watch(query);
    await expect.poll(() => latest()?.length).toBe(2);
    await db.delete(app.notes, note.id).wait({ tier: "global" });
    await expect.poll(latest).toEqual([]);
    expect(await db.all(query)).toEqual([]);
  });

  it("restores old versions and adds the replacement content version", async () => {
    const note = await insertNote();
    const query = app.notes.history.where({ id: note.id }).orderBy("$acceptedAt");
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

  it("does not re-evaluate read policies for locally retained history", async () => {
    const note = await insertNote();
    const query = app.notes.history.where({ id: note.id }).orderBy("$acceptedAt");
    const latest = watch(query);
    await expect.poll(() => latest()?.length).toBe(1);
    // The deployed read policy denies done=true; writes remain allowed.
    await db.update(app.notes, note.id, { done: true }).wait({ tier: "global" });
    await expect.poll(() => latest()?.map((row) => row.done)).toEqual([false, true]);
    expect((await db.all(query)).map((row) => row.done)).toEqual([false, true]);
  });

  it("rejects remote history reads", async () => {
    const note = await insertNote();
    await expect(
      db.all(app.notes.history.where({ id: note.id }), { tier: "remote" }),
    ).rejects.toThrow(new Error("Remote history queries are not supported yet"));
  });

  it("rejects history reads without an exact row id", async () => {
    await expect(db.all(app.notes.history)).rejects.toMatchObject({
      message:
        "Query: unsupported relation query: Reading anything other than one exact id equality filter is not supported in history queries yet",
      code: "query",
    });
  });

  it("rejects insertion into a history query at type and runtime boundaries", () => {
    expect(() => {
      // @ts-expect-error History is not a mutation target.
      db.insert(app.notes.history, { title: "wrong", done: false });
    }).toThrow(new Error('History table "history.notes" is read-only'));
  });

  it("rejects deletion through a history query without deleting the current row", async () => {
    const note = await insertNote();
    expect(() => {
      // @ts-expect-error History is not a mutation target.
      db.delete(app.notes.history, note.id);
    }).toThrow(new Error('History table "history.notes" is read-only'));
  });

  it("rejects additional history predicates at type and runtime boundaries", async () => {
    const note = await insertNote();
    // @ts-expect-error Only exact row-id filtering is supported initially.
    const query = app.notes.history.where({ id: note.id, title: "first" });
    await expect(db.all(query)).rejects.toMatchObject({
      message:
        "Query: unsupported relation query: Reading anything other than one exact id equality filter is not supported in history queries yet",
      code: "query",
    });
  });

  it("rejects current-row provenance in history projections", async () => {
    const note = await insertNote();
    // @ts-expect-error Current-row provenance is not history metadata.
    const query = app.notes.history.where({ id: note.id }).select("$updatedAt");
    await expect(db.all(query)).rejects.toMatchObject({
      message:
        "Query: unsupported relation query: Reading current-row provenance columns is not supported in history queries yet",
      code: "query",
    });
  });

  it("rejects relationship includes on history queries", async () => {
    const note = await insertNote();
    // @ts-expect-error History joins/includes are deferred.
    const query = app.notes.history.where({ id: note.id }).include({ owner: true });
    await expect(db.all(query)).rejects.toThrow(
      new Error('Unknown relation "owner" on table "history.notes"'),
    );
  });
});
