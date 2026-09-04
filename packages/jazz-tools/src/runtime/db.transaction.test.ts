import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import { ExclusiveWriteHandle } from "./client.js";
import { createDb, type Db } from "./db.js";

const todoSchema = {
  todos: s.table({
    title: s.string(),
    done: s.boolean(),
  }),
};
type TodoSchema = s.Schema<typeof todoSchema>;
const app: s.App<TodoSchema> = s.defineApp(todoSchema);

const otherTodoSchema = {
  todos: s.table({
    title: s.string(),
    done: s.boolean(),
    note: s.string().default(""),
  }),
};
type OtherTodoSchema = s.Schema<typeof otherTodoSchema>;
const otherApp: s.App<OtherTodoSchema> = s.defineApp(otherTodoSchema);

const taggedTodoSchema = {
  tagged_todos: s.table({
    title: s.string(),
    tags: s.array(s.string()).default([]),
  }),
};
type TaggedTodoSchema = s.Schema<typeof taggedTodoSchema>;
const taggedApp: s.App<TaggedTodoSchema> = s.defineApp(taggedTodoSchema);

const defaultsTodoSchema = {
  defaults_todos: s.table({
    title: s.string().default("default title"),
    done: s.boolean().default(false),
  }),
};
type DefaultsTodoSchema = s.Schema<typeof defaultsTodoSchema>;
const defaultsApp: s.App<DefaultsTodoSchema> = s.defineApp(defaultsTodoSchema);

let db: Db;

beforeEach(async () => {
  db = await createDb({
    appId: `db-transaction-test`,
    driver: { type: "memory" },
    serverUrl: "ws://example.invalid",
    adminSecret: "db-transaction-test-admin",
  });
});

afterEach(async () => {
  await db.shutdown();
});

function allTodos() {
  return db.all(app.todos.where({}), { tier: "local" });
}

describe("Db exclusive transaction initialization", () => {
  it("rejects beginning an exclusive transaction before the JazzClient exists", () => {
    expect(() => db.beginExclusiveTransaction()).toThrow(
      "Cannot begin an exclusive transaction before the JazzClient has been created. Run a query or mutation first.",
    );
  });
});

describe("Db transactions", () => {
  it("anchors an exclusive read snapshot at the public begin call", async () => {
    const { value: beforeBegin } = db.insert(app.todos, {
      title: "visible at begin",
      done: false,
    });
    const tx = db.beginExclusiveTransaction();
    db.insert(app.todos, { title: "committed after begin", done: false });

    await expect(tx.all(app.todos.where({}), { tier: "local" })).resolves.toEqual([beforeBegin]);
    await tx.rollback();
  });

  it("rolls back an exclusive callback transaction when commit is called inside the callback", async () => {
    await allTodos();
    await expect(
      db.exclusiveTransaction(async (tx) => {
        tx.insert(app.todos, { title: "Rejected callback transaction", done: false });
        // @ts-expect-error - commit is not available on TransactionScope
        return tx.commit();
      }),
    ).rejects.toEqual(new TypeError("tx.commit is not a function"));

    await expect(allTodos()).resolves.toEqual([]);
  });

  it("rolls back an exclusive callback transaction when rollback is called inside the callback", async () => {
    await allTodos();
    await expect(
      db.exclusiveTransaction(async (tx) => {
        tx.insert(app.todos, { title: "Rejected callback transaction", done: false });
        // @ts-expect-error - rollback is not available on TransactionScope
        return tx.rollback();
      }),
    ).rejects.toEqual(new TypeError("tx.rollback is not a function"));

    await expect(allTodos()).resolves.toEqual([]);
  });

  it("uses mergeable transactions by default", () => {
    const tx = db.beginTransaction();
    tx.insert(app.todos, { title: "Default transaction", done: false });

    expect(tx.kind).toBe("mergeable");
  });

  it("uses mergeable callback transactions by default", async () => {
    const result = await db.transaction((tx) => {
      expect(tx.kind).toBe("mergeable");
      tx.insert(app.todos, { title: "Rejected callback transaction", done: false });
      return tx.kind;
    });

    expect(result.value).toBe("mergeable");
  });

  it("reads its own staged writes inside a mergeable callback transaction", async () => {
    const result = await db.transaction(async (tx) => {
      const inserted = tx.insert(app.todos, { title: "staged", done: false });

      await expect(allTodos()).resolves.toEqual([]);
      await expect(
        tx.one(app.todos.where({ id: inserted.id }), { tier: "local" }),
      ).resolves.toEqual(inserted);

      tx.update(app.todos, inserted.id, { done: true });
      await expect(
        tx.one(app.todos.where({ id: inserted.id }), { tier: "local" }),
      ).resolves.toEqual({
        ...inserted,
        done: true,
      });

      return inserted.id;
    });

    await result.wait({ tier: "local" });
    await expect(db.one(app.todos.where({ id: result.value }), { tier: "local" })).resolves.toEqual(
      {
        id: result.value,
        title: "staged",
        done: true,
      },
    );
  });

  it("reads insert update delete effects inside a mergeable callback transaction", async () => {
    const existing = db.insert(app.todos, { title: "committed", done: false }).value;

    const result = await db.transaction(async (tx) => {
      const inserted = tx.insert(app.todos, { title: "inserted", done: false });
      tx.update(app.todos, existing.id, { done: true });
      tx.delete(app.todos, inserted.id);

      await expect(tx.all(app.todos.where({}), { tier: "local" })).resolves.toEqual([
        { id: existing.id, title: "committed", done: true },
      ]);

      return existing.id;
    });

    await result.wait({ tier: "local" });
    await expect(db.all(app.todos.where({}), { tier: "local" })).resolves.toEqual([
      { id: existing.id, title: "committed", done: true },
    ]);
  });

  it("reads a restored row inside a mergeable callback transaction", async () => {
    const deleted = await db
      .insert(app.todos, { title: "deleted", done: false })
      .wait({ tier: "local" });
    await db.delete(app.todos, deleted.id).wait({ tier: "local" });

    const result = await db.transaction(async (tx) => {
      const restored = tx.restore(app.todos, deleted.id, { title: "restored", done: true });

      await expect(tx.one(app.todos.where({ id: deleted.id }), { tier: "local" })).resolves.toEqual(
        restored,
      );

      return restored;
    });

    await result.wait({ tier: "local" });
    await expect(db.one(app.todos.where({ id: deleted.id }), { tier: "local" })).resolves.toEqual(
      result.value,
    );
  });

  it("applies defaults to empty inserts and restores inside a mergeable callback transaction", async () => {
    const inserted = await db.insert(defaultsApp.defaults_todos, {}).wait({ tier: "local" });
    await db.delete(defaultsApp.defaults_todos, inserted.id).wait({ tier: "local" });

    const result = await db.transaction(async (tx) => {
      const restored = tx.restore(defaultsApp.defaults_todos, inserted.id, {});

      await expect(
        tx.one(defaultsApp.defaults_todos.where({ id: inserted.id }), { tier: "local" }),
      ).resolves.toEqual(restored);

      return restored;
    });

    await result.wait({ tier: "local" });
    await expect(
      db.one(defaultsApp.defaults_todos.where({ id: inserted.id }), { tier: "local" }),
    ).resolves.toEqual(inserted);
  });

  it("orders in-transaction reads by explicit orderBy and by implicit row id when omitted", async () => {
    const result = await db.transaction(async (tx) => {
      tx.insert(app.todos, { title: "b", done: false });
      tx.insert(app.todos, { title: "a", done: false });
      tx.insert(app.todos, { title: "c", done: false });

      // The deleted TS read overlay used to sort transaction reads itself. These
      // assertions pin the observable behavior: explicit title ordering here,
      // and SPEC 6.4.1's implicit ascending row_uuid ordering below.
      await expect(
        tx.all(app.todos.where({}).orderBy("title", "asc"), { tier: "local" }),
      ).resolves.toMatchObject([{ title: "a" }, { title: "b" }, { title: "c" }]);

      return tx.all(app.todos.where({}), { tier: "local" });
    });

    await result.wait({ tier: "local" });
    await expect(db.all(app.todos.where({}), { tier: "local" })).resolves.toEqual(result.value);
  });

  it("applies non-eq predicates and limit offset inside a mergeable transaction", async () => {
    const result = await db.transaction(async (tx) => {
      tx.insert(app.todos, { title: "a", done: false });
      tx.insert(app.todos, { title: "b", done: false });
      tx.insert(app.todos, { title: "c", done: false });
      tx.insert(app.todos, { title: "d", done: false });

      await expect(
        tx.all(app.todos.where({ title: { gt: "b" } } as never).orderBy("title", "asc"), {
          tier: "local",
        }),
      ).resolves.toMatchObject([{ title: "c" }, { title: "d" }]);

      return tx.all(app.todos.where({}).orderBy("title", "asc").offset(1).limit(2), {
        tier: "local",
      });
    });

    expect(result.value).toMatchObject([{ title: "b" }, { title: "c" }]);
    await result.wait({ tier: "local" });
  });

  it("applies contains predicates inside a mergeable transaction", async () => {
    const taggedDb = await createDb({
      appId: `db-transaction-tagged-test`,
      driver: { type: "memory" },
      serverUrl: "ws://example.invalid",
      adminSecret: "db-transaction-tagged-test-admin",
    });

    try {
      const result = await taggedDb.transaction(async (tx) => {
        tx.insert(taggedApp.tagged_todos, { title: "work", tags: ["urgent", "team"] });
        tx.insert(taggedApp.tagged_todos, { title: "home", tags: ["personal"] });

        return tx.all(taggedApp.tagged_todos.where({ tags: { contains: "urgent" } } as never), {
          tier: "local",
        });
      });

      expect(result.value).toMatchObject([{ title: "work", tags: ["urgent", "team"] }]);
      await result.wait({ tier: "local" });
    } finally {
      await taggedDb.shutdown();
    }
  });

  it("types exclusive transaction waits without durability options", async () => {
    if (false) {
      const result = await db.exclusiveTransaction((tx) => tx.kind);
      void result.wait();
      // @ts-expect-error - exclusive transactions are confirmed by the global authority.
      void result.wait({ tier: "global" });

      const tx = db.beginExclusiveTransaction();
      const committed = await tx.commit();
      void committed.wait();
      // @ts-expect-error - exclusive transactions are confirmed by the global authority.
      void committed.wait({ tier: "global" });
    }
  });

  it("commits an empty exclusive transaction synchronously at begin", async () => {
    await allTodos();
    const tx = db.beginExclusiveTransaction();
    const committed = tx.commit();
    expect(committed).toBeInstanceOf(ExclusiveWriteHandle);
  });

  it("rejects exclusive transaction operations after commit", async () => {
    await allTodos();
    const tx = db.beginExclusiveTransaction();
    tx.insert(app.todos, { title: "Committed transaction", done: false });
    const openTransactionId = tx.openTransactionId();

    await tx.commit();

    const coreError = `open transaction ${openTransactionId} is already committed`;
    expect(() => tx.commit()).toThrow(`Write error: ${coreError}`);
    expect(() => tx.rollback()).toThrow(`Write error: ${coreError}`);
    expect(() => tx.insert(app.todos, { title: "Nope", done: false })).toThrow(
      `Insert failed: WriteError("${coreError}")`,
    );
    await expect(tx.all(app.todos.where({}))).rejects.toThrow(
      `Query setup failed: Write error: ${coreError}`,
    );
  });

  it("rejects exclusive transaction operations after rollback", async () => {
    await allTodos();
    const tx = db.beginExclusiveTransaction();
    tx.insert(app.todos, { title: "Rolled-back transaction", done: false });
    const openTransactionId = tx.openTransactionId();

    await tx.rollback();

    const coreError = `open transaction ${openTransactionId} has already been completed or was never opened`;
    expect(() => tx.commit()).toThrow(`Commit transaction failed: Write error: ${coreError}`);
    expect(() => tx.rollback()).toThrow(`Rollback transaction failed: Write error: ${coreError}`);
    expect(() => tx.insert(app.todos, { title: "Nope", done: false })).toThrow(
      `Insert failed: WriteError("${coreError}")`,
    );
    await expect(tx.all(app.todos.where({}))).rejects.toThrow(
      `Query setup failed: Write error: ${coreError}`,
    );
  });

  it("rejects exclusive writes from a second schema view", async () => {
    await allTodos();
    const tx = db.beginExclusiveTransaction();
    tx.insert(app.todos, { title: "Primary client", done: false });
    expect(() =>
      tx.insert(otherApp.todos, { title: "Second schema", done: false, note: "kept" }),
    ).toThrow(
      "Db is already initialized with a different schema. Create a separate Db for each schema/app.",
    );
    await tx.rollback();
  });
});

describe("Db mergeable transactions", () => {
  it("requires a table operation before committing on a fresh Db", () => {
    const tx = db.beginTransaction();

    expect(() => tx.commit()).toThrow(
      "DbTransaction.commit() requires at least one table operation first",
    );
  });

  it("rejects committing an empty mergeable transaction after Db initialization", async () => {
    await allTodos();
    const tx = db.beginTransaction();

    expect(() => tx.commit()).toThrow(
      "empty mergeable transaction has no committed unit; roll it back instead",
    );
  });

  it("rejects mergeable transaction operations after commit", async () => {
    const tx = db.beginTransaction();
    tx.insert(app.todos, { title: "Committed transaction", done: false });
    const openTransactionId = tx.openTransactionId();

    await tx.commit();

    const coreError = `open transaction ${openTransactionId} is already committed`;
    expect(() => tx.commit()).toThrow(`Write error: ${coreError}`);
    expect(() => tx.rollback()).toThrow(`Write error: ${coreError}`);
    expect(() => tx.insert(app.todos, { title: "Nope", done: false })).toThrow(
      `Insert failed: WriteError("${coreError}")`,
    );
    await expect(tx.all(app.todos.where({}))).rejects.toThrow(
      `Query setup failed: Write error: ${coreError}`,
    );
  });

  it("rejects mergeable transaction operations after rollback", async () => {
    const tx = db.beginTransaction();
    tx.insert(app.todos, { title: "Rolled-back transaction", done: false });
    const openTransactionId = tx.openTransactionId();

    await tx.rollback();

    const coreError = `open transaction ${openTransactionId} has already been completed or was never opened`;
    expect(() => tx.commit()).toThrow(`Commit transaction failed: Write error: ${coreError}`);
    expect(() => tx.rollback()).toThrow(`Rollback transaction failed: Write error: ${coreError}`);
    expect(() => tx.insert(app.todos, { title: "Nope", done: false })).toThrow(
      `Insert failed: WriteError("${coreError}")`,
    );
    await expect(tx.all(app.todos.where({}))).rejects.toThrow(
      `Query setup failed: Write error: ${coreError}`,
    );
  });

  it("rolls back a callback mergeable transaction when the callback throws after a write", async () => {
    const error = new Error("callback failed");

    await expect(
      db.transaction((tx) => {
        tx.insert(app.todos, { title: "Thrown callback transaction", done: false });
        throw error;
      }),
    ).rejects.toThrow(error);

    await expect(allTodos()).resolves.toEqual([]);
  });

  it("stages session-scoped mergeable transaction writes with core identity", async () => {
    const sessionDb = await createDb({
      appId: `db-transaction-session-test`,
      driver: { type: "memory" },
      serverUrl: "ws://example.invalid",
    });

    try {
      const tx = sessionDb.beginTransaction();
      tx.insert(app.todos, { title: "Session-scoped transaction", done: false });
      await tx.commit();
      await expect(sessionDb.all(app.todos.where({}), { tier: "local" })).resolves.toEqual([
        { id: expect.any(String), title: "Session-scoped transaction", done: false },
      ]);
    } finally {
      await sessionDb.shutdown();
    }
  });

  it("rejects mergeable writes from a second schema view", async () => {
    const tx = db.beginTransaction();
    tx.insert(app.todos, { title: "Primary client", done: false });
    expect(() =>
      tx.insert(otherApp.todos, { title: "Second schema", done: false, note: "kept" }),
    ).toThrow(
      "Db is already initialized with a different schema. Create a separate Db for each schema/app.",
    );
    await tx.rollback();
  });

  it("keeps client permission advice unknown across uncommitted transaction rows", async () => {
    await expect(db.canInsert(app.todos, { title: "allowed", done: false })).resolves.toBe(
      "unknown",
    );

    const tx = db.beginTransaction();
    const staged = tx.insert(app.todos, { title: "staged dry-run", done: false });

    await expect(db.canUpdate(app.todos, staged.id, { done: true })).resolves.toBe("unknown");
    expect(tx.kind).toBe("mergeable");

    await tx.rollback();
    await expect(allTodos()).resolves.toEqual([]);
  });
});
