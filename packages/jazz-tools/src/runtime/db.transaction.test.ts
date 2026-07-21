import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
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

describe("Db transactions", () => {
  it("rolls back an exclusive callback transaction when commit is called inside the callback", async () => {
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
    const result = db.transaction((tx) => {
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

  it("uses core ordering and default ordering for in-transaction reads", async () => {
    const result = await db.transaction(async (tx) => {
      tx.insert(app.todos, { title: "b", done: false });
      tx.insert(app.todos, { title: "a", done: false });
      tx.insert(app.todos, { title: "c", done: false });

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

  it("types exclusive transaction waits without durability options", () => {
    if (false) {
      const result = db.exclusiveTransaction((tx) => tx.kind);
      void result.wait();
      // @ts-expect-error - exclusive transactions are confirmed by the global authority.
      void result.wait({ tier: "global" });

      const tx = db.beginExclusiveTransaction();
      const committed = tx.commit();
      void committed.wait();
      // @ts-expect-error - exclusive transactions are confirmed by the global authority.
      void committed.wait({ tier: "global" });
    }
  });

  it("throws when committing a db transaction before any actions", () => {
    const tx = db.beginExclusiveTransaction();

    expect(() => tx.commit()).toThrow(
      "DbTransaction.commit() requires at least one table operation first",
    );
  });

  it("rejects exclusive transaction operations after commit", async () => {
    const tx = db.beginExclusiveTransaction();
    tx.insert(app.todos, { title: "Committed transaction", done: false });
    const transactionId = tx.transactionId();

    tx.commit();

    const coreError = `transaction ${transactionId} is already committed`;
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
    const tx = db.beginExclusiveTransaction();
    tx.insert(app.todos, { title: "Rolled-back transaction", done: false });
    const transactionId = tx.transactionId();

    tx.rollback();

    const coreError = `transaction ${transactionId} has already been completed or was never opened`;
    expect(() => tx.commit()).toThrow(`Commit transaction failed: Write error: ${coreError}`);
    expect(() => tx.rollback()).toThrow(`Rollback transaction failed: Write error: ${coreError}`);
    expect(() => tx.insert(app.todos, { title: "Nope", done: false })).toThrow(
      `Insert failed: WriteError("${coreError}")`,
    );
    await expect(tx.all(app.todos.where({}))).rejects.toThrow(
      `Query setup failed: Write error: ${coreError}`,
    );
  });

  it("rejects db exclusive transaction writes against a different client/schema", () => {
    const tx = db.beginExclusiveTransaction();
    tx.insert(app.todos, { title: "Primary client", done: false });

    expect(() =>
      tx.insert(otherApp.todos, { title: "Wrong client", done: false, note: "nope" }),
    ).toThrow(/cannot be used with table "todos" from a different schema\/client/);
  });
});

describe("Db mergeable transactions", () => {
  it("throws when committing a mergeable transaction before any actions", () => {
    const tx = db.beginTransaction();

    expect(() => tx.commit()).toThrow(
      "DbTransaction.commit() requires at least one table operation first",
    );
  });

  it("rejects mergeable transaction operations after commit", async () => {
    const tx = db.beginTransaction();
    tx.insert(app.todos, { title: "Committed transaction", done: false });
    const transactionId = tx.transactionId();

    tx.commit();

    const coreError = `transaction ${transactionId} is already committed`;
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
    const transactionId = tx.transactionId();

    tx.rollback();

    const coreError = `transaction ${transactionId} has already been completed or was never opened`;
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

    expect(() =>
      db.transaction((tx) => {
        tx.insert(app.todos, { title: "Thrown callback transaction", done: false });
        throw error;
      }),
    ).toThrow(error);

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
      tx.commit();
      await expect(sessionDb.all(app.todos.where({}), { tier: "local" })).resolves.toEqual([
        { id: expect.any(String), title: "Session-scoped transaction", done: false },
      ]);
    } finally {
      await sessionDb.shutdown();
    }
  });

  it("rejects db mergeable transaction writes against a different client/schema", () => {
    const tx = db.beginTransaction();
    tx.insert(app.todos, { title: "Primary client", done: false });

    expect(() =>
      tx.insert(otherApp.todos, { title: "Wrong client", done: false, note: "nope" }),
    ).toThrow(/cannot be used with table "todos" from a different schema\/client/);
  });

  it("keeps write-policy dry-runs independent from uncommitted transaction rows", async () => {
    expect(db.canInsert(app.todos, { title: "allowed", done: false })).toBe(true);

    const tx = db.beginTransaction();
    const staged = tx.insert(app.todos, { title: "staged dry-run", done: false });

    expect(db.canUpdate(app.todos, staged.id, { done: true })).toBe(false);
    expect(tx.kind).toBe("mergeable");

    tx.rollback();
    await expect(allTodos()).resolves.toEqual([]);
  });
});
