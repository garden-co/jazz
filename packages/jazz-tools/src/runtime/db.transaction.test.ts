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

let db: Db;

beforeEach(async () => {
  db = await createDb({
    appId: `db-transaction-test`,
    driver: { type: "memory" },
    serverUrl: "ws://example.invalid",
  });
});

afterEach(async () => {
  await db.shutdown();
});

function allTodos() {
  return db.all(app.todos.where({}), { tier: "local" });
}

describe("Db transactions", () => {
  it("cannot commit a callback transaction by calling commit()", async () => {
    await expect(
      db.transaction(async (tx) => {
        tx.insert(app.todos, { title: "Rejected callback transaction", done: false });
        // @ts-expect-error - commit is not available on TransactionScope
        return tx.commit();
      }),
    ).rejects.toEqual(new TypeError("tx.commit is not a function"));

    await expect(allTodos()).resolves.toEqual([]);
  });

  it("cannot roll back a callback transaction by calling rollback()", async () => {
    await expect(
      db.transaction(async (tx) => {
        tx.insert(app.todos, { title: "Rejected callback transaction", done: false });
        // @ts-expect-error - rollback is not available on TransactionScope
        return tx.rollback();
      }),
    ).rejects.toEqual(new TypeError("tx.rollback is not a function"));

    await expect(allTodos()).resolves.toEqual([]);
  });

  it("throws when committing a db transaction before any actions", () => {
    const tx = db.beginTransaction();

    expect(() => tx.commit()).toThrow(
      "DbTransaction.commit() requires at least one table operation first",
    );
  });

  it("rejects transaction operations after commit", async () => {
    const tx = db.beginTransaction();
    tx.insert(app.todos, { title: "Committed transaction", done: false });
    const batchId = tx.batchId();

    tx.commit();

    const coreError = `transaction ${batchId} is already committed`;
    expect(() => tx.commit()).toThrow(`Write error: ${coreError}`);
    expect(() => tx.rollback()).toThrow(`Write error: ${coreError}`);
    expect(() => tx.insert(app.todos, { title: "Nope", done: false })).toThrow(
      `Insert failed: WriteError("${coreError}")`,
    );
    await expect(tx.all(app.todos.where({}))).rejects.toThrow(
      `Query setup failed: Write error: ${coreError}`,
    );
  });

  it("rejects transaction operations after rollback", async () => {
    const tx = db.beginTransaction();
    tx.insert(app.todos, { title: "Rolled-back transaction", done: false });
    const batchId = tx.batchId();

    tx.rollback();

    const coreError = `batch ${batchId} has already been completed or was never opened`;
    expect(() => tx.commit()).toThrow(`Commit batch failed: Write error: ${coreError}`);
    expect(() => tx.rollback()).toThrow(`Rollback batch failed: Write error: ${coreError}`);
    expect(() => tx.insert(app.todos, { title: "Nope", done: false })).toThrow(
      `Insert failed: WriteError("${coreError}")`,
    );
    await expect(tx.all(app.todos.where({}))).rejects.toThrow(
      `Query setup failed: Write error: ${coreError}`,
    );
  });

  it("rejects db transaction writes against a different client/schema", () => {
    const tx = db.beginTransaction();
    tx.insert(app.todos, { title: "Primary client", done: false });

    expect(() =>
      tx.insert(otherApp.todos, { title: "Wrong client", done: false, note: "nope" }),
    ).toThrow(/cannot be used with table "todos" from a different schema\/client/);
  });
});

describe("Db batches", () => {
  it("throws when committing a db batch before any actions", () => {
    const batch = db.beginBatch();

    expect(() => batch.commit()).toThrow(
      "DbDirectBatch.commit() requires at least one table operation first",
    );
  });

  it("rejects batch operations after commit", async () => {
    const batch = db.beginBatch();
    batch.insert(app.todos, { title: "Committed batch", done: false });
    const batchId = batch.batchId();

    batch.commit();

    const coreError = `batch ${batchId} is already committed`;
    expect(() => batch.commit()).toThrow(`Write error: ${coreError}`);
    expect(() => batch.rollback()).toThrow(`Write error: ${coreError}`);
    expect(() => batch.insert(app.todos, { title: "Nope", done: false })).toThrow(
      `Insert failed: WriteError("${coreError}")`,
    );
    await expect(batch.all(app.todos.where({}))).rejects.toThrow(
      `Query setup failed: Write error: ${coreError}`,
    );
  });

  it("rejects batch operations after rollback", async () => {
    const batch = db.beginBatch();
    batch.insert(app.todos, { title: "Rolled-back batch", done: false });
    const batchId = batch.batchId();

    batch.rollback();

    const coreError = `batch ${batchId} has already been completed or was never opened`;
    expect(() => batch.commit()).toThrow(`Commit batch failed: Write error: ${coreError}`);
    expect(() => batch.rollback()).toThrow(`Rollback batch failed: Write error: ${coreError}`);
    expect(() => batch.insert(app.todos, { title: "Nope", done: false })).toThrow(
      `Insert failed: WriteError("${coreError}")`,
    );
    await expect(batch.all(app.todos.where({}))).rejects.toThrow(
      `Query setup failed: Write error: ${coreError}`,
    );
  });

  it("rolls back a callback batch when the callback throws after a write", async () => {
    const error = new Error("callback failed");

    expect(() =>
      db.batch((batch) => {
        batch.insert(app.todos, { title: "Thrown callback batch", done: false });
        throw error;
      }),
    ).toThrow(error);

    await expect(allTodos()).resolves.toEqual([]);
  });

  it("rejects db batch writes against a different client/schema", () => {
    const batch = db.beginBatch();
    batch.insert(app.todos, { title: "Primary client", done: false });

    expect(() =>
      batch.insert(otherApp.todos, { title: "Wrong client", done: false, note: "nope" }),
    ).toThrow(/cannot be used with table "todos" from a different schema\/client/);
  });
});
