import { afterEach, describe, expect, it } from "vitest";
import { createDb, type Db, type QueryBuilder, type TableProxy } from "../../src/runtime/db.js";
import type { WasmSchema } from "../../src/drivers/types.js";
import { uniqueDbName } from "./support.js";

const schema: WasmSchema = {
  todos: {
    columns: [
      { name: "title", column_type: { type: "Text" }, nullable: false },
      { name: "done", column_type: { type: "Boolean" }, nullable: false },
    ],
  },
};

interface Todo {
  id: string;
  title: string;
  done: boolean;
}

const todos: TableProxy<Todo, Omit<Todo, "id">> = {
  _table: "todos",
  _schema: schema,
  _rowType: {} as Todo,
  _initType: {} as Omit<Todo, "id">,
};

function makeTodoQuery(
  conditions: Array<{ column: string; op: string; value?: unknown }> = [],
): QueryBuilder<Todo> {
  return {
    _table: "todos",
    _schema: schema,
    _rowType: {} as Todo,
    _build() {
      return JSON.stringify({
        table: "todos",
        conditions,
        includes: {},
        orderBy: [],
      });
    },
  };
}

const dbs: Db[] = [];

function track(db: Db): Db {
  dbs.push(db);
  return db;
}

afterEach(async () => {
  for (const db of dbs.splice(0).reverse()) {
    await db.shutdown();
  }
});

describe("db transaction reads browser integration", () => {
  it("shows only the current transaction's staged inserts through tx.all", async () => {
    const db = track(
      await createDb({
        appId: "db-transaction-reads-test",
        driver: { type: "persistent", dbName: uniqueDbName("tx-insert-reads") },
      }),
    );

    const aliceTx = db.beginTransaction();
    const bobTx = db.beginTransaction();

    const aliceDraft = aliceTx.insert(todos, { title: "Alice draft", done: false });
    const bobDraft = bobTx.insert(todos, { title: "Bob draft", done: false });

    const aliceRows = await aliceTx.all<Todo>(makeTodoQuery());
    expect(aliceRows).toEqual([aliceDraft]);

    const bobRows = await bobTx.all<Todo>(makeTodoQuery());
    expect(bobRows).toEqual([bobDraft]);

    const globalRows = await db.all<Todo>(makeTodoQuery());
    expect(globalRows).toEqual([]);
  });

  it("keeps same-row staged updates isolated to the transaction that issued them", async () => {
    const db = track(
      await createDb({
        appId: "db-transaction-reads-test",
        driver: { type: "persistent", dbName: uniqueDbName("tx-update-reads") },
      }),
    );

    const { value: base } = db.insert(todos, { title: "Shared", done: false });

    const aliceTx = db.beginTransaction();
    const bobTx = db.beginTransaction();

    aliceTx.update(todos, base.id, { title: "Alice draft" });
    bobTx.update(todos, base.id, { title: "Bob draft" });

    expect(await db.one<Todo>(makeTodoQuery())).toEqual(base);

    await expect(aliceTx.one<Todo>(makeTodoQuery())).resolves.toMatchObject({
      id: base.id,
      title: "Alice draft",
      done: false,
    });
    await expect(bobTx.one<Todo>(makeTodoQuery())).resolves.toMatchObject({
      id: base.id,
      title: "Bob draft",
      done: false,
    });
  });

  it("makes transaction writes visible globally once the transaction commits and the authority accepts the transaction", async () => {
    const db = track(
      await createDb({
        appId: "db-transaction-reads-test",
        driver: { type: "persistent", dbName: uniqueDbName("tx-commit-reads") },
      }),
    );

    const tx = db.beginTransaction();
    const insertedTodo = tx.insert(todos, { title: "Batch", done: false });

    expect(await db.one<Todo>(makeTodoQuery())).toBeNull();

    const _txResult = tx.commit();
    // No need to wait in this case, because the Db is not connected to a server
    // await _txResult.wait({ tier: "global" });

    expect(await db.one<Todo>(makeTodoQuery())).toMatchObject(insertedTodo);
  });

  it("supports custom ids and upserts inside transactions", async () => {
    const db = track(
      await createDb({
        appId: "db-transaction-reads-test",
        driver: { type: "persistent", dbName: uniqueDbName("tx-write-api-parity") },
      }),
    );

    const { value: existingTodo } = db.insert(todos, {
      title: "Bob drafted release notes",
      done: false,
    });

    const tx = db.beginTransaction();

    const customId = "00000000-0000-0000-0000-000000000123";
    const insertedTodo = tx.insert(
      todos,
      { title: "Alice planned the launch", done: false },
      { id: customId },
    );

    const createdByUpsertId = "00000000-0000-0000-0000-000000000124";
    tx.upsert(todos, { title: "Bob wrote release notes", done: false }, { id: createdByUpsertId });
    tx.upsert(todos, { done: true }, { id: existingTodo.id });

    expect(insertedTodo).toEqual({
      id: customId,
      title: "Alice planned the launch",
      done: false,
    });
    expect(await db.all<Todo>(makeTodoQuery())).toEqual([existingTodo]);

    tx.commit();

    const committedRows = await db.all<Todo>(makeTodoQuery());
    expect(committedRows).toHaveLength(3);
    expect(committedRows).toEqual(
      expect.arrayContaining([
        insertedTodo,
        {
          id: createdByUpsertId,
          title: "Bob wrote release notes",
          done: false,
        },
        {
          id: existingTodo.id,
          title: "Bob drafted release notes",
          done: true,
        },
      ]),
    );
  });

  it("commits changes once the callback resolves and the authority accepts the transaction", async () => {
    const db = track(
      await createDb({
        appId: "db-batch-reads-test",
        driver: { type: "persistent", dbName: uniqueDbName("tx-batch-reads") },
      }),
    );

    const txResult = db.transaction((tsx) => {
      return tsx.insert(todos, { title: "Batch", done: false });
    });
    // No need to wait in this case, because the Db is not connected to a server
    // await txResult.wait({ tier: "global" });
    const insertedTodo = txResult.value;

    expect(await db.one<Todo>(makeTodoQuery())).toMatchObject(insertedTodo);
  });

  it("does not commit changes if the callback rejects", async () => {
    const db = track(
      await createDb({
        appId: "db-batch-reads-test",
        driver: { type: "persistent", dbName: uniqueDbName("tx-batch-reads") },
      }),
    );

    expect(() =>
      db.transaction((tsx) => {
        tsx.insert(todos, { title: "Batch", done: false });
        throw new Error("callback failed");
      }),
    ).toThrow("callback failed");

    expect(await db.one<Todo>(makeTodoQuery())).toBeNull();
  });
});

describe("db batch reads browser integration", () => {
  it("changes in an uncommited batch are visible globally", async () => {
    const db = track(
      await createDb({
        appId: "db-batch-reads-test",
        driver: { type: "persistent", dbName: uniqueDbName("tx-batch-reads") },
      }),
    );

    const batch = db.beginBatch();
    const insertedTodo = batch.insert(todos, { title: "Batch", done: false });

    // Changes are visible globally even without a batch.commit()

    expect(await db.one<Todo>(makeTodoQuery())).toMatchObject(insertedTodo);
  });

  it("commits changes once the callback resolves", async () => {
    const db = track(
      await createDb({
        appId: "db-batch-reads-test",
        driver: { type: "persistent", dbName: uniqueDbName("tx-batch-reads") },
      }),
    );

    const batchResult = db.batch((batch) => {
      return batch.insert(todos, { title: "Batch", done: false });
    });
    const insertedTodo = batchResult.value;

    expect(await db.one<Todo>(makeTodoQuery())).toMatchObject(insertedTodo);
  });

  it("supports custom ids and upserts inside direct batches", async () => {
    const db = track(
      await createDb({
        appId: "db-batch-reads-test",
        driver: { type: "persistent", dbName: uniqueDbName("batch-write-api-parity") },
      }),
    );

    const { value: existingTodo } = db.insert(todos, {
      title: "Bob queued docs review",
      done: false,
    });

    const batch = db.beginBatch();

    const customId = "00000000-0000-0000-0000-000000000223";
    const insertedTodo = batch.insert(
      todos,
      { title: "Alice staged screenshots", done: false },
      { id: customId },
    );

    const createdByUpsertId = "00000000-0000-0000-0000-000000000224";
    batch.upsert(todos, { title: "Bob checked the docs", done: false }, { id: createdByUpsertId });
    batch.upsert(todos, { done: true }, { id: existingTodo.id });

    expect(insertedTodo).toEqual({
      id: customId,
      title: "Alice staged screenshots",
      done: false,
    });
    const visibleRows = await db.all<Todo>(makeTodoQuery());
    expect(visibleRows).toHaveLength(3);
    expect(visibleRows).toEqual(
      expect.arrayContaining([
        {
          id: existingTodo.id,
          title: "Bob queued docs review",
          done: true,
        },
        insertedTodo,
        {
          id: createdByUpsertId,
          title: "Bob checked the docs",
          done: false,
        },
      ]),
    );

    batch.commit();
  });

  it("does not rollback changes if the callback rejects", async () => {
    const db = track(
      await createDb({
        appId: "db-batch-reads-test",
        driver: { type: "persistent", dbName: uniqueDbName("tx-batch-reads") },
      }),
    );

    let insertedTodo: Todo | undefined;
    expect(() =>
      db.batch((batch) => {
        insertedTodo = batch.insert(todos, { title: "Batch", done: false });
        throw new Error("callback failed");
      }),
    ).toThrow("callback failed");

    const globalTodo = await db.one<Todo>(makeTodoQuery());
    expect(globalTodo).toBeDefined();
    expect(globalTodo).toEqual(insertedTodo);
  });
});
