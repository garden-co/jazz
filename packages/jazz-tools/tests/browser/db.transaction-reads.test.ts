import { afterEach, beforeEach, describe, expect, it } from "vitest";
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

let db: Db;

beforeEach(async () => {
  db = await createDb({
    appId: "db-transaction-reads-test",
    driver: { type: "persistent", dbName: uniqueDbName("db-transaction-reads-test") },
  });
});

afterEach(async () => {
  await db.shutdown();
});

describe("db transaction reads browser integration", () => {
  it("shows only the current transaction's staged inserts through tx.all", async () => {
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
    const tx = db.beginTransaction();
    const insertedTodo = tx.insert(todos, { title: "Batch", done: false });

    expect(await db.one<Todo>(makeTodoQuery())).toBeNull();

    const _txResult = tx.commit();
    // No need to wait in this case, because the Db is not connected to a server
    // await _txResult.wait({ tier: "global" });

    expect(await db.one<Todo>(makeTodoQuery())).toMatchObject(insertedTodo);
  });

  it("changes from rolled-back transactions are not visible globally", async () => {
    const tx = db.beginTransaction();
    tx.insert(todos, { title: "Batch", done: false });

    tx.rollback();

    expect(await db.one<Todo>(makeTodoQuery())).toBeNull();
  });

  it("supports custom ids and upserts inside transactions", async () => {
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
    tx.upsert(todos, { title: "Bob drafted release notes", done: true }, { id: existingTodo.id });

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

  it("rejects partial upserts for missing rows inside transactions", async () => {
    const tx = db.beginTransaction();

    expect(() =>
      tx.upsert(todos, { done: true }, { id: "00000000-0000-0000-0000-000000000125" }),
    ).toThrow("missing required field `title`");
  });

  describe("db.transaction(cb)", () => {
    it("returns the callback value when an async transaction only reads", async () => {
      const { value: existingTodo } = db.insert(todos, {
        title: "Alice checked the roadmap",
        done: false,
      });

      await expect(
        db.transaction(async (tx) => {
          const rows = await tx.all<Todo>(makeTodoQuery());
          expect(rows).toEqual([existingTodo]);
          return "no writes needed";
        }),
      ).resolves.toMatchObject({ value: "no writes needed" });
    });

    it("rolls back cleanly when an async transaction reads then throws before writing", async () => {
      const { value: existingTodo } = db.insert(todos, {
        title: "Alice checked rollback",
        done: false,
      });
      const error = new Error("no write transaction failed");

      await expect(
        db.transaction(async (tx) => {
          const rows = await tx.all<Todo>(makeTodoQuery());
          expect(rows).toEqual([existingTodo]);
          throw error;
        }),
      ).rejects.toBe(error);

      await expect(db.all<Todo>(makeTodoQuery())).resolves.toEqual([existingTodo]);
    });

    it("commits changes once the callback resolves and the authority accepts the transaction", async () => {
      const txResult = db.transaction((tx) => {
        return tx.insert(todos, { title: "Batch", done: false });
      });
      // No need to wait in this case, because the Db is not connected to a server
      // await txResult.wait({ tier: "global" });
      const insertedTodo = txResult.value;

      expect(await db.one<Todo>(makeTodoQuery())).toMatchObject(insertedTodo);
    });

    it("does not commit changes if the callback rejects", async () => {
      expect(() =>
        db.transaction((tx) => {
          tx.insert(todos, { title: "Batch", done: false });
          throw new Error("callback failed");
        }),
      ).toThrow("callback failed");

      expect(await db.one<Todo>(makeTodoQuery())).toBeNull();
    });
  });
});

describe("db batch reads browser integration", () => {
  it("keeps uncommitted batch changes out of global reads", async () => {
    const batch = db.beginBatch();
    const insertedTodo = batch.insert(todos, { title: "Batch", done: false });

    expect(await db.one<Todo>(makeTodoQuery())).toBeNull();

    batch.commit();
    expect(await db.one<Todo>(makeTodoQuery())).toMatchObject(insertedTodo);
  });

  it("supports custom ids and upserts inside direct batches", async () => {
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
    batch.upsert(todos, { title: "Bob queued docs review", done: true }, { id: existingTodo.id });

    expect(insertedTodo).toEqual({
      id: customId,
      title: "Alice staged screenshots",
      done: false,
    });
    expect(await db.all<Todo>(makeTodoQuery())).toEqual([existingTodo]);

    batch.commit();

    const committedRows = await db.all<Todo>(makeTodoQuery());
    expect(committedRows).toHaveLength(3);
    expect(committedRows).toEqual(
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
  });

  it("rejects partial upserts for missing rows inside direct batches", async () => {
    const batch = db.beginBatch();

    expect(() =>
      batch.upsert(todos, { done: true }, { id: "00000000-0000-0000-0000-000000000225" }),
    ).toThrow("missing required field `title`");
  });

  describe("db.batch(cb)", () => {
    it("returns the callback value when an async batch only reads", async () => {
      const { value: existingTodo } = db.insert(todos, {
        title: "Alice reviewed the plan",
        done: false,
      });

      await expect(
        db.batch(async (batch) => {
          const rows = await batch.all<Todo>(makeTodoQuery());
          expect(rows).toEqual([existingTodo]);
          return "no writes needed";
        }),
      ).resolves.toMatchObject({ value: "no writes needed" });
    });

    it("rolls back cleanly when an async batch reads then throws before writing", async () => {
      const { value: existingTodo } = db.insert(todos, {
        title: "Alice reviewed rollback",
        done: false,
      });
      const error = new Error("no write batch failed");

      await expect(
        db.batch(async (batch) => {
          const rows = await batch.all<Todo>(makeTodoQuery());
          expect(rows).toEqual([existingTodo]);
          throw error;
        }),
      ).rejects.toBe(error);

      await expect(db.all<Todo>(makeTodoQuery())).resolves.toEqual([existingTodo]);
    });

    it("commits changes once the callback resolves", async () => {
      const batchResult = db.batch((batch) => {
        return batch.insert(todos, { title: "Batch", done: false });
      });
      const insertedTodo = batchResult.value;

      expect(await db.one<Todo>(makeTodoQuery())).toMatchObject(insertedTodo);
    });

    it("rolls back changes if the callback rejects", async () => {
      expect(() =>
        db.batch((batch) => {
          batch.insert(todos, { title: "Batch", done: false });
          throw new Error("callback failed");
        }),
      ).toThrow("callback failed");

      expect(await db.one<Todo>(makeTodoQuery())).toBeNull();
    });
  });
});
