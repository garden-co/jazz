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

describe("db transaction reads browser integration", () => {
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

  it("shows only the current transaction's staged inserts through tx.all", async () => {
    const db = track(
      await createDb({
        appId: "db-transaction-reads-test",
        driver: { type: "persistent", dbName: uniqueDbName("tx-insert-reads") },
      }),
    );

    const aliceTx = db.beginTransaction(todos);
    const bobTx = db.beginTransaction(todos);

    const aliceDraft = aliceTx.insert(todos, { title: "Alice draft", done: false });
    bobTx.insert(todos, { title: "Bob draft", done: false });

    const aliceRows = await aliceTx.all<Todo>(makeTodoQuery());
    expect(aliceRows).toEqual([aliceDraft]);

    const bobRows = await bobTx.all<Todo>(makeTodoQuery());
    expect(bobRows.map((row) => row.title)).toEqual(["Bob draft"]);
  });

  it("keeps same-row staged updates isolated to the transaction that issued them", async () => {
    const db = track(
      await createDb({
        appId: "db-transaction-reads-test",
        driver: { type: "persistent", dbName: uniqueDbName("tx-update-reads") },
      }),
    );

    const base = await db.insert(todos, { title: "Shared", done: false });

    const aliceTx = db.beginTransaction(todos);
    const bobTx = db.beginTransaction(todos);

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
});
