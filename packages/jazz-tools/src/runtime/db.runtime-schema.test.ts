import { afterEach, describe, expect, it } from "vitest";
import { createDb, type Db, type QueryBuilder } from "./db.js";
import type { WasmSchema } from "../drivers/types.js";

const schema: WasmSchema = {
  todos: {
    columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }],
  },
};

function makeQuery(): QueryBuilder<{ id: string; title: string }> {
  return {
    _table: "todos",
    _schema: schema,
    _rowType: {} as { id: string; title: string },
    _build() {
      return JSON.stringify({ table: "todos", conditions: [], includes: {}, orderBy: [] });
    },
  };
}

const dbs: Db[] = [];

afterEach(async () => {
  while (dbs.length > 0) {
    const db = dbs.pop();
    if (db) await db.shutdown();
  }
});

async function makeDb(): Promise<Db> {
  const db = await createDb({
    appId: `runtime-schema-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
  });
  dbs.push(db);
  return db;
}

describe("Db.getRuntimeSchema", () => {
  it("throws before any client has been created", async () => {
    const db = await makeDb();
    expect(() => db.getRuntimeSchema()).toThrow(/runtime client/);
  });

  it("returns the runtime schema once a client exists", async () => {
    const db = await makeDb();
    const unsubscribe = db.subscribeAll(makeQuery(), () => undefined);

    const runtimeSchema = db.getRuntimeSchema();
    expect(runtimeSchema).toBeTruthy();
    expect(runtimeSchema.todos).toBeDefined();
    expect(runtimeSchema.todos!.columns.some((c) => c.name === "title")).toBe(true);

    unsubscribe();
  });
});
