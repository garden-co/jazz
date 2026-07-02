import { afterEach, describe, expect, it } from "vitest";
import { createDb, type Db } from "./db.js";
import { col, defineApp } from "../index.js";

const app = defineApp({
  todos: { title: col.string() },
});

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
  it("returns null before any client has been created", async () => {
    const db = await makeDb();
    expect(db.getRuntimeSchema()).toBeNull();
  });

  it("returns the runtime schema once a client exists", async () => {
    const db = await makeDb();
    const unsubscribe = db.subscribeAll(app.todos, () => undefined);

    const runtimeSchema = db.getRuntimeSchema();
    expect(runtimeSchema).toBeTruthy();
    expect(runtimeSchema?.todos).toBeDefined();
    expect(runtimeSchema?.todos?.columns.some((c) => c.name === "title")).toBe(true);

    unsubscribe();
  });
});
