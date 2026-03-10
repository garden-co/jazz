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
      return JSON.stringify({
        table: "todos",
        conditions: [],
        includes: {},
        orderBy: [],
      });
    },
  };
}

const dbs: Db[] = [];

afterEach(async () => {
  while (dbs.length > 0) {
    const db = dbs.pop();
    if (db) {
      await db.shutdown();
    }
  }
});

async function makeDb(devMode?: boolean, userBranch?: string): Promise<Db> {
  const db = await createDb({
    appId: `dev-mode-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    devMode,
    userBranch,
  });
  dbs.push(db);
  return db;
}

describe("Db devMode active query tracing", () => {
  it("does not expose traces when devMode is disabled", async () => {
    const db = await makeDb(false);
    const unsubscribe = db.subscribeAll(makeQuery(), () => undefined);

    expect(db.getActiveQuerySubscriptions()).toEqual([]);

    unsubscribe();
  });

  it("tracks visible subscriptions with default metadata and cleanup", async () => {
    const db = await makeDb(true, "feature-branch");
    const observed: Array<ReturnType<Db["getActiveQuerySubscriptions"]>> = [];
    const stop = db.onActiveQuerySubscriptionsChange((traces) => {
      observed.push(traces.map((trace) => ({ ...trace })));
    });

    const unsubscribe = db.subscribeAll(makeQuery(), () => undefined);
    const [trace] = db.getActiveQuerySubscriptions();

    expect(trace?.table).toBe("todos");
    expect(trace?.branches).toEqual(["feature-branch"]);
    expect(trace?.tier).toBe("worker");
    expect(trace?.query).toContain('"table":"todos"');
    expect(trace?.stack).toContain("Error");
    expect(observed.at(-1)).toHaveLength(1);

    unsubscribe();

    expect(db.getActiveQuerySubscriptions()).toEqual([]);
    expect(observed.at(-1)).toEqual([]);

    stop();
  });

  it("filters hidden subscriptions out of the inspector list", async () => {
    const db = await makeDb(true);

    const unsubscribe = db.subscribeAll(makeQuery(), () => undefined, {
      visibility: "hidden_from_live_query_list",
    });

    expect(db.getActiveQuerySubscriptions()).toEqual([]);

    unsubscribe();
  });

  it("records explicit tier overrides", async () => {
    const db = await makeDb(true);
    const unsubscribe = db.subscribeAll(makeQuery(), () => undefined, { tier: "edge" });

    expect(db.getActiveQuerySubscriptions()[0]?.tier).toBe("edge");

    unsubscribe();
  });

  it("clears traces on shutdown", async () => {
    const db = await makeDb(true);
    const observed: Array<ReturnType<Db["getActiveQuerySubscriptions"]>> = [];
    const stop = db.onActiveQuerySubscriptionsChange((traces) => {
      observed.push(traces.map((trace) => ({ ...trace })));
    });

    db.subscribeAll(makeQuery(), () => undefined);
    await db.shutdown();
    dbs.splice(dbs.indexOf(db), 1);

    expect(db.getActiveQuerySubscriptions()).toEqual([]);
    expect(observed.at(-1)).toEqual([]);

    stop();
  });
});
