import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { createDb, type Db, type QueryBuilder, type TableProxy } from "../../src/runtime/db.js";
import type { SubscriptionDelta } from "../../src/runtime/subscription-manager.js";
import type { WasmSchema } from "../../src/drivers/types.js";

const schema: WasmSchema = {
  tables: {
    orgs: {
      columns: [{ name: "name", column_type: { type: "Text" }, nullable: false }],
    },
    teams: {
      columns: [
        { name: "name", column_type: { type: "Text" }, nullable: false },
        { name: "org_id", column_type: { type: "Uuid" }, nullable: true, references: "orgs" },
        {
          name: "parent_id",
          column_type: { type: "Uuid" },
          nullable: true,
          references: "teams",
        },
      ],
    },
    users: {
      columns: [
        { name: "name", column_type: { type: "Text" }, nullable: false },
        { name: "team_id", column_type: { type: "Uuid" }, nullable: true, references: "teams" },
      ],
    },
    todos: {
      columns: [
        { name: "title", column_type: { type: "Text" }, nullable: false },
        { name: "done", column_type: { type: "Boolean" }, nullable: false },
        { name: "priority", column_type: { type: "Integer" }, nullable: true },
        { name: "owner_id", column_type: { type: "Uuid" }, nullable: true, references: "users" },
        {
          name: "tags",
          column_type: { type: "Array", element: { type: "Text" } },
          nullable: false,
        },
      ],
    },
  },
};

interface Org {
  id: string;
  name: string;
}

interface Team {
  id: string;
  name: string;
  org_id?: string;
  parent_id?: string;
}

interface User {
  id: string;
  name: string;
  team_id?: string;
}

interface Todo {
  id: string;
  title: string;
  done: boolean;
  priority?: number;
  owner_id?: string;
  tags: string[];
}

const orgs: TableProxy<Org, Omit<Org, "id">> = {
  _table: "orgs",
  _schema: schema,
  _rowType: {} as Org,
  _initType: {} as Omit<Org, "id">,
};

const teams: TableProxy<Team, Omit<Team, "id">> = {
  _table: "teams",
  _schema: schema,
  _rowType: {} as Team,
  _initType: {} as Omit<Team, "id">,
};

const users: TableProxy<User, Omit<User, "id">> = {
  _table: "users",
  _schema: schema,
  _rowType: {} as User,
  _initType: {} as Omit<User, "id">,
};

const todos: TableProxy<Todo, Omit<Todo, "id">> = {
  _table: "todos",
  _schema: schema,
  _rowType: {} as Todo,
  _initType: {} as Omit<Todo, "id">,
};

function uniqueDbName(label: string): string {
  return `db-subscribe-all-${label}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

function makeQuery<T>(
  table: string,
  body: {
    conditions?: Array<{ column: string; op: string; value?: unknown }>;
    includes?: Record<string, boolean | object>;
    orderBy?: Array<[string, "asc" | "desc"]>;
    limit?: number;
    offset?: number;
    hops?: string[];
    gather?: {
      max_depth: number;
      step_table: string;
      step_current_column: string;
      step_conditions: Array<{ column: string; op: string; value: unknown }>;
      step_hops: string[];
    };
  },
): QueryBuilder<T> {
  return {
    _table: table,
    _schema: schema,
    _rowType: {} as T,
    _build() {
      return JSON.stringify({
        table,
        conditions: body.conditions ?? [],
        includes: body.includes ?? {},
        orderBy: body.orderBy ?? [],
        limit: body.limit,
        offset: body.offset,
        hops: body.hops,
        gather: body.gather,
      });
    },
  };
}

async function waitForCondition(
  check: () => boolean,
  timeoutMs: number,
  errorMessage: string,
): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (check()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  throw new Error(errorMessage);
}

describe("db.subscribeAll browser integration", () => {
  const dbs: Db[] = [];
  const unsubscribes: Array<() => void> = [];
  let conditionsDb: Db;
  const conditionCases: Array<{
    name: string;
    query: QueryBuilder<Todo>;
    insert: Omit<Todo, "id">;
  }> = [
    {
      name: "eq",
      query: makeQuery<Todo>("todos", {
        conditions: [{ column: "title", op: "eq", value: "eq-hit" }],
      }),
      insert: { title: "eq-hit", done: false, priority: 1, owner_id: undefined, tags: ["x"] },
    },
    {
      name: "ne",
      query: makeQuery<Todo>("todos", {
        conditions: [{ column: "title", op: "ne", value: "blocked" }],
      }),
      insert: { title: "ne-hit", done: false, priority: 2, owner_id: undefined, tags: ["x"] },
    },
    {
      name: "gt",
      query: makeQuery<Todo>("todos", {
        conditions: [{ column: "priority", op: "gt", value: 10 }],
      }),
      insert: { title: "gt-hit", done: false, priority: 11, owner_id: undefined, tags: ["x"] },
    },
    {
      name: "gte",
      query: makeQuery<Todo>("todos", {
        conditions: [{ column: "priority", op: "gte", value: 10 }],
      }),
      insert: { title: "gte-hit", done: false, priority: 10, owner_id: undefined, tags: ["x"] },
    },
    {
      name: "lt",
      query: makeQuery<Todo>("todos", {
        conditions: [{ column: "priority", op: "lt", value: 0 }],
      }),
      insert: { title: "lt-hit", done: false, priority: -1, owner_id: undefined, tags: ["x"] },
    },
    {
      name: "lte",
      query: makeQuery<Todo>("todos", {
        conditions: [{ column: "priority", op: "lte", value: 0 }],
      }),
      insert: { title: "lte-hit", done: false, priority: 0, owner_id: undefined, tags: ["x"] },
    },
    {
      name: "isNull",
      query: makeQuery<Todo>("todos", {
        conditions: [{ column: "priority", op: "isNull" }],
      }),
      insert: {
        title: "null-hit",
        done: false,
        priority: undefined,
        owner_id: undefined,
        tags: ["x"],
      },
    },
    {
      name: "contains-array",
      query: makeQuery<Todo>("todos", {
        conditions: [{ column: "tags", op: "contains", value: "needle" }],
      }),
      insert: {
        title: "contains-array-hit",
        done: false,
        priority: 1,
        owner_id: undefined,
        tags: ["needle", "hay"],
      },
    },
    {
      name: "contains-text",
      query: makeQuery<Todo>("todos", {
        conditions: [{ column: "title", op: "contains", value: "needle" }],
      }),
      insert: {
        title: "hay-needle-title",
        done: false,
        priority: 1,
        owner_id: undefined,
        tags: ["x"],
      },
    },
    {
      name: "contains-text-empty",
      query: makeQuery<Todo>("todos", {
        conditions: [{ column: "title", op: "contains", value: "" }],
      }),
      insert: {
        title: "any-title",
        done: false,
        priority: 1,
        owner_id: undefined,
        tags: ["x"],
      },
    },
  ];

  function track(db: Db): Db {
    dbs.push(db);
    return db;
  }

  function trackUnsubscribe(unsubscribe: () => void): () => void {
    unsubscribes.push(unsubscribe);
    return unsubscribe;
  }

  afterEach(async () => {
    for (const unsubscribe of unsubscribes.splice(0)) {
      try {
        unsubscribe();
      } catch {
        // best effort
      }
    }

    for (const db of dbs.splice(0).reverse()) {
      await db.shutdown();
    }
  });

  beforeAll(async () => {
    conditionsDb = await createDb({
      appId: "db-subscribe-test",
      dbName: uniqueDbName("filters"),
    });
  });

  afterAll(async () => {
    await conditionsDb.shutdown();
  });

  it("emits added, updated, removed, and all", async () => {
    const db = track(await createDb({ appId: "db-subscribe-test", dbName: uniqueDbName("delta") }));

    const deltas: Array<SubscriptionDelta<Todo>> = [];
    const unsubscribe = trackUnsubscribe(
      db.subscribeAll(
        makeQuery<Todo>("todos", {
          conditions: [{ column: "done", op: "eq", value: false }],
        }),
        (delta) => {
          deltas.push(delta);
        },
      ),
    );

    const id = db.insert(todos, {
      title: "watch-me",
      done: false,
      priority: 1,
      owner_id: undefined,
      tags: ["x"],
    });

    await waitForCondition(
      () => deltas.some((delta) => delta.added.some((row) => row.id === id)),
      4000,
      "expected add delta",
    );

    db.update(todos, id, { title: "watch-me-updated" });

    await waitForCondition(
      () => deltas.some((delta) => delta.updated.some((row) => row.id === id)),
      4000,
      "expected update delta",
    );

    db.update(todos, id, { done: true });

    await waitForCondition(
      () => deltas.some((delta) => delta.removed.some((row) => row.id === id)),
      4000,
      "expected remove delta",
    );

    const latestAll = deltas[deltas.length - 1]?.all ?? [];
    expect(latestAll.some((row) => row.id === id)).toBe(false);

    unsubscribe();
  });

  for (const testCase of conditionCases) {
    it(`supports condition filter ${testCase.name}`, async () => {
      const deltas: Array<SubscriptionDelta<Todo>> = [];
      const unsubscribe = trackUnsubscribe(
        conditionsDb.subscribeAll(testCase.query, (delta) => {
          deltas.push(delta);
        }),
      );

      const insertedId = conditionsDb.insert(todos, testCase.insert);

      await waitForCondition(
        () => deltas.some((delta) => delta.added.some((row) => row.id === insertedId)),
        4000,
        `expected add delta for ${testCase.name}`,
      );

      unsubscribe();
    });
  }

  it("supports orderBy + limit + offset", async () => {
    const db = track(await createDb({ appId: "db-subscribe-test", dbName: uniqueDbName("order") }));

    const deltas: Array<SubscriptionDelta<Todo>> = [];
    const unsubscribe = trackUnsubscribe(
      db.subscribeAll(
        makeQuery<Todo>("todos", {
          orderBy: [["priority", "desc"]],
          offset: 1,
          limit: 1,
        }),
        (delta) => deltas.push(delta),
      ),
    );

    db.insert(todos, {
      title: "p1",
      done: false,
      priority: 1,
      owner_id: undefined,
      tags: ["x"],
    });
    db.insert(todos, {
      title: "p2",
      done: false,
      priority: 2,
      owner_id: undefined,
      tags: ["x"],
    });
    db.insert(todos, {
      title: "p3",
      done: false,
      priority: 3,
      owner_id: undefined,
      tags: ["x"],
    });

    await waitForCondition(
      () => deltas.some((delta) => delta.all.length === 1 && delta.all[0]?.title === "p2"),
      4000,
      "expected sorted/paginated result in subscription",
    );

    unsubscribe();
  });

  it("does not emit add for non-matching text contains", async () => {
    const db = track(
      await createDb({ appId: "db-subscribe-test", dbName: uniqueDbName("contains-text-miss") }),
    );

    const deltas: Array<SubscriptionDelta<Todo>> = [];
    const unsubscribe = trackUnsubscribe(
      db.subscribeAll(
        makeQuery<Todo>("todos", {
          conditions: [{ column: "title", op: "contains", value: "needle" }],
        }),
        (delta) => deltas.push(delta),
      ),
    );

    const insertedId = db.insert(todos, {
      title: "completely unrelated",
      done: false,
      priority: 1,
      owner_id: undefined,
      tags: ["x"],
    });

    await new Promise((resolve) => setTimeout(resolve, 150));
    expect(deltas.some((delta) => delta.added.some((row) => row.id === insertedId))).toBe(false);

    unsubscribe();
  });

  it("supports include query execution path", async () => {
    const db = track(
      await createDb({ appId: "db-subscribe-test", dbName: uniqueDbName("include") }),
    );

    const deltas: Array<SubscriptionDelta<User>> = [];
    const unsubscribe = trackUnsubscribe(
      db.subscribeAll(
        makeQuery<User>("users", {
          includes: { todosViaOwner: true },
        }),
        (delta) => deltas.push(delta),
      ),
    );

    const userId = db.insert(users, { name: "Owner", team_id: undefined });

    await waitForCondition(
      () => deltas.some((delta) => delta.added.some((row) => row.id === userId)),
      4000,
      "expected include query subscription delta",
    );

    unsubscribe();
  });

  it("supports hop queries", async () => {
    const db = track(await createDb({ appId: "db-subscribe-test", dbName: uniqueDbName("hops") }));

    const deltas: Array<SubscriptionDelta<Org>> = [];
    const unsubscribe = trackUnsubscribe(
      db.subscribeAll(
        makeQuery<Org>("users", {
          hops: ["team", "org"],
        }),
        (delta) => deltas.push(delta),
      ),
    );

    const orgId = db.insert(orgs, { name: "Hop Org" });
    const teamId = db.insert(teams, { name: "Hop Team", org_id: orgId, parent_id: undefined });
    db.insert(users, { name: "Hop User", team_id: teamId });

    await waitForCondition(
      () =>
        deltas.some((delta) => delta.all.some((row) => row.id === orgId && row.name === "Hop Org")),
      4000,
      "expected hop query subscription result",
    );

    unsubscribe();
  });

  it("supports gather queries", async () => {
    const db = track(
      await createDb({ appId: "db-subscribe-test", dbName: uniqueDbName("gather") }),
    );

    const deltas: Array<SubscriptionDelta<Team>> = [];
    const unsubscribe = trackUnsubscribe(
      db.subscribeAll(
        makeQuery<Team>("teams", {
          conditions: [{ column: "name", op: "eq", value: "leaf" }],
          gather: {
            max_depth: 10,
            step_table: "teams",
            step_current_column: "id",
            step_conditions: [],
            step_hops: ["parent"],
          },
        }),
        (delta) => deltas.push(delta),
      ),
    );

    const rootId = db.insert(teams, { name: "root", org_id: undefined, parent_id: undefined });
    const midId = db.insert(teams, { name: "mid", org_id: undefined, parent_id: rootId });
    const leafId = db.insert(teams, { name: "leaf", org_id: undefined, parent_id: midId });

    await waitForCondition(
      () => {
        const latestAll = deltas[deltas.length - 1]?.all ?? [];
        const ids = latestAll.map((row) => row.id);
        return ids.includes(rootId) && ids.includes(midId) && ids.includes(leafId);
      },
      4000,
      "expected gather query subscription result",
    );

    unsubscribe();
  });
});
