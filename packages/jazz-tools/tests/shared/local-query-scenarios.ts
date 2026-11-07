import { expect } from "vitest";
import { schema } from "../../src/index.js";
import type { Db, QueryBuilder } from "../../src/runtime/db.js";

export const app = schema.defineApp(
  {
    orgs: schema.table({ name: schema.string() }, { teamsViaOrg: schema.reverse("teams", "org") }),
    teams: schema.table(
      {
        name: schema.string(),
        org_id: schema.uuid().optional(),
        parent_id: schema.uuid().optional(),
      },
      {
        org: schema.rel("orgs", "org_id"),
        parent: schema.rel("teams", "parent_id"),
        teamsViaParent: schema.reverse("teams", "parent"),
        usersViaTeam: schema.reverse("users", "team"),
      },
    ),
    users: schema.table(
      {
        name: schema.string(),
        team_id: schema.uuid().optional(),
      },
      { team: schema.rel("teams", "team_id"), todosViaOwner: schema.reverse("todos", "owner") },
    ),
    todos: schema.table(
      {
        title: schema.string(),
        done: schema.boolean(),
        priority: schema.int().optional(),
        owner_id: schema.uuid().optional(),
        tags: schema.array(schema.string()),
        payload: schema.bytes().optional(),
      },
      { owner: schema.rel("users", "owner_id") },
    ),
    bundle_items: schema.table(
      { label: schema.string() },
      { bundlesViaItems: schema.reverse("bundles", "itemsRelation") },
    ),
    bundles: schema.table(
      {
        name: schema.string(),
        items: schema.array(schema.uuid()),
      },
      { itemsRelation: schema.rel("bundle_items", "items") },
    ),
  },
  { migrations: "none" },
);

export const { orgs, teams, users, todos, bundle_items: bundleItems, bundles } = app;
export type Org = schema.RowOf<typeof orgs>;
export type Team = schema.RowOf<typeof teams>;
export type User = schema.RowOf<typeof users>;
export type BundleItem = schema.RowOf<typeof bundleItems>;
export type TodoInsert = schema.InsertOf<typeof todos>;
export type Todo = schema.RowOf<typeof todos>;

export const CONDITION_OWNER_ID = "00000000-0000-0000-0000-000000000101";
export const CONDITION_ALPHA_ID = "00000000-0000-0000-0000-000000000102";
export const CONDITION_BETA_ID = "00000000-0000-0000-0000-000000000103";
export const CONDITION_GAMMA_ID = "00000000-0000-0000-0000-000000000104";

export const conditionCases: Array<{
  name: string;
  query: QueryBuilder<Todo>;
  expectedTitles: string[];
}> = [
  {
    name: "eq",
    query: todos.where({ title: "alpha" }),
    expectedTitles: ["alpha"],
  },
  {
    name: "ne",
    query: todos.where({ title: { ne: "alpha" } }),
    expectedTitles: ["beta", "gamma"],
  },
  {
    name: "gt",
    query: todos.where({ priority: { gt: 1 } }),
    expectedTitles: ["beta"],
  },
  {
    name: "gte",
    query: todos.where({ priority: { gte: 2 } }),
    expectedTitles: ["beta"],
  },
  {
    name: "lt",
    query: todos.where({ priority: { lt: 2 } }),
    expectedTitles: ["alpha"],
  },
  {
    name: "lte",
    query: todos.where({ priority: { lte: 1 } }),
    expectedTitles: ["alpha"],
  },
  {
    name: "isNull",
    query: todos.where({ priority: null }),
    expectedTitles: ["gamma"],
  },
  {
    name: "contains-array",
    query: todos.where({ tags: { contains: "work" } }),
    expectedTitles: ["alpha", "gamma"],
  },
  {
    name: "contains-text",
    query: todos.where({ title: { contains: "alp" } }),
    expectedTitles: ["alpha"],
  },
  {
    name: "contains-text-empty",
    query: todos.where({ title: { contains: "" } }),
    expectedTitles: ["alpha", "beta", "gamma"],
  },
  {
    name: "in-id",
    query: todos.where({
      id: { in: [CONDITION_ALPHA_ID, "00000000-0000-0000-0000-000000000199"] },
    }),
    expectedTitles: ["alpha"],
  },
  {
    name: "in-text",
    query: todos.where({ title: { in: ["alpha", "gamma"] } }),
    expectedTitles: ["alpha", "gamma"],
  },
  {
    name: "in-boolean",
    query: todos.where({ done: { in: [false] } }),
    expectedTitles: ["alpha"],
  },
  {
    name: "in-number",
    query: todos.where({ priority: { in: [1, 999] } }),
    expectedTitles: ["alpha"],
  },
  {
    name: "in-reference",
    query: todos.where({ owner_id: { in: [CONDITION_OWNER_ID] } }),
    expectedTitles: ["alpha", "beta", "gamma"],
  },
  {
    name: "in-array-whole-value",
    query: todos.where({ tags: { in: [["work", "backend"]] } }),
    expectedTitles: ["alpha"],
  },
  {
    name: "in-bytea",
    query: todos.where({ payload: { in: [new Uint8Array([1, 2, 3])] } }),
    expectedTitles: ["alpha"],
  },
  {
    name: "in-empty",
    query: todos.where({ title: { in: [] } }),
    expectedTitles: [],
  },
  {
    name: "eq-bytea",
    query: todos.where({ payload: { eq: new Uint8Array([1, 2, 3]) } }),
    expectedTitles: ["alpha"],
  },
];
export async function seedTodosForConditions(db: Db): Promise<void> {
  const {
    value: { id: orgId },
  } = await db.insert(orgs, { name: "Acme" });
  const {
    value: { id: teamId },
  } = await db.insert(teams, {
    name: "Core",
    org_id: orgId,
    parent_id: undefined,
  });
  const {
    value: { id: userId },
  } = await db.insert(users, { name: "Alice", team_id: teamId }, { id: CONDITION_OWNER_ID });

  await db.insert(
    todos,
    {
      title: "alpha",
      done: false,
      priority: 1,
      owner_id: userId,
      tags: ["work", "backend"],
      payload: new Uint8Array([1, 2, 3]),
    },
    { id: CONDITION_ALPHA_ID },
  );
  await db.insert(
    todos,
    {
      title: "beta",
      done: true,
      priority: 2,
      owner_id: userId,
      tags: ["home"],
      payload: new Uint8Array([4, 5, 6]),
    },
    { id: CONDITION_BETA_ID },
  );
  await db.insert(
    todos,
    {
      title: "gamma",
      done: true,
      priority: undefined,
      owner_id: userId,
      tags: ["work", "urgent"],
      payload: undefined,
    },
    { id: CONDITION_GAMMA_ID },
  );
}
export async function assertByteaQuery(db: Db): Promise<void> {
  const {
    value: { id },
  } = await db.insert(todos, {
    title: "has-bytes",
    done: false,
    priority: 1,
    owner_id: undefined,
    tags: ["x"],
    payload: new Uint8Array([0, 1, 2, 255]),
  });

  const rows = await db.all(todos.where({ id }));

  expect(rows).toHaveLength(1);
  expect(rows[0]?.payload).toBeInstanceOf(Uint8Array);
  expect(Array.from(rows[0]?.payload ?? [])).toEqual([0, 1, 2, 255]);
}

export async function assertUuidOrderQuery(db: Db): Promise<void> {
  const ids: string[] = [];
  for (const title of ["first", "second", "third"]) {
    const result = await db.insert(todos, {
      title,
      done: false,
      priority: undefined,
      owner_id: undefined,
      tags: [],
      payload: undefined,
    });
    ids.push(result.value.id);
  }

  for (const id of ids) {
    expect(id).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
  }
  const generatedAtMs = Number.parseInt(ids[0]!.replaceAll("-", "").slice(0, 12), 16);
  expect(Math.abs(Date.now() - generatedAtMs)).toBeLessThan(60_000);
  expect(ids).toEqual([...ids].sort());

  const rows = await db.all(todos);
  expect(rows.map((row) => row.id)).toEqual(ids);
}

export async function assertWindowQuery(db: Db): Promise<void> {
  await db.insert(todos, {
    title: "p1",
    done: false,
    priority: 1,
    owner_id: undefined,
    tags: ["x"],
  });
  await db.insert(todos, {
    title: "p2",
    done: false,
    priority: 2,
    owner_id: undefined,
    tags: ["x"],
  });
  await db.insert(todos, {
    title: "p3",
    done: false,
    priority: 3,
    owner_id: undefined,
    tags: ["x"],
  });

  const rows = await db.all(todos.orderBy("priority", "desc").offset(1).limit(1));

  expect(rows).toHaveLength(1);
  expect(rows[0].priority).toBe(2);
  expect(rows[0].title).toBe("p2");
}

export async function assertConditionQuery(
  db: Db,
  testCase: (typeof conditionCases)[number],
): Promise<void> {
  const rows = await db.all(testCase.query);
  const actual = rows.map((row) => row.title).sort();
  const expected = [...testCase.expectedTitles].sort();
  expect(actual).toEqual(expected);
}

export function makeLocalQuery<T>(
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
    _schema: todos._schema,
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

// Live queries observe rows inserted after subscription; snapshot queries above use a fixed seed.
export const liveConditionCases: Array<{
  name: string;
  query: QueryBuilder<Todo>;
  insert: TodoInsert;
  insertId?: string;
}> = [
  {
    name: "eq",
    query: makeLocalQuery<Todo>("todos", {
      conditions: [{ column: "title", op: "eq", value: "eq-hit" }],
    }),
    insert: { title: "eq-hit", done: false, priority: 1, owner_id: undefined, tags: ["x"] },
  },
  {
    name: "ne",
    query: makeLocalQuery<Todo>("todos", {
      conditions: [{ column: "title", op: "ne", value: "blocked" }],
    }),
    insert: { title: "ne-hit", done: false, priority: 2, owner_id: undefined, tags: ["x"] },
  },
  {
    name: "gt",
    query: makeLocalQuery<Todo>("todos", {
      conditions: [{ column: "priority", op: "gt", value: 10 }],
    }),
    insert: { title: "gt-hit", done: false, priority: 11, owner_id: undefined, tags: ["x"] },
  },
  {
    name: "gte",
    query: makeLocalQuery<Todo>("todos", {
      conditions: [{ column: "priority", op: "gte", value: 10 }],
    }),
    insert: { title: "gte-hit", done: false, priority: 10, owner_id: undefined, tags: ["x"] },
  },
  {
    name: "lt",
    query: makeLocalQuery<Todo>("todos", {
      conditions: [{ column: "priority", op: "lt", value: 2 }],
    }),
    insert: { title: "lt-hit", done: false, priority: 1, owner_id: undefined, tags: ["x"] },
  },
  {
    name: "lte",
    query: makeLocalQuery<Todo>("todos", {
      conditions: [{ column: "priority", op: "lte", value: 0 }],
    }),
    insert: { title: "lte-hit", done: false, priority: 0, owner_id: undefined, tags: ["x"] },
  },
  {
    name: "isNull",
    query: makeLocalQuery<Todo>("todos", {
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
    query: makeLocalQuery<Todo>("todos", {
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
    query: makeLocalQuery<Todo>("todos", {
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
    query: makeLocalQuery<Todo>("todos", {
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
  {
    name: "in-id",
    query: makeLocalQuery<Todo>("todos", {
      conditions: [
        {
          column: "id",
          op: "in",
          value: [CONDITION_ALPHA_ID, "00000000-0000-0000-0000-000000000299"],
        },
      ],
    }),
    insert: {
      title: "in-id-hit",
      done: false,
      priority: 1,
      owner_id: undefined,
      tags: ["x"],
    },
    insertId: CONDITION_ALPHA_ID,
  },
  {
    name: "in-text",
    query: makeLocalQuery<Todo>("todos", {
      conditions: [{ column: "title", op: "in", value: ["in-text-hit", "other"] }],
    }),
    insert: {
      title: "in-text-hit",
      done: false,
      priority: 1,
      owner_id: undefined,
      tags: ["x"],
    },
  },
  {
    name: "in-reference",
    query: makeLocalQuery<Todo>("todos", {
      conditions: [{ column: "owner_id", op: "in", value: [CONDITION_OWNER_ID] }],
    }),
    insert: {
      title: "in-reference-hit",
      done: false,
      priority: 1,
      owner_id: CONDITION_OWNER_ID,
      tags: ["x"],
    },
  },
  {
    name: "eq-bytea",
    query: makeLocalQuery<Todo>("todos", {
      conditions: [{ column: "payload", op: "eq", value: [1, 2, 3] }],
    }),
    insert: {
      title: "eq-bytea-hit",
      done: false,
      priority: 1,
      owner_id: undefined,
      tags: ["x"],
      payload: new Uint8Array([1, 2, 3]),
    },
  },
];
