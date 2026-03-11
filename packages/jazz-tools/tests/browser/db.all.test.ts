import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { createDb, type Db, type QueryBuilder, type TableProxy } from "../../src/runtime/db.js";
import type { WasmSchema } from "../../src/drivers/types.js";

const schema: WasmSchema = {
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
      { name: "payload", column_type: { type: "Bytea" }, nullable: true },
    ],
  },
  file_parts: {
    columns: [{ name: "label", column_type: { type: "Text" }, nullable: false }],
  },
  files: {
    columns: [
      { name: "name", column_type: { type: "Text" }, nullable: false },
      {
        name: "parts",
        column_type: { type: "Array", element: { type: "Uuid" } },
        nullable: false,
        references: "file_parts",
      },
    ],
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
  todosViaOwner?: Todo[];
}

interface Todo {
  id: string;
  title: string;
  done: boolean;
  priority?: number;
  owner_id?: string;
  tags: string[];
  payload?: Uint8Array;
  owner?: User;
}

interface FilePart {
  id: string;
  label: string;
}

interface File {
  id: string;
  name: string;
  parts: string[];
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

const todos: TableProxy<Todo, Omit<Todo, "id" | "owner">> = {
  _table: "todos",
  _schema: schema,
  _rowType: {} as Todo,
  _initType: {} as Omit<Todo, "id" | "owner">,
};

const fileParts: TableProxy<FilePart, Omit<FilePart, "id">> = {
  _table: "file_parts",
  _schema: schema,
  _rowType: {} as FilePart,
  _initType: {} as Omit<FilePart, "id">,
};

const files: TableProxy<File, Omit<File, "id">> = {
  _table: "files",
  _schema: schema,
  _rowType: {} as File,
  _initType: {} as Omit<File, "id">,
};

function uniqueDbName(label: string): string {
  return `db-all-${label}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
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

describe("db.all browser integration", () => {
  const dbs: Db[] = [];
  let conditionsDb: Db;
  const conditionCases: Array<{
    name: string;
    conditions: Array<{ column: string; op: string; value?: unknown }>;
    expectedTitles: string[];
  }> = [
    {
      name: "eq",
      conditions: [{ column: "title", op: "eq", value: "alpha" }],
      expectedTitles: ["alpha"],
    },
    {
      name: "ne",
      conditions: [{ column: "title", op: "ne", value: "alpha" }],
      expectedTitles: ["beta", "gamma"],
    },
    {
      name: "gt",
      conditions: [{ column: "priority", op: "gt", value: 1 }],
      expectedTitles: ["beta"],
    },
    {
      name: "gte",
      conditions: [{ column: "priority", op: "gte", value: 2 }],
      expectedTitles: ["beta"],
    },
    {
      name: "lt",
      conditions: [{ column: "priority", op: "lt", value: 2 }],
      expectedTitles: ["alpha"],
    },
    {
      name: "lte",
      conditions: [{ column: "priority", op: "lte", value: 1 }],
      expectedTitles: ["alpha"],
    },
    {
      name: "isNull",
      conditions: [{ column: "priority", op: "isNull" }],
      expectedTitles: ["gamma"],
    },
    {
      name: "contains-array",
      conditions: [{ column: "tags", op: "contains", value: "work" }],
      expectedTitles: ["alpha", "gamma"],
    },
    {
      name: "contains-text",
      conditions: [{ column: "title", op: "contains", value: "alp" }],
      expectedTitles: ["alpha"],
    },
    {
      name: "contains-text-empty",
      conditions: [{ column: "title", op: "contains", value: "" }],
      expectedTitles: ["alpha", "beta", "gamma"],
    },
    {
      name: "eq-bytea",
      conditions: [{ column: "payload", op: "eq", value: [1, 2, 3] }],
      expectedTitles: ["alpha"],
    },
  ];

  function track(db: Db): Db {
    dbs.push(db);
    return db;
  }

  async function seedTodosForConditions(db: Db): Promise<void> {
    const { id: orgId } = await db.insert(orgs, { name: "Acme" });
    const { id: teamId } = await db.insert(teams, {
      name: "Core",
      org_id: orgId,
      parent_id: undefined,
    });
    const { id: userId } = await db.insert(users, { name: "Alice", team_id: teamId });

    await db.insert(todos, {
      title: "alpha",
      done: false,
      priority: 1,
      owner_id: userId,
      tags: ["work", "backend"],
      payload: new Uint8Array([1, 2, 3]),
    });
    await db.insert(todos, {
      title: "beta",
      done: true,
      priority: 2,
      owner_id: userId,
      tags: ["home"],
      payload: new Uint8Array([4, 5, 6]),
    });
    await db.insert(todos, {
      title: "gamma",
      done: true,
      priority: undefined,
      owner_id: userId,
      tags: ["work", "urgent"],
      payload: undefined,
    });
  }

  afterEach(async () => {
    for (const db of dbs.splice(0).reverse()) {
      await db.shutdown();
    }
  });

  beforeAll(async () => {
    conditionsDb = await createDb({
      appId: "db-all-test",
      driver: { type: "persistent", dbName: uniqueDbName("ops") },
    });
    await seedTodosForConditions(conditionsDb);
  });

  afterAll(async () => {
    await conditionsDb.shutdown();
  });

  for (const testCase of conditionCases) {
    it(`supports condition operator ${testCase.name}`, async () => {
      const rows = await conditionsDb.all<Todo>(
        makeQuery<Todo>("todos", { conditions: testCase.conditions }),
      );
      const actual = rows.map((row) => row.title).sort();
      const expected = [...testCase.expectedTitles].sort();
      expect(actual).toEqual(expected);
    });
  }

  it("returns BYTEA columns as Uint8Array", async () => {
    const db = track(
      await createDb({
        appId: "db-all-test",
        driver: { type: "persistent", dbName: uniqueDbName("bytea") },
      }),
    );

    const { id } = await db.insert(todos, {
      title: "has-bytes",
      done: false,
      priority: 1,
      owner_id: undefined,
      tags: ["x"],
      payload: new Uint8Array([0, 1, 2, 255]),
    });

    const rows = await db.all<Todo>(
      makeQuery<Todo>("todos", {
        conditions: [{ column: "id", op: "eq", value: id }],
      }),
    );

    expect(rows).toHaveLength(1);
    expect(rows[0]?.payload).toBeInstanceOf(Uint8Array);
    expect(Array.from(rows[0]?.payload ?? [])).toEqual([0, 1, 2, 255]);
  });

  it("supports orderBy + limit + offset", async () => {
    const db = track(
      await createDb({
        appId: "db-all-test",
        driver: { type: "persistent", dbName: uniqueDbName("paginate") },
      }),
    );

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

    const rows = await db.all<Todo>(
      makeQuery<Todo>("todos", {
        orderBy: [["priority", "desc"]],
        offset: 1,
        limit: 1,
      }),
    );

    expect(rows).toHaveLength(1);
    expect(rows[0].priority).toBe(2);
    expect(rows[0].title).toBe("p2");
  });

  it("supports filtered top-k when filter and sort use the same column", async () => {
    const db = track(
      await createDb({
        appId: "db-all-test",
        driver: { type: "persistent", dbName: uniqueDbName("filtered-topk-same-column") },
      }),
    );

    for (const [title, priority] of [
      ["Alice", 100],
      ["Bob", 50],
      ["Charlie", 75],
      ["Diana", 60],
    ] as const) {
      await db.insert(todos, {
        title,
        done: false,
        priority,
        owner_id: undefined,
        tags: ["x"],
      });
    }

    const rows = await db.all<Todo>(
      makeQuery<Todo>("todos", {
        conditions: [{ column: "priority", op: "gte", value: 60 }],
        orderBy: [["priority", "asc"]],
        limit: 2,
      }),
    );

    expect(rows.map((row) => ({ title: row.title, priority: row.priority }))).toEqual([
      { title: "Diana", priority: 60 },
      { title: "Charlie", priority: 75 },
    ]);
  });

  it("supports filtered top-k when filter and sort use different columns", async () => {
    const db = track(
      await createDb({
        appId: "db-all-test",
        driver: { type: "persistent", dbName: uniqueDbName("filtered-topk-different-columns") },
      }),
    );

    for (const [title, priority] of [
      ["Alice", 10],
      ["Bob", 60],
      ["Alice", 50],
      ["Alice", 40],
      ["Cara", 70],
    ] as const) {
      await db.insert(todos, {
        title,
        done: false,
        priority,
        owner_id: undefined,
        tags: ["x"],
      });
    }

    const rows = await db.all<Todo>(
      makeQuery<Todo>("todos", {
        conditions: [{ column: "title", op: "eq", value: "Alice" }],
        orderBy: [["priority", "desc"]],
        limit: 2,
      }),
    );

    expect(rows.map((row) => ({ title: row.title, priority: row.priority }))).toEqual([
      { title: "Alice", priority: 50 },
      { title: "Alice", priority: 40 },
    ]);
  });

  it("supports include relations", async () => {
    const db = track(
      await createDb({
        appId: "db-all-test",
        driver: { type: "persistent", dbName: uniqueDbName("include") },
      }),
    );

    const { id: orgId } = await db.insert(orgs, { name: "Acme" });
    const { id: teamId } = await db.insert(teams, {
      name: "Core",
      org_id: orgId,
      parent_id: undefined,
    });
    const { id: ownerId } = await db.insert(users, { name: "Owner", team_id: teamId });
    await db.insert(todos, {
      title: "with-owner-1",
      done: false,
      priority: 1,
      owner_id: ownerId,
      tags: ["x"],
    });
    await db.insert(todos, {
      title: "with-owner-2",
      done: true,
      priority: 2,
      owner_id: ownerId,
      tags: ["y"],
    });

    const rows = await db.all<User>(
      makeQuery<User>("users", {
        conditions: [{ column: "id", op: "eq", value: ownerId }],
        includes: { todosViaOwner: true },
      }),
    );

    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      id: ownerId,
      name: "Owner",
    });
    expect(rows[0].todosViaOwner).toHaveLength(2);
    expect(rows[0].todosViaOwner).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ title: "with-owner-1", owner_id: ownerId }),
        expect.objectContaining({ title: "with-owner-2", owner_id: ownerId }),
      ]),
    );
  });

  it("supports multi-hop queries", async () => {
    const db = track(
      await createDb({
        appId: "db-all-test",
        driver: { type: "persistent", dbName: uniqueDbName("hops") },
      }),
    );

    const { id: orgId } = await db.insert(orgs, { name: "Org A" });
    const { id: teamId } = await db.insert(teams, {
      name: "Team A",
      org_id: orgId,
      parent_id: undefined,
    });
    const { id: userId } = await db.insert(users, { name: "User A", team_id: teamId });

    const rows = await db.all<Org>(
      makeQuery<Org>("users", {
        conditions: [{ column: "id", op: "eq", value: userId }],
        hops: ["team", "org"],
      }),
    );

    expect(rows).toHaveLength(1);
    expect(rows[0]).toEqual({ id: orgId, name: "Org A" });
  });

  it("supports top-k hop queries ordered by joined-table columns", async () => {
    const db = track(
      await createDb({
        appId: "db-all-test",
        driver: { type: "persistent", dbName: uniqueDbName("join-topk-hop") },
      }),
    );

    const { id: teamAId } = await db.insert(teams, {
      name: "Alpha",
      org_id: undefined,
      parent_id: undefined,
    });
    const { id: teamBId } = await db.insert(teams, {
      name: "Gamma",
      org_id: undefined,
      parent_id: undefined,
    });
    const { id: teamCId } = await db.insert(teams, {
      name: "Beta",
      org_id: undefined,
      parent_id: undefined,
    });

    await db.insert(users, { name: "User A", team_id: teamAId });
    await db.insert(users, { name: "User B", team_id: teamBId });
    await db.insert(users, { name: "User C", team_id: teamCId });

    const rows = await db.all<Team>(
      makeQuery<Team>("users", {
        hops: ["team"],
        orderBy: [["__hop_0.name", "desc"]],
        limit: 2,
      }),
    );

    expect(rows.map((row) => ({ id: row.id, name: row.name }))).toEqual([
      { id: teamBId, name: "Gamma" },
      { id: teamCId, name: "Beta" },
    ]);
  });

  it("supports one-off all queries across scalar and UUID[] foreign-key hops", async () => {
    const db = track(
      await createDb({
        appId: "db-all-test",
        driver: { type: "persistent", dbName: uniqueDbName("fk-hops") },
      }),
    );

    const { id: orgId } = await db.insert(orgs, { name: "FK Org" });
    const { id: teamId } = await db.insert(teams, {
      name: "FK Team",
      org_id: orgId,
      parent_id: undefined,
    });
    const { id: userId } = await db.insert(users, { name: "FK User", team_id: teamId });

    const { id: partAId } = await db.insert(fileParts, { label: "A" });
    const { id: partBId } = await db.insert(fileParts, { label: "B" });
    const { id: fileId } = await db.insert(files, { name: "File 1", parts: [partBId, partAId] });

    const teamRows = await db.all<Team>(
      makeQuery<Team>("users", {
        conditions: [{ column: "id", op: "eq", value: userId }],
        hops: ["team"],
      }),
    );
    expect(teamRows).toHaveLength(1);
    expect(teamRows[0]).toMatchObject({ id: teamId, name: "FK Team" });

    const partRows = await db.all<FilePart>(
      makeQuery<FilePart>("files", {
        conditions: [{ column: "id", op: "eq", value: fileId }],
        hops: ["parts"],
      }),
    );
    expect(partRows).toHaveLength(2);
    expect(partRows.map((row) => row.label).sort()).toEqual(["A", "B"]);
  });

  it("supports gather queries", async () => {
    const db = track(
      await createDb({
        appId: "db-all-test",
        driver: { type: "persistent", dbName: uniqueDbName("gather") },
      }),
    );

    const { id: rootId } = await db.insert(teams, {
      name: "root",
      org_id: undefined,
      parent_id: undefined,
    });
    const { id: midId } = await db.insert(teams, {
      name: "mid",
      org_id: undefined,
      parent_id: rootId,
    });
    const { id: leafId } = await db.insert(teams, {
      name: "leaf",
      org_id: undefined,
      parent_id: midId,
    });

    const rows = await db.all<Team>(
      makeQuery<Team>("teams", {
        conditions: [{ column: "id", op: "eq", value: leafId }],
        gather: {
          max_depth: 10,
          step_table: "teams",
          step_current_column: "id",
          step_conditions: [],
          step_hops: ["parent"],
        },
      }),
    );

    const ids = rows.map((row) => row.id).sort();
    expect(ids).toEqual([leafId, midId, rootId].sort());
  });
});
