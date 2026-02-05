/**
 * E2E tests for generated client functionality.
 *
 * Tests cover:
 * 1. Persistence across Db restarts (the bug fix)
 * 2. Filtering operators (eq, ne, gt, gte, lt, lte, contains, in, isNull)
 * 3. Ordering and pagination
 * 4. Subscription deltas
 * 5. Basic CRUD operations
 *
 * Uses a rich schema with required/optional columns, multiple types, and relations.
 */

import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { createDb, Db, type QueryBuilder, type TableProxy } from "../runtime/db.js";
import type { WasmSchema } from "../drivers/types.js";
import { SqliteNodeDriver } from "../drivers/sqlite-node.js";
import * as fs from "node:fs";
import * as path from "node:path";
import * as os from "node:os";

// Check if node:sqlite is available (Node.js 22+)
const nodeVersion = parseInt(process.version.slice(1).split(".")[0], 10);
const hasNodeSqlite = nodeVersion >= 22;

/**
 * Rich schema for E2E testing.
 *
 * Tests:
 * - Required vs optional columns
 * - String, boolean, int types
 * - Foreign key references
 */
const schema: WasmSchema = {
  tables: {
    users: {
      columns: [
        { name: "name", column_type: { type: "Text" }, nullable: false },
        { name: "email", column_type: { type: "Text" }, nullable: false },
        { name: "age", column_type: { type: "Integer" }, nullable: true },
      ],
    },
    todos: {
      columns: [
        { name: "title", column_type: { type: "Text" }, nullable: false },
        { name: "done", column_type: { type: "Boolean" }, nullable: false },
        { name: "priority", column_type: { type: "Integer" }, nullable: false },
        { name: "description", column_type: { type: "Text" }, nullable: true },
        {
          name: "owner_id",
          column_type: { type: "Uuid" },
          nullable: false,
          references: "users",
        },
      ],
    },
  },
};

// Table proxies for mutations
const usersTable: TableProxy<
  { id: string; name: string; email: string; age: number | null },
  { name: string; email: string; age?: number | null }
> = {
  _table: "users",
  _schema: schema,
};

const todosTable: TableProxy<
  {
    id: string;
    title: string;
    done: boolean;
    priority: number;
    description: string | null;
    owner_id: string;
  },
  {
    title: string;
    done: boolean;
    priority: number;
    description?: string | null;
    owner_id: string;
  }
> = {
  _table: "todos",
  _schema: schema,
};

// QueryBuilder factories with filter support
type UserRow = { id: string; name: string; email: string; age?: number };
type TodoRow = {
  id: string;
  title: string;
  done: boolean;
  priority: number;
  description?: string;
  owner_id: string;
};

interface TodoConditions {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  title?: string | { eq?: string; ne?: string; contains?: string };
  done?: boolean;
  priority?:
    | number
    | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  description?: { isNull?: boolean } | string;
  owner_id?: string;
}

interface UserConditions {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  name?: string | { eq?: string; ne?: string; contains?: string };
  email?: string | { eq?: string; ne?: string; contains?: string };
  age?:
    | number
    | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number }
    | { isNull?: boolean };
}

/**
 * Create a TodoQueryBuilder that supports filtering, ordering, and pagination.
 */
function createTodosQuery(): {
  where(conditions: TodoConditions): QueryBuilder<TodoRow>;
  orderBy(column: keyof TodoRow, dir?: "asc" | "desc"): ReturnType<typeof createTodosQuery>;
  limit(n: number): ReturnType<typeof createTodosQuery>;
  offset(n: number): ReturnType<typeof createTodosQuery>;
  _table: string;
  _schema: WasmSchema;
  _build(): string;
} {
  let conditions: Array<{ column: string; op: string; value: unknown }> = [];
  let orderBys: Array<[string, "asc" | "desc"]> = [];
  let limitVal: number | undefined;
  let offsetVal: number | undefined;

  const builder = {
    _table: "todos",
    _schema: schema,

    where(conds: TodoConditions): QueryBuilder<TodoRow> {
      conditions = parseConditions(conds);
      return builder;
    },

    orderBy(column: keyof TodoRow, dir: "asc" | "desc" = "asc") {
      orderBys.push([column as string, dir]);
      return builder;
    },

    limit(n: number) {
      limitVal = n;
      return builder;
    },

    offset(n: number) {
      offsetVal = n;
      return builder;
    },

    _build(): string {
      return JSON.stringify({
        table: "todos",
        conditions,
        includes: {},
        orderBy: orderBys,
        limit: limitVal,
        offset: offsetVal,
      });
    },
  };

  return builder;
}

function createUsersQuery(): {
  where(conditions: UserConditions): QueryBuilder<UserRow>;
  orderBy(column: keyof UserRow, dir?: "asc" | "desc"): ReturnType<typeof createUsersQuery>;
  limit(n: number): ReturnType<typeof createUsersQuery>;
  offset(n: number): ReturnType<typeof createUsersQuery>;
  _table: string;
  _schema: WasmSchema;
  _build(): string;
} {
  let conditions: Array<{ column: string; op: string; value: unknown }> = [];
  let orderBys: Array<[string, "asc" | "desc"]> = [];
  let limitVal: number | undefined;
  let offsetVal: number | undefined;

  const builder = {
    _table: "users",
    _schema: schema,

    where(conds: UserConditions): QueryBuilder<UserRow> {
      conditions = parseConditions(conds);
      return builder;
    },

    orderBy(column: keyof UserRow, dir: "asc" | "desc" = "asc") {
      orderBys.push([column as string, dir]);
      return builder;
    },

    limit(n: number) {
      limitVal = n;
      return builder;
    },

    offset(n: number) {
      offsetVal = n;
      return builder;
    },

    _build(): string {
      return JSON.stringify({
        table: "users",
        conditions,
        includes: {},
        orderBy: orderBys,
        limit: limitVal,
        offset: offsetVal,
      });
    },
  };

  return builder;
}

/**
 * Parse condition object into array of condition tuples.
 */
function parseConditions(
  conds: Record<string, unknown>,
): Array<{ column: string; op: string; value: unknown }> {
  const result: Array<{ column: string; op: string; value: unknown }> = [];

  for (const [column, value] of Object.entries(conds)) {
    if (value === undefined) continue;

    if (typeof value === "object" && value !== null && !Array.isArray(value)) {
      // Object with operators: { eq: 1, gt: 0 }
      const ops = value as Record<string, unknown>;
      for (const [op, opValue] of Object.entries(ops)) {
        if (opValue !== undefined) {
          result.push({ column, op, value: opValue });
        }
      }
    } else {
      // Direct value: shorthand for eq
      result.push({ column, op: "eq", value });
    }
  }

  return result;
}

/**
 * Simple query factory for basic queries without filters.
 */
function todosQuery(): QueryBuilder<TodoRow> {
  return {
    _table: "todos",
    _schema: schema,
    _build: () =>
      JSON.stringify({
        table: "todos",
        conditions: [],
        includes: {},
        orderBy: [],
      }),
  };
}

function usersQuery(): QueryBuilder<UserRow> {
  return {
    _table: "users",
    _schema: schema,
    _build: () =>
      JSON.stringify({
        table: "users",
        conditions: [],
        includes: {},
        orderBy: [],
      }),
  };
}

// ===========================================================================
// Persistence Tests (THE BUG)
// ===========================================================================

describe.skipIf(!hasNodeSqlite)("E2E: Persistence", () => {
  let tmpDir: string;
  let dbPath: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "jazz-e2e-"));
    dbPath = path.join(tmpDir, "test.db");
  });

  afterEach(() => {
    // Cleanup temp files
    try {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    } catch {
      // Ignore cleanup errors
    }
  });

  it("persists data across Db restarts", async () => {
    // Create first Db instance and insert data
    const driver1 = await SqliteNodeDriver.open(dbPath);
    const db1 = await createDb({
      appId: "persist-test",
      driver: driver1,
      env: "dev",
      userBranch: "main",
    });

    // Insert a user
    const userId = db1.insert(usersTable, { name: "Alice", email: "alice@test.com" });

    // Insert a todo
    const todoId = db1.insert(todosTable, {
      title: "Test Todo",
      done: false,
      priority: 5,
      owner_id: userId,
    });

    // Verify data exists in first instance
    const todos1 = await db1.all(todosQuery());
    expect(todos1.length).toBe(1);
    expect(todos1[0].id).toBe(todoId);

    // Shutdown first instance
    await db1.shutdown();
    await driver1.close();

    // Wait a bit for file to be flushed
    await new Promise((resolve) => setTimeout(resolve, 100));

    // Create second Db instance with same file
    const driver2 = await SqliteNodeDriver.open(dbPath);
    const db2 = await createDb({
      appId: "persist-test",
      driver: driver2,
      env: "dev",
      userBranch: "main",
    });

    // Query should return previously inserted data
    const todos2 = await db2.all(todosQuery());
    expect(todos2.length).toBe(1);
    expect(todos2[0].id).toBe(todoId);
    expect(todos2[0].title).toBe("Test Todo");
    expect(todos2[0].done).toBe(false);
    expect(todos2[0].priority).toBe(5);

    const users2 = await db2.all(usersQuery());
    expect(users2.length).toBe(1);
    expect(users2[0].id).toBe(userId);
    expect(users2[0].name).toBe("Alice");

    await db2.shutdown();
    await driver2.close();
  });

  it("persists updates across restarts", async () => {
    // Create and insert
    const driver1 = await SqliteNodeDriver.open(dbPath);
    const db1 = await createDb({
      appId: "persist-update-test",
      driver: driver1,
      env: "dev",
      userBranch: "main",
    });

    const userId = db1.insert(usersTable, { name: "Bob", email: "bob@test.com", age: 25 });

    // Update the record
    db1.update(usersTable, userId, { age: 30 });

    // Verify update in first instance
    const users1 = await db1.all(usersQuery());
    expect(users1[0].age).toBe(30);

    await db1.shutdown();
    await driver1.close();

    await new Promise((resolve) => setTimeout(resolve, 100));

    // Reopen and verify
    const driver2 = await SqliteNodeDriver.open(dbPath);
    const db2 = await createDb({
      appId: "persist-update-test",
      driver: driver2,
      env: "dev",
      userBranch: "main",
    });

    const users2 = await db2.all(usersQuery());
    expect(users2.length).toBe(1);
    expect(users2[0].age).toBe(30);

    await db2.shutdown();
    await driver2.close();
  });

  it("persists deletes across restarts", async () => {
    const driver1 = await SqliteNodeDriver.open(dbPath);
    const db1 = await createDb({
      appId: "persist-delete-test",
      driver: driver1,
      env: "dev",
      userBranch: "main",
    });

    const id1 = db1.insert(usersTable, { name: "Keep", email: "keep@test.com" });
    const id2 = db1.insert(usersTable, { name: "Delete", email: "delete@test.com" });

    // Delete one
    db1.deleteFrom(usersTable, id2);

    // Verify
    const users1 = await db1.all(usersQuery());
    expect(users1.length).toBe(1);
    expect(users1[0].id).toBe(id1);

    await db1.shutdown();
    await driver1.close();

    await new Promise((resolve) => setTimeout(resolve, 100));

    // Reopen and verify
    const driver2 = await SqliteNodeDriver.open(dbPath);
    const db2 = await createDb({
      appId: "persist-delete-test",
      driver: driver2,
      env: "dev",
      userBranch: "main",
    });

    const users2 = await db2.all(usersQuery());
    expect(users2.length).toBe(1);
    expect(users2[0].id).toBe(id1);
    expect(users2[0].name).toBe("Keep");

    await db2.shutdown();
    await driver2.close();
  });
});

// ===========================================================================
// Filtering Tests
// ===========================================================================

describe.skipIf(!hasNodeSqlite)("E2E: Filtering", () => {
  let driver: SqliteNodeDriver;
  let db: Db;
  let userId: string;

  beforeEach(async () => {
    driver = await SqliteNodeDriver.open(":memory:");
    db = await createDb({
      appId: "filter-test",
      driver,
      env: "dev",
      userBranch: "main",
    });

    // Create test user
    userId = db.insert(usersTable, { name: "Test User", email: "test@test.com" });

    // Create test todos with varying priorities and titles
    db.insert(todosTable, {
      title: "First task",
      done: false,
      priority: 1,
      owner_id: userId,
    });
    db.insert(todosTable, {
      title: "Second task",
      done: true,
      priority: 5,
      description: "Has a description",
      owner_id: userId,
    });
    db.insert(todosTable, {
      title: "Third task",
      done: false,
      priority: 10,
      owner_id: userId,
    });
  });

  afterEach(async () => {
    await db.shutdown();
    try {
      await driver.close();
    } catch {
      // Already closed
    }
  });

  describe("equality operators", () => {
    it("filters by exact string match (eq)", async () => {
      const results = await db.all(createTodosQuery().where({ title: "First task" }));
      expect(results.length).toBe(1);
      expect(results[0].title).toBe("First task");
    });

    it("filters by not equal (ne)", async () => {
      const results = await db.all(createTodosQuery().where({ title: { ne: "First task" } }));
      expect(results.length).toBe(2);
      expect(results.every((r) => r.title !== "First task")).toBe(true);
    });

    it("filters by boolean", async () => {
      const results = await db.all(createTodosQuery().where({ done: true }));
      expect(results.length).toBe(1);
      expect(results[0].done).toBe(true);
    });
  });

  describe("numeric comparison operators", () => {
    it("filters by greater than (gt)", async () => {
      const results = await db.all(createTodosQuery().where({ priority: { gt: 5 } }));
      expect(results.length).toBe(1);
      expect(results[0].priority).toBe(10);
    });

    it("filters by greater than or equal (gte)", async () => {
      const results = await db.all(createTodosQuery().where({ priority: { gte: 5 } }));
      expect(results.length).toBe(2);
      expect(results.every((r) => r.priority >= 5)).toBe(true);
    });

    it("filters by less than (lt)", async () => {
      const results = await db.all(createTodosQuery().where({ priority: { lt: 5 } }));
      expect(results.length).toBe(1);
      expect(results[0].priority).toBe(1);
    });

    it("filters by less than or equal (lte)", async () => {
      const results = await db.all(createTodosQuery().where({ priority: { lte: 5 } }));
      expect(results.length).toBe(2);
      expect(results.every((r) => r.priority <= 5)).toBe(true);
    });

    it("combines gt and lte for range", async () => {
      const results = await db.all(createTodosQuery().where({ priority: { gt: 1, lte: 10 } }));
      expect(results.length).toBe(2);
      expect(results.every((r) => r.priority > 1 && r.priority <= 10)).toBe(true);
    });
  });

  describe("text search operators", () => {
    it("filters by contains", async () => {
      const results = await db.all(createTodosQuery().where({ title: { contains: "task" } }));
      expect(results.length).toBe(3);
    });

    it("filters by contains (partial match)", async () => {
      const results = await db.all(createTodosQuery().where({ title: { contains: "Second" } }));
      expect(results.length).toBe(1);
      expect(results[0].title).toBe("Second task");
    });
  });

  describe("set membership operators", () => {
    it("filters by in (array of ids)", async () => {
      const todos = await db.all(todosQuery());
      const ids = [todos[0].id, todos[2].id];

      const results = await db.all(createTodosQuery().where({ id: { in: ids } }));
      expect(results.length).toBe(2);
      expect(results.map((r) => r.id).sort()).toEqual(ids.sort());
    });
  });

  describe("null check operators", () => {
    it("filters by isNull: true", async () => {
      const results = await db.all(createTodosQuery().where({ description: { isNull: true } }));
      expect(results.length).toBe(2);
      expect(results.every((r) => r.description === undefined)).toBe(true);
    });
  });
});

// ===========================================================================
// Ordering and Pagination Tests
// ===========================================================================

describe.skipIf(!hasNodeSqlite)("E2E: Ordering and Pagination", () => {
  let driver: SqliteNodeDriver;
  let db: Db;
  let userId: string;

  beforeEach(async () => {
    driver = await SqliteNodeDriver.open(":memory:");
    db = await createDb({
      appId: "ordering-test",
      driver,
      env: "dev",
      userBranch: "main",
    });

    userId = db.insert(usersTable, { name: "Test User", email: "test@test.com" });

    // Create todos with specific priorities for predictable ordering
    for (let i = 1; i <= 10; i++) {
      db.insert(todosTable, {
        title: `Task ${i}`,
        done: i % 2 === 0,
        priority: i,
        owner_id: userId,
      });
    }
  });

  afterEach(async () => {
    await db.shutdown();
    try {
      await driver.close();
    } catch {
      // Already closed
    }
  });

  describe("orderBy", () => {
    it("orders by column ascending", async () => {
      const results = await db.all(createTodosQuery().orderBy("priority", "asc"));
      expect(results.length).toBe(10);
      for (let i = 0; i < 9; i++) {
        expect(results[i].priority).toBeLessThanOrEqual(results[i + 1].priority);
      }
    });

    it("orders by column descending", async () => {
      const results = await db.all(createTodosQuery().orderBy("priority", "desc"));
      expect(results.length).toBe(10);
      for (let i = 0; i < 9; i++) {
        expect(results[i].priority).toBeGreaterThanOrEqual(results[i + 1].priority);
      }
    });

    it("orders by title ascending", async () => {
      const results = await db.all(createTodosQuery().orderBy("title", "asc"));
      expect(results.length).toBe(10);
      // Task 1, Task 10, Task 2, Task 3... (string ordering)
      expect(results[0].title).toBe("Task 1");
      expect(results[1].title).toBe("Task 10");
    });
  });

  describe("limit", () => {
    it("limits results", async () => {
      const results = await db.all(createTodosQuery().limit(5));
      expect(results.length).toBe(5);
    });

    it("limit with orderBy returns top N", async () => {
      const results = await db.all(createTodosQuery().orderBy("priority", "desc").limit(3));
      expect(results.length).toBe(3);
      expect(results[0].priority).toBe(10);
      expect(results[1].priority).toBe(9);
      expect(results[2].priority).toBe(8);
    });
  });

  describe("offset", () => {
    it("skips first N results", async () => {
      const all = await db.all(createTodosQuery().orderBy("priority", "asc"));
      const offset = await db.all(createTodosQuery().orderBy("priority", "asc").offset(3));

      expect(offset.length).toBe(7);
      expect(offset[0].id).toBe(all[3].id);
    });

    it("combines offset and limit for pagination", async () => {
      const all = await db.all(createTodosQuery().orderBy("priority", "asc"));
      const page2 = await db.all(createTodosQuery().orderBy("priority", "asc").limit(3).offset(3));

      expect(page2.length).toBe(3);
      expect(page2[0].id).toBe(all[3].id);
      expect(page2[1].id).toBe(all[4].id);
      expect(page2[2].id).toBe(all[5].id);
    });
  });
});

// ===========================================================================
// Subscription Tests
// ===========================================================================

describe.skipIf(!hasNodeSqlite)("E2E: Subscriptions", () => {
  let driver: SqliteNodeDriver;
  let db: Db;
  let userId: string;

  beforeEach(async () => {
    driver = await SqliteNodeDriver.open(":memory:");
    db = await createDb({
      appId: "subscription-test",
      driver,
      env: "dev",
      userBranch: "main",
    });

    userId = db.insert(usersTable, { name: "Test User", email: "test@test.com" });
  });

  afterEach(async () => {
    await db.shutdown();
    try {
      await driver.close();
    } catch {
      // Already closed
    }
  });

  it("receives initial data in all array", async () => {
    // Insert before subscribing
    const id = db.insert(todosTable, {
      title: "Existing",
      done: false,
      priority: 1,
      owner_id: userId,
    });

    type DeltaType = {
      all: TodoRow[];
      added: TodoRow[];
      updated: [TodoRow, TodoRow][];
      removed: TodoRow[];
    };
    let receivedDelta: DeltaType | null = null;

    const unsubscribe = db.subscribeAll(todosQuery(), (delta) => {
      if (!receivedDelta) {
        receivedDelta = delta as DeltaType;
      }
    });

    await new Promise((resolve) => setTimeout(resolve, 200));
    unsubscribe();

    expect(receivedDelta).not.toBeNull();
    expect(receivedDelta!.all.length).toBe(1);
    expect(receivedDelta!.all[0].id).toBe(id);
  });

  it("receives added items after insert", async () => {
    const deltas: { all: TodoRow[]; added: TodoRow[] }[] = [];

    const unsubscribe = db.subscribeAll(todosQuery(), (delta) => {
      deltas.push(delta as { all: TodoRow[]; added: TodoRow[] });
    });

    // Wait for initial callback
    await new Promise((resolve) => setTimeout(resolve, 100));
    const initialCount = deltas.length;

    // Insert new item
    const id = db.insert(todosTable, {
      title: "New Item",
      done: false,
      priority: 5,
      owner_id: userId,
    });

    // Wait for update
    await new Promise((resolve) => setTimeout(resolve, 200));
    unsubscribe();

    expect(deltas.length).toBeGreaterThan(initialCount);

    // Find the delta with our new item
    const addedDelta = deltas.find((d) => d.added.some((item) => item.id === id));
    expect(addedDelta).toBeDefined();
    expect(addedDelta!.added.find((item) => item.id === id)!.title).toBe("New Item");
  });

  it("receives updated items after update", async () => {
    // Insert item first
    const id = db.insert(todosTable, {
      title: "To Update",
      done: false,
      priority: 1,
      owner_id: userId,
    });

    const deltas: { updated: [TodoRow, TodoRow][] }[] = [];

    const unsubscribe = db.subscribeAll(todosQuery(), (delta) => {
      deltas.push(delta as { updated: [TodoRow, TodoRow][] });
    });

    await new Promise((resolve) => setTimeout(resolve, 100));
    const initialCount = deltas.length;

    // Update the item
    db.update(todosTable, id, { done: true });

    await new Promise((resolve) => setTimeout(resolve, 200));
    unsubscribe();

    expect(deltas.length).toBeGreaterThan(initialCount);

    // Find delta with our update
    const updateDelta = deltas.find((d) => d.updated.some(([_, newRow]) => newRow.id === id));
    expect(updateDelta).toBeDefined();

    const [oldRow, newRow] = updateDelta!.updated.find(([_, row]) => row.id === id)!;
    expect(oldRow.done).toBe(false);
    expect(newRow.done).toBe(true);
  });

  it("receives removed items after delete", async () => {
    // Insert item first
    const id = db.insert(todosTable, {
      title: "To Delete",
      done: false,
      priority: 1,
      owner_id: userId,
    });

    const deltas: { removed: TodoRow[] }[] = [];

    const unsubscribe = db.subscribeAll(todosQuery(), (delta) => {
      deltas.push(delta as { removed: TodoRow[] });
    });

    await new Promise((resolve) => setTimeout(resolve, 100));
    const initialCount = deltas.length;

    // Delete the item
    db.deleteFrom(todosTable, id);

    await new Promise((resolve) => setTimeout(resolve, 200));
    unsubscribe();

    expect(deltas.length).toBeGreaterThan(initialCount);

    // Find delta with our delete
    const removeDelta = deltas.find((d) => d.removed.some((item) => item.id === id));
    expect(removeDelta).toBeDefined();
    expect(removeDelta!.removed.find((item) => item.id === id)!.title).toBe("To Delete");
  });

  it("all array always contains current full result set", async () => {
    const allArrays: TodoRow[][] = [];

    const unsubscribe = db.subscribeAll(todosQuery(), (delta) => {
      allArrays.push([...(delta as { all: TodoRow[] }).all]);
    });

    await new Promise((resolve) => setTimeout(resolve, 100));

    // Insert multiple items
    const id1 = db.insert(todosTable, {
      title: "Item 1",
      done: false,
      priority: 1,
      owner_id: userId,
    });
    await new Promise((resolve) => setTimeout(resolve, 100));

    const id2 = db.insert(todosTable, {
      title: "Item 2",
      done: true,
      priority: 2,
      owner_id: userId,
    });
    await new Promise((resolve) => setTimeout(resolve, 100));

    db.deleteFrom(todosTable, id1);
    await new Promise((resolve) => setTimeout(resolve, 100));

    unsubscribe();

    // Last all array should contain only id2
    const finalAll = allArrays[allArrays.length - 1];
    expect(finalAll.length).toBe(1);
    expect(finalAll[0].id).toBe(id2);
  });
});

// ===========================================================================
// Basic CRUD Tests (expanded coverage)
// ===========================================================================

describe.skipIf(!hasNodeSqlite)("E2E: CRUD Operations", () => {
  let driver: SqliteNodeDriver;
  let db: Db;
  let userId: string;

  beforeEach(async () => {
    driver = await SqliteNodeDriver.open(":memory:");
    db = await createDb({
      appId: "crud-test",
      driver,
      env: "dev",
      userBranch: "main",
    });

    userId = db.insert(usersTable, { name: "Test User", email: "test@test.com" });
  });

  afterEach(async () => {
    await db.shutdown();
    try {
      await driver.close();
    } catch {
      // Already closed
    }
  });

  describe("insert", () => {
    it("returns a valid UUID", () => {
      const id = db.insert(todosTable, {
        title: "Test",
        done: false,
        priority: 1,
        owner_id: userId,
      });

      expect(typeof id).toBe("string");
      expect(id.length).toBe(36); // UUID format
      expect(id).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i);
    });

    it("handles all column types", async () => {
      const id = db.insert(todosTable, {
        title: "Full Test", // string
        done: true, // boolean
        priority: 42, // int
        description: "A description", // optional string
        owner_id: userId, // uuid (FK)
      });

      const results = await db.all(todosQuery());
      const row = results.find((r) => r.id === id);

      expect(row).toBeDefined();
      expect(row!.title).toBe("Full Test");
      expect(row!.done).toBe(true);
      expect(row!.priority).toBe(42);
      expect(row!.description).toBe("A description");
      expect(row!.owner_id).toBe(userId);
    });

    it("handles optional fields as undefined", async () => {
      const id = db.insert(todosTable, {
        title: "No Description",
        done: false,
        priority: 1,
        owner_id: userId,
        // description omitted
      });

      const results = await db.all(todosQuery());
      const row = results.find((r) => r.id === id);

      expect(row).toBeDefined();
      expect(row!.description).toBeUndefined();
    });

    it("handles optional fields as null", async () => {
      const id = db.insert(todosTable, {
        title: "Null Description",
        done: false,
        priority: 1,
        owner_id: userId,
        description: null,
      });

      const results = await db.all(todosQuery());
      const row = results.find((r) => r.id === id);

      expect(row).toBeDefined();
      expect(row!.description).toBeUndefined();
    });
  });

  describe("update", () => {
    it("updates single field", async () => {
      const id = db.insert(todosTable, {
        title: "Original",
        done: false,
        priority: 1,
        owner_id: userId,
      });

      db.update(todosTable, id, { title: "Updated" });

      const results = await db.all(todosQuery());
      const row = results.find((r) => r.id === id);

      expect(row!.title).toBe("Updated");
      expect(row!.done).toBe(false); // unchanged
      expect(row!.priority).toBe(1); // unchanged
    });

    it("updates multiple fields", async () => {
      const id = db.insert(todosTable, {
        title: "Original",
        done: false,
        priority: 1,
        owner_id: userId,
      });

      db.update(todosTable, id, { title: "Updated", done: true, priority: 10 });

      const results = await db.all(todosQuery());
      const row = results.find((r) => r.id === id);

      expect(row!.title).toBe("Updated");
      expect(row!.done).toBe(true);
      expect(row!.priority).toBe(10);
    });

    it("sets optional field to null", async () => {
      const id = db.insert(todosTable, {
        title: "Has Description",
        done: false,
        priority: 1,
        description: "Original description",
        owner_id: userId,
      });

      db.update(todosTable, id, { description: null });

      const results = await db.all(todosQuery());
      const row = results.find((r) => r.id === id);

      expect(row!.description).toBeUndefined();
    });
  });

  describe("delete", () => {
    it("removes row from results", async () => {
      const id1 = db.insert(todosTable, {
        title: "Keep",
        done: false,
        priority: 1,
        owner_id: userId,
      });
      const id2 = db.insert(todosTable, {
        title: "Delete",
        done: false,
        priority: 2,
        owner_id: userId,
      });

      let results = await db.all(todosQuery());
      expect(results.length).toBe(2);

      db.deleteFrom(todosTable, id2);

      results = await db.all(todosQuery());
      expect(results.length).toBe(1);
      expect(results[0].id).toBe(id1);
    });

    it("deleting non-existent row is a no-op", async () => {
      const id = db.insert(todosTable, {
        title: "Test",
        done: false,
        priority: 1,
        owner_id: userId,
      });

      // Delete it
      db.deleteFrom(todosTable, id);

      // Delete again - should not throw
      db.deleteFrom(todosTable, id);

      const results = await db.all(todosQuery());
      expect(results.length).toBe(0);
    });
  });

  describe("one", () => {
    it("returns first matching row", async () => {
      db.insert(todosTable, { title: "First", done: false, priority: 1, owner_id: userId });
      db.insert(todosTable, { title: "Second", done: false, priority: 2, owner_id: userId });

      const result = await db.one(todosQuery());
      expect(result).not.toBeNull();
      expect(["First", "Second"]).toContain(result!.title);
    });

    it("returns null when no rows match", async () => {
      const result = await db.one(todosQuery());
      expect(result).toBeNull();
    });
  });
});
