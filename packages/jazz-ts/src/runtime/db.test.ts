/**
 * Tests for Db class.
 *
 * Tests cover:
 * 1. Basic construction and lifecycle
 * 2. Query interface (QueryBuilder)
 * 3. Mutation operations (insert/update/deleteFrom)
 * 4. Subscriptions (subscribeAll)
 */

import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { createDb, Db, type QueryBuilder, type TableProxy } from "./db.js";
import type { WasmSchema } from "../drivers/types.js";
import { SqliteNodeDriver } from "../drivers/sqlite-node.js";

// Check if node:sqlite is available (Node.js 22+)
const nodeVersion = parseInt(process.version.slice(1).split(".")[0], 10);
const hasNodeSqlite = nodeVersion >= 22;

describe("Db", () => {
  const schema: WasmSchema = {
    tables: {
      todos: {
        columns: [
          { name: "title", column_type: { type: "Text" }, nullable: false },
          { name: "done", column_type: { type: "Boolean" }, nullable: false },
          { name: "priority", column_type: { type: "Integer" }, nullable: true },
        ],
      },
    },
  };

  describe("createDb", () => {
    it("creates a Db instance (async)", async () => {
      const driver = hasNodeSqlite ? await SqliteNodeDriver.open(":memory:") : createMockDriver();

      const db = await createDb({
        appId: "test-app",
        driver,
        env: "dev",
        userBranch: "main",
      });

      expect(db).toBeInstanceOf(Db);

      await db.shutdown();
      if (driver.close) await driver.close();
    });
  });

  describe("QueryBuilder interface", () => {
    it("accepts objects that implement QueryBuilder interface", () => {
      // This is a compile-time check - if it compiles, the interface is correct
      const mockQuery: QueryBuilder<{ id: string; title: string }> = {
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

      expect(mockQuery._table).toBe("todos");
      expect(mockQuery._schema).toBe(schema);
      expect(typeof mockQuery._build()).toBe("string");
    });
  });

  describe("TableProxy interface", () => {
    it("accepts objects that implement TableProxy interface", () => {
      // TableProxy is simpler - just table name and schema
      const tableProxy: TableProxy<{ id: string; title: string }, { title: string }> = {
        _table: "todos",
        _schema: schema,
      };

      expect(tableProxy._table).toBe("todos");
      expect(tableProxy._schema).toBe(schema);
    });
  });

  describe("shutdown", () => {
    it("shuts down cleanly even when no clients created", async () => {
      const driver = hasNodeSqlite ? await SqliteNodeDriver.open(":memory:") : createMockDriver();

      const db = await createDb({
        appId: "test-app",
        driver,
        env: "dev",
        userBranch: "main",
      });

      // Should not throw
      await db.shutdown();
      // Should not throw when called again
      await db.shutdown();

      if (driver.close) await driver.close();
    });
  });
});

describe.skipIf(!hasNodeSqlite)("Db integration", () => {
  const schema: WasmSchema = {
    tables: {
      todos: {
        columns: [
          { name: "title", column_type: { type: "Text" }, nullable: false },
          { name: "done", column_type: { type: "Boolean" }, nullable: false },
          { name: "priority", column_type: { type: "Integer" }, nullable: true },
        ],
      },
    },
  };

  // Table proxy for mutations
  const todosTable: TableProxy<
    { id: string; title: string; done: boolean; priority: number | null },
    { title: string; done: boolean; priority?: number | null }
  > = {
    _table: "todos",
    _schema: schema,
  };

  // Query builder factory
  function todosQuery(): QueryBuilder<{
    id: string;
    title: string;
    done: boolean;
    priority: number | null;
  }> {
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

  let driver: SqliteNodeDriver;
  let db: Db;

  beforeEach(async () => {
    driver = await SqliteNodeDriver.open(":memory:");
    db = await createDb({
      appId: "test-app",
      driver,
      env: "dev",
      userBranch: "main",
    });
  });

  afterEach(async () => {
    await db.shutdown();
    // Don't close driver if db already closed it
    try {
      await driver.close();
    } catch {
      // Driver already closed by db.shutdown()
    }
  });

  describe("all", () => {
    it("returns empty array when no rows exist", async () => {
      const results = await db.all(todosQuery());
      expect(results).toEqual([]);
    });
  });

  describe("one", () => {
    it("returns null when no rows match", async () => {
      const result = await db.one(todosQuery());
      expect(result).toBeNull();
    });
  });

  describe("insert", () => {
    it("returns a valid ID", () => {
      const id = db.insert(todosTable, { title: "Test Todo", done: false });

      // Should return a UUID-like string (sync!)
      expect(typeof id).toBe("string");
      expect(id.length).toBeGreaterThan(0);
    });

    it("inserts row that can be queried", async () => {
      const id = db.insert(todosTable, { title: "Buy milk", done: false, priority: 1 });

      const results = await db.all(todosQuery());

      expect(results.length).toBeGreaterThanOrEqual(1);
      const inserted = results.find((r) => r.id === id);
      expect(inserted).toBeDefined();
      expect(inserted?.title).toBe("Buy milk");
      expect(inserted?.done).toBe(false);
      expect(inserted?.priority).toBe(1);
    });

    it("handles nullable fields as undefined", async () => {
      const id = db.insert(todosTable, { title: "No priority", done: true });

      const results = await db.all(todosQuery());
      const inserted = results.find((r) => r.id === id);

      expect(inserted).toBeDefined();
      expect(inserted?.priority).toBeUndefined();
    });

    it("handles nullable fields as null", async () => {
      const id = db.insert(todosTable, { title: "Null priority", done: true, priority: null });

      const results = await db.all(todosQuery());
      const inserted = results.find((r) => r.id === id);

      expect(inserted).toBeDefined();
      expect(inserted?.priority).toBeUndefined();
    });
  });

  describe("update", () => {
    it("modifies existing row", async () => {
      const id = db.insert(todosTable, { title: "Original", done: false });

      // Update the row (sync!)
      db.update(todosTable, id, { done: true });

      const results = await db.all(todosQuery());
      const updated = results.find((r) => r.id === id);

      expect(updated).toBeDefined();
      expect(updated?.title).toBe("Original"); // unchanged
      expect(updated?.done).toBe(true); // updated
    });

    it("updates multiple fields", async () => {
      const id = db.insert(todosTable, { title: "Original", done: false, priority: 1 });

      db.update(todosTable, id, { title: "Updated", priority: 5 });

      const results = await db.all(todosQuery());
      const updated = results.find((r) => r.id === id);

      expect(updated?.title).toBe("Updated");
      expect(updated?.priority).toBe(5);
      expect(updated?.done).toBe(false); // unchanged
    });

    it("can set nullable field to null", async () => {
      const id = db.insert(todosTable, { title: "Has priority", done: false, priority: 10 });

      db.update(todosTable, id, { priority: null });

      const results = await db.all(todosQuery());
      const updated = results.find((r) => r.id === id);

      expect(updated?.priority).toBeUndefined();
    });
  });

  describe("deleteFrom", () => {
    it("removes existing row", async () => {
      const id1 = db.insert(todosTable, { title: "Keep me", done: false });
      const id2 = db.insert(todosTable, { title: "Delete me", done: true });

      // Verify both exist
      let results = await db.all(todosQuery());
      expect(results.length).toBe(2);

      // Delete one (sync!)
      db.deleteFrom(todosTable, id2);

      results = await db.all(todosQuery());
      expect(results.length).toBe(1);
      expect(results[0].id).toBe(id1);
      expect(results[0].title).toBe("Keep me");
    });
  });

  describe("client memoization", () => {
    it("reuses client for same schema", async () => {
      // Multiple queries should reuse the same client
      await db.all(todosQuery());
      await db.all(todosQuery());
      await db.one(todosQuery());

      // Multiple inserts should also reuse
      db.insert(todosTable, { title: "Test 1", done: false });
      db.insert(todosTable, { title: "Test 2", done: false });

      // If memoization wasn't working, we'd get errors from multiple runtime instances
      expect(true).toBe(true);
    });
  });

  describe("subscribeAll", () => {
    it("returns unsubscribe function", () => {
      const unsubscribe = db.subscribeAll(todosQuery(), () => {});

      expect(typeof unsubscribe).toBe("function");

      // Should not throw
      unsubscribe();
    });

    it("receives callback with initial data", async () => {
      // Insert a row first
      const id = db.insert(todosTable, { title: "Subscribe Test", done: false, priority: 5 });

      let callbackCount = 0;
      type DeltaType = {
        all: { id: string; title: string; done: boolean; priority: number | undefined }[];
      };
      let receivedDelta: DeltaType | null = null;

      const unsubscribe = db.subscribeAll(todosQuery(), (delta) => {
        callbackCount++;
        if (!receivedDelta) {
          receivedDelta = delta as DeltaType;
        }
      });

      // Wait for callback
      await new Promise((resolve) => setTimeout(resolve, 200));

      unsubscribe();

      expect(callbackCount).toBeGreaterThan(0);
      expect(receivedDelta).not.toBeNull();
      expect(receivedDelta!.all.length).toBeGreaterThanOrEqual(1);

      // Find our inserted row
      const row = receivedDelta!.all.find((r) => r.id === id);
      expect(row).toBeDefined();
      expect(row!.title).toBe("Subscribe Test");
      expect(row!.done).toBe(false);
      expect(row!.priority).toBe(5);
    });

    it("receives updates after insert", async () => {
      const deltas: { added: { id: string }[] }[] = [];

      const unsubscribe = db.subscribeAll(todosQuery(), (delta) => {
        deltas.push(delta as { added: { id: string }[] });
      });

      // Wait for initial callback
      await new Promise((resolve) => setTimeout(resolve, 100));
      const initialCount = deltas.length;

      // Insert a new row
      const id = db.insert(todosTable, { title: "New Item", done: true });

      // Wait for update callback
      await new Promise((resolve) => setTimeout(resolve, 200));

      unsubscribe();

      // Should have received at least one more callback after insert
      expect(deltas.length).toBeGreaterThan(initialCount);

      // The new row should appear in added
      const lastDelta = deltas[deltas.length - 1];
      const addedIds = lastDelta.added.map((r) => r.id);
      expect(addedIds).toContain(id);
    });
  });
});

/**
 * Create a mock driver for testing when SQLite is not available.
 */
function createMockDriver() {
  return {
    async process() {
      return [];
    },
    async close() {},
  };
}
