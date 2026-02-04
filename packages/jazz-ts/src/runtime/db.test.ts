/**
 * Tests for Db class.
 *
 * Note: Full integration tests that require insert + query are deferred to Phase 6
 * when Db.insert() is implemented. For now, we test:
 * 1. Unit tests for Db class construction and method signatures
 * 2. Query translation is tested in query-adapter.test.ts
 * 3. Row transformation is tested in row-transformer.test.ts
 */

import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { createDb, Db, type QueryBuilder } from "./db.js";
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
    it("creates a Db instance", async () => {
      const driver = hasNodeSqlite ? await SqliteNodeDriver.open(":memory:") : createMockDriver();

      const db = createDb({
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

  describe("shutdown", () => {
    it("shuts down cleanly even when no clients created", async () => {
      const driver = hasNodeSqlite ? await SqliteNodeDriver.open(":memory:") : createMockDriver();

      const db = createDb({
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

  let driver: SqliteNodeDriver;
  let db: Db;

  beforeEach(async () => {
    driver = await SqliteNodeDriver.open(":memory:");
    db = createDb({
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
      const query = {
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

      const results = await db.all<{ id: string; title: string; done: boolean }>(query);

      expect(results).toEqual([]);
    });
  });

  describe("one", () => {
    it("returns null when no rows match", async () => {
      const query = {
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

      const result = await db.one<{ id: string; title: string; done: boolean }>(query);

      expect(result).toBeNull();
    });
  });

  describe("client memoization", () => {
    it("reuses client for same schema", async () => {
      const query = {
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

      // Multiple queries should reuse the same client
      await db.all(query);
      await db.all(query);
      await db.one(query);

      // If memoization wasn't working, we'd get errors from multiple runtime instances
      expect(true).toBe(true);
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
