/**
 * Tests for JazzClient.
 *
 * Uses the real WASM runtime with an in-memory SQLite driver.
 */

import { describe, it, expect, beforeEach, afterEach } from "vitest";
import type { WasmSchema, Value } from "../drivers/types.js";
import { JazzClient } from "./client.js";
import type { AppContext } from "./context.js";
import { SqliteNodeDriver } from "../drivers/sqlite-node.js";

// Check if node:sqlite is available (Node.js 22+)
const nodeVersion = parseInt(process.version.slice(1).split(".")[0], 10);
const hasNodeSqlite = nodeVersion >= 22;

describe.skipIf(!hasNodeSqlite)("JazzClient", () => {
  const schema: WasmSchema = {
    tables: {
      todos: {
        columns: [
          { name: "title", column_type: { type: "Text" }, nullable: false },
          { name: "completed", column_type: { type: "Boolean" }, nullable: false },
        ],
      },
    },
  };

  let driver: SqliteNodeDriver;

  const createContext = async (): Promise<AppContext> => {
    driver = await SqliteNodeDriver.open(":memory:");
    return {
      appId: "test-app",
      schema,
      driver,
      env: "dev",
      userBranch: "main",
    };
  };

  describe("connect", () => {
    it("creates a client with the provided context", async () => {
      const context = await createContext();
      const client = await JazzClient.connect(context);

      expect(client).toBeInstanceOf(JazzClient);
      await client.shutdown();
    });
  });

  describe("CRUD operations", () => {
    let client: JazzClient;

    beforeEach(async () => {
      const context = await createContext();
      client = await JazzClient.connect(context);
    });

    afterEach(async () => {
      await client.shutdown();
    });

    it("creates a row and returns a valid ID", async () => {
      const values: Value[] = [
        { type: "Text", value: "Test Todo" },
        { type: "Boolean", value: false },
      ];

      const id = await client.create("todos", values);

      // Should return a UUID-like string
      expect(typeof id).toBe("string");
      expect(id.length).toBeGreaterThan(0);
    });

    it("queries rows after creating", async () => {
      // Create a todo first
      const values: Value[] = [
        { type: "Text", value: "Query Test" },
        { type: "Boolean", value: true },
      ];
      await client.create("todos", values);

      // Note: Don't specify branches - let the runtime use the schema context's branches
      // which include the composed branch name (e.g., "dev-{hash}-main")
      const queryJson = JSON.stringify({
        table: "todos",
        branches: [],
        disjuncts: [{ conditions: [] }],
        order_by: [],
        offset: 0,
        include_deleted: false,
        array_subqueries: [],
        joins: [],
      });

      const rows = await client.query(queryJson);

      expect(rows.length).toBeGreaterThanOrEqual(1);
    });
  });

  describe("schema", () => {
    it("returns the current schema", async () => {
      const context = await createContext();
      const client = await JazzClient.connect(context);

      const returnedSchema = client.getSchema();

      expect(returnedSchema).toBeDefined();
      expect(returnedSchema.tables).toBeDefined();

      await client.shutdown();
    });
  });

  describe("shutdown", () => {
    it("shuts down cleanly", async () => {
      const context = await createContext();
      const client = await JazzClient.connect(context);

      // Should not throw
      await client.shutdown();
    });
  });
});
