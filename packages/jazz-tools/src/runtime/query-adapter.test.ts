/**
 * Tests for query-adapter.
 */

import { describe, it, expect } from "vitest";
import { translateQuery } from "./query-adapter.js";
import type { WasmSchema } from "../drivers/types.js";

describe("translateQuery", () => {
  const basicSchema: WasmSchema = {
    tables: {
      todos: {
        columns: [
          { name: "title", column_type: { type: "Text" }, nullable: false },
          { name: "done", column_type: { type: "Boolean" }, nullable: false },
          { name: "priority", column_type: { type: "Integer" }, nullable: true },
          { name: "project", column_type: { type: "Uuid" }, nullable: true },
          {
            name: "tags",
            column_type: { type: "Array", element: { type: "Text" } },
            nullable: false,
          },
          { name: "created_at", column_type: { type: "Timestamp" }, nullable: true },
        ],
      },
    },
  };

  describe("basic query structure", () => {
    it("translates empty query", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [],
        includes: {},
        orderBy: [],
      });

      const result = JSON.parse(translateQuery(builderJson, basicSchema));

      expect(result.table).toBe("todos");
      expect(result.branches).toEqual([]);
      expect(result.disjuncts).toEqual([{ conditions: [] }]);
      expect(result.order_by).toEqual([]);
      expect(result.offset).toBe(0);
      expect(result.limit).toBeNull();
      expect(result.include_deleted).toBe(false);
      expect(result.array_subqueries).toEqual([]);
      expect(result.joins).toEqual([]);
    });

    it("translates limit and offset", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [],
        includes: {},
        orderBy: [],
        limit: 10,
        offset: 5,
      });

      const result = JSON.parse(translateQuery(builderJson, basicSchema));

      expect(result.limit).toBe(10);
      expect(result.offset).toBe(5);
    });
  });

  describe("condition translation", () => {
    it("translates eq condition with string", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [{ column: "title", op: "eq", value: "Buy milk" }],
        includes: {},
        orderBy: [],
      });

      const result = JSON.parse(translateQuery(builderJson, basicSchema));

      expect(result.disjuncts[0].conditions).toEqual([
        { Eq: { column: "title", value: { Text: "Buy milk" } } },
      ]);
    });

    it("translates eq condition with UUID string for Uuid columns", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [
          { column: "project", op: "eq", value: "00000000-0000-0000-0000-000000000123" },
        ],
        includes: {},
        orderBy: [],
      });

      const result = JSON.parse(translateQuery(builderJson, basicSchema));

      expect(result.disjuncts[0].conditions).toEqual([
        { Eq: { column: "project", value: { Uuid: "00000000-0000-0000-0000-000000000123" } } },
      ]);
    });

    it("treats implicit id column as UUID", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [{ column: "id", op: "eq", value: "00000000-0000-0000-0000-000000000abc" }],
        includes: {},
        orderBy: [],
      });

      const result = JSON.parse(translateQuery(builderJson, basicSchema));

      expect(result.disjuncts[0].conditions).toEqual([
        { Eq: { column: "_id", value: { Uuid: "00000000-0000-0000-0000-000000000abc" } } },
      ]);
    });

    it("translates eq condition with boolean", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [{ column: "done", op: "eq", value: false }],
        includes: {},
        orderBy: [],
      });

      const result = JSON.parse(translateQuery(builderJson, basicSchema));

      expect(result.disjuncts[0].conditions).toEqual([
        { Eq: { column: "done", value: { Boolean: false } } },
      ]);
    });

    it("translates eq condition with number", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [{ column: "priority", op: "eq", value: 5 }],
        includes: {},
        orderBy: [],
      });

      const result = JSON.parse(translateQuery(builderJson, basicSchema));

      expect(result.disjuncts[0].conditions).toEqual([
        { Eq: { column: "priority", value: { Integer: 5 } } },
      ]);
    });

    it("translates eq condition with number for Timestamp columns", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [{ column: "created_at", op: "eq", value: 1712345678 }],
        includes: {},
        orderBy: [],
      });

      const result = JSON.parse(translateQuery(builderJson, basicSchema));

      expect(result.disjuncts[0].conditions).toEqual([
        { Eq: { column: "created_at", value: { Timestamp: 1712345678 } } },
      ]);
    });

    it("translates eq condition with array value", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [
          {
            column: "tags",
            op: "eq",
            value: ["tag1", "tag2"],
          },
        ],
        includes: {},
        orderBy: [],
      });

      const result = JSON.parse(translateQuery(builderJson, basicSchema));

      expect(result.disjuncts[0].conditions).toEqual([
        {
          Eq: {
            column: "tags",
            value: {
              Array: [{ Text: "tag1" }, { Text: "tag2" }],
            },
          },
        },
      ]);
    });

    it("translates contains condition with array element value", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [
          {
            column: "tags",
            op: "contains",
            value: "tag1",
          },
        ],
        includes: {},
        orderBy: [],
      });

      const result = JSON.parse(translateQuery(builderJson, basicSchema));

      expect(result.disjuncts[0].conditions).toEqual([
        {
          Contains: {
            column: "tags",
            value: { Text: "tag1" },
          },
        },
      ]);
    });

    it("translates ne condition", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [{ column: "done", op: "ne", value: true }],
        includes: {},
        orderBy: [],
      });

      const result = JSON.parse(translateQuery(builderJson, basicSchema));

      expect(result.disjuncts[0].conditions).toEqual([
        { Ne: { column: "done", value: { Boolean: true } } },
      ]);
    });

    it("translates gt condition", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [{ column: "priority", op: "gt", value: 3 }],
        includes: {},
        orderBy: [],
      });

      const result = JSON.parse(translateQuery(builderJson, basicSchema));

      expect(result.disjuncts[0].conditions).toEqual([
        { Gt: { column: "priority", value: { Integer: 3 } } },
      ]);
    });

    it("translates gte condition", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [{ column: "priority", op: "gte", value: 3 }],
        includes: {},
        orderBy: [],
      });

      const result = JSON.parse(translateQuery(builderJson, basicSchema));

      expect(result.disjuncts[0].conditions).toEqual([
        { Ge: { column: "priority", value: { Integer: 3 } } },
      ]);
    });

    it("translates lt condition", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [{ column: "priority", op: "lt", value: 3 }],
        includes: {},
        orderBy: [],
      });

      const result = JSON.parse(translateQuery(builderJson, basicSchema));

      expect(result.disjuncts[0].conditions).toEqual([
        { Lt: { column: "priority", value: { Integer: 3 } } },
      ]);
    });

    it("translates lte condition", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [{ column: "priority", op: "lte", value: 3 }],
        includes: {},
        orderBy: [],
      });

      const result = JSON.parse(translateQuery(builderJson, basicSchema));

      expect(result.disjuncts[0].conditions).toEqual([
        { Le: { column: "priority", value: { Integer: 3 } } },
      ]);
    });

    it("translates isNull condition", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [{ column: "priority", op: "isNull", value: true }],
        includes: {},
        orderBy: [],
      });

      const result = JSON.parse(translateQuery(builderJson, basicSchema));

      expect(result.disjuncts[0].conditions).toEqual([{ IsNull: { column: "priority" } }]);
    });

    it("translates multiple conditions", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [
          { column: "done", op: "eq", value: false },
          { column: "priority", op: "gt", value: 3 },
        ],
        includes: {},
        orderBy: [],
      });

      const result = JSON.parse(translateQuery(builderJson, basicSchema));

      expect(result.disjuncts[0].conditions).toHaveLength(2);
      expect(result.disjuncts[0].conditions[0]).toEqual({
        Eq: { column: "done", value: { Boolean: false } },
      });
      expect(result.disjuncts[0].conditions[1]).toEqual({
        Gt: { column: "priority", value: { Integer: 3 } },
      });
    });

    it("translates null value", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [{ column: "priority", op: "eq", value: null }],
        includes: {},
        orderBy: [],
      });

      const result = JSON.parse(translateQuery(builderJson, basicSchema));

      expect(result.disjuncts[0].conditions).toEqual([
        { Eq: { column: "priority", value: { Null: null } } },
      ]);
    });

    it("throws for unknown operator", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [{ column: "done", op: "unknown", value: true }],
        includes: {},
        orderBy: [],
      });

      expect(() => translateQuery(builderJson, basicSchema)).toThrow("Unknown operator: unknown");
    });
  });

  describe("orderBy translation", () => {
    it("translates ascending order", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [],
        includes: {},
        orderBy: [["priority", "asc"]],
      });

      const result = JSON.parse(translateQuery(builderJson, basicSchema));

      expect(result.order_by).toEqual([["priority", "Ascending"]]);
    });

    it("translates descending order", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [],
        includes: {},
        orderBy: [["priority", "desc"]],
      });

      const result = JSON.parse(translateQuery(builderJson, basicSchema));

      expect(result.order_by).toEqual([["priority", "Descending"]]);
    });

    it("translates multiple orderBy clauses", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [],
        includes: {},
        orderBy: [
          ["priority", "desc"],
          ["title", "asc"],
        ],
      });

      const result = JSON.parse(translateQuery(builderJson, basicSchema));

      expect(result.order_by).toEqual([
        ["priority", "Descending"],
        ["title", "Ascending"],
      ]);
    });
  });

  describe("include translation", () => {
    const schemaWithRelations: WasmSchema = {
      tables: {
        todos: {
          columns: [
            { name: "title", column_type: { type: "Text" }, nullable: false },
            {
              name: "owner_id",
              column_type: { type: "Uuid" },
              nullable: false,
              references: "users",
            },
          ],
        },
        users: {
          columns: [{ name: "name", column_type: { type: "Text" }, nullable: false }],
        },
      },
    };

    it("translates forward relation include", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [],
        includes: { owner: true },
        orderBy: [],
      });

      const result = JSON.parse(translateQuery(builderJson, schemaWithRelations));

      expect(result.array_subqueries).toEqual([
        {
          column_name: "owner",
          table: "users",
          inner_column: "id",
          outer_column: "todos.owner_id",
          filters: [],
          joins: [],
          select_columns: null,
          order_by: [],
          limit: null,
          nested_arrays: [],
        },
      ]);
    });

    it("translates reverse relation include", () => {
      const builderJson = JSON.stringify({
        table: "users",
        conditions: [],
        includes: { todosViaOwner: true },
        orderBy: [],
      });

      const result = JSON.parse(translateQuery(builderJson, schemaWithRelations));

      expect(result.array_subqueries).toEqual([
        {
          column_name: "todosViaOwner",
          table: "todos",
          inner_column: "owner_id",
          outer_column: "users.id",
          filters: [],
          joins: [],
          select_columns: null,
          order_by: [],
          limit: null,
          nested_arrays: [],
        },
      ]);
    });

    it("skips false includes", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [],
        includes: { owner: false },
        orderBy: [],
      });

      const result = JSON.parse(translateQuery(builderJson, schemaWithRelations));

      expect(result.array_subqueries).toEqual([]);
    });

    it("throws for unknown relation", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [],
        includes: { nonexistent: true },
        orderBy: [],
      });

      expect(() => translateQuery(builderJson, schemaWithRelations)).toThrow(
        'Unknown relation "nonexistent" on table "todos"',
      );
    });

    it("translates nested includes", () => {
      const nestedSchema: WasmSchema = {
        tables: {
          comments: {
            columns: [
              { name: "text", column_type: { type: "Text" }, nullable: false },
              {
                name: "todo_id",
                column_type: { type: "Uuid" },
                nullable: false,
                references: "todos",
              },
            ],
          },
          todos: {
            columns: [
              { name: "title", column_type: { type: "Text" }, nullable: false },
              {
                name: "owner_id",
                column_type: { type: "Uuid" },
                nullable: false,
                references: "users",
              },
            ],
          },
          users: {
            columns: [{ name: "name", column_type: { type: "Text" }, nullable: false }],
          },
        },
      };

      const builderJson = JSON.stringify({
        table: "comments",
        conditions: [],
        includes: {
          todo: {
            owner: true,
          },
        },
        orderBy: [],
      });

      const result = JSON.parse(translateQuery(builderJson, nestedSchema));

      expect(result.array_subqueries).toEqual([
        {
          column_name: "todo",
          table: "todos",
          inner_column: "id",
          outer_column: "comments.todo_id",
          filters: [],
          joins: [],
          select_columns: null,
          order_by: [],
          limit: null,
          nested_arrays: [
            {
              column_name: "owner",
              table: "users",
              inner_column: "id",
              outer_column: "todos.owner_id",
              filters: [],
              joins: [],
              select_columns: null,
              order_by: [],
              limit: null,
              nested_arrays: [],
            },
          ],
        },
      ]);
    });
  });

  describe("self-referential relations", () => {
    const selfRefSchema: WasmSchema = {
      tables: {
        todos: {
          columns: [
            { name: "title", column_type: { type: "Text" }, nullable: false },
            {
              name: "parent_id",
              column_type: { type: "Uuid" },
              nullable: true,
              references: "todos",
            },
          ],
        },
      },
    };

    it("translates forward self-reference", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [],
        includes: { parent: true },
        orderBy: [],
      });

      const result = JSON.parse(translateQuery(builderJson, selfRefSchema));

      expect(result.array_subqueries).toEqual([
        {
          column_name: "parent",
          table: "todos",
          inner_column: "id",
          outer_column: "todos.parent_id",
          filters: [],
          joins: [],
          select_columns: null,
          order_by: [],
          limit: null,
          nested_arrays: [],
        },
      ]);
    });

    it("translates reverse self-reference", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [],
        includes: { todosViaParent: true },
        orderBy: [],
      });

      const result = JSON.parse(translateQuery(builderJson, selfRefSchema));

      expect(result.array_subqueries).toEqual([
        {
          column_name: "todosViaParent",
          table: "todos",
          inner_column: "parent_id",
          outer_column: "todos.id",
          filters: [],
          joins: [],
          select_columns: null,
          order_by: [],
          limit: null,
          nested_arrays: [],
        },
      ]);
    });
  });

  describe("full query translation", () => {
    const fullSchema: WasmSchema = {
      tables: {
        todos: {
          columns: [
            { name: "title", column_type: { type: "Text" }, nullable: false },
            { name: "done", column_type: { type: "Boolean" }, nullable: false },
            { name: "priority", column_type: { type: "Integer" }, nullable: true },
            {
              name: "owner_id",
              column_type: { type: "Uuid" },
              nullable: false,
              references: "users",
            },
          ],
        },
        users: {
          columns: [{ name: "name", column_type: { type: "Text" }, nullable: false }],
        },
      },
    };

    it("translates complex query", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [
          { column: "done", op: "eq", value: false },
          { column: "priority", op: "gte", value: 3 },
        ],
        includes: { owner: true },
        orderBy: [
          ["priority", "desc"],
          ["title", "asc"],
        ],
        limit: 10,
        offset: 5,
      });

      const result = JSON.parse(translateQuery(builderJson, fullSchema));

      expect(result).toEqual({
        table: "todos",
        branches: [],
        disjuncts: [
          {
            conditions: [
              { Eq: { column: "done", value: { Boolean: false } } },
              { Ge: { column: "priority", value: { Integer: 3 } } },
            ],
          },
        ],
        order_by: [
          ["priority", "Descending"],
          ["title", "Ascending"],
        ],
        offset: 5,
        limit: 10,
        include_deleted: false,
        array_subqueries: [
          {
            column_name: "owner",
            table: "users",
            inner_column: "id",
            outer_column: "todos.owner_id",
            filters: [],
            joins: [],
            select_columns: null,
            order_by: [],
            limit: null,
            nested_arrays: [],
          },
        ],
        joins: [],
      });
    });
  });

  describe("error handling", () => {
    it("throws for unknown column", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [{ column: "unknown", op: "eq", value: "test" }],
      });

      expect(() => translateQuery(builderJson, basicSchema)).toThrow(
        'Unknown column "unknown" in table "todos"',
      );
    });

    it("throws for array value in scalar column", () => {
      const builderJson = JSON.stringify({
        table: "todos",
        conditions: [{ column: "title", op: "eq", value: ["test"] }],
      });

      expect(() => translateQuery(builderJson, basicSchema)).toThrow(
        "Unexpected array value for scalar column",
      );
    });
  });
});
