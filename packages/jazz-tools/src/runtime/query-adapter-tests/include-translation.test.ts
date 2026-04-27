import { describe, expect, it } from "vitest";
import { parseTranslatedQuery, translateQuery, type WasmSchema } from "./support.js";

describe("include translation", () => {
  const schemaWithRelations: WasmSchema = {
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
  };

  it("translates forward relation include", () => {
    const builderJson = JSON.stringify({
      table: "todos",
      conditions: [],
      includes: { owner: true },
      orderBy: [],
    });

    const result = parseTranslatedQuery(builderJson, schemaWithRelations);

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

  it("marks forward scalar relation include as required when requested", () => {
    const builderJson = JSON.stringify({
      table: "todos",
      conditions: [],
      includes: { owner: true },
      __jazz_requireIncludes: true,
      orderBy: [],
    });

    const result = parseTranslatedQuery(builderJson, schemaWithRelations);

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
        requirement: "AtLeastOne",
        nested_arrays: [],
      },
    ]);
  });

  it("hides top-level include column names when projecting selected columns", () => {
    const builderJson = JSON.stringify({
      table: "todos",
      conditions: [],
      includes: { owner: true },
      select: ["title"],
      orderBy: [],
    });

    const result = parseTranslatedQuery(builderJson, schemaWithRelations);

    expect(result.select_columns).toEqual(["title", "__jazz_include_owner"]);
    expect(result.array_subqueries).toEqual([
      {
        column_name: "__jazz_include_owner",
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

    const result = parseTranslatedQuery(builderJson, schemaWithRelations);

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

  it("translates UUID[] forward and reverse includes using membership columns", () => {
    const arrayFkSchema: WasmSchema = {
      files: {
        columns: [
          {
            name: "parts",
            column_type: { type: "Array", element: { type: "Uuid" } },
            nullable: false,
            references: "file_parts",
          },
        ],
      },
      file_parts: {
        columns: [{ name: "name", column_type: { type: "Text" }, nullable: false }],
      },
    };

    const forward = JSON.parse(
      translateQuery(
        JSON.stringify({
          table: "files",
          conditions: [],
          includes: { parts: true },
          orderBy: [],
        }),
        arrayFkSchema,
      ),
    );
    expect(forward.array_subqueries).toEqual([
      {
        column_name: "parts",
        table: "file_parts",
        inner_column: "id",
        outer_column: "files.parts",
        filters: [],
        joins: [],
        select_columns: null,
        order_by: [],
        limit: null,
        nested_arrays: [],
      },
    ]);

    const reverse = JSON.parse(
      translateQuery(
        JSON.stringify({
          table: "file_parts",
          conditions: [],
          includes: { filesViaParts: true },
          orderBy: [],
        }),
        arrayFkSchema,
      ),
    );
    expect(reverse.array_subqueries).toEqual([
      {
        column_name: "filesViaParts",
        table: "files",
        inner_column: "parts",
        outer_column: "file_parts.id",
        filters: [],
        joins: [],
        select_columns: null,
        order_by: [],
        limit: null,
        nested_arrays: [],
      },
    ]);
  });

  it("marks UUID[] forward includes with cardinality requirement when requested", () => {
    const arrayFkSchema: WasmSchema = {
      files: {
        columns: [
          {
            name: "parts",
            column_type: { type: "Array", element: { type: "Uuid" } },
            nullable: false,
            references: "file_parts",
          },
        ],
      },
      file_parts: {
        columns: [{ name: "name", column_type: { type: "Text" }, nullable: false }],
      },
    };

    const forward = JSON.parse(
      translateQuery(
        JSON.stringify({
          table: "files",
          conditions: [],
          includes: { parts: true },
          __jazz_requireIncludes: true,
          orderBy: [],
        }),
        arrayFkSchema,
      ),
    );
    expect(forward.array_subqueries).toEqual([
      {
        column_name: "parts",
        table: "file_parts",
        inner_column: "id",
        outer_column: "files.parts",
        filters: [],
        joins: [],
        select_columns: null,
        order_by: [],
        limit: null,
        requirement: "MatchCorrelationCardinality",
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

    const result = parseTranslatedQuery(builderJson, schemaWithRelations);

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

    const result = parseTranslatedQuery(builderJson, nestedSchema);

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

  it("translates nested required includes", () => {
    const nestedSchema: WasmSchema = {
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
    };

    const builderJson = JSON.stringify({
      table: "comments",
      conditions: [],
      includes: {
        todo: {
          table: "todos",
          conditions: [],
          includes: { owner: true },
          __jazz_requireIncludes: true,
          select: [],
          orderBy: [],
          hops: [],
        },
      },
      orderBy: [],
    });

    const result = parseTranslatedQuery(builderJson, nestedSchema);

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
            requirement: "AtLeastOne",
            nested_arrays: [],
          },
        ],
      },
    ]);
  });

  it("translates include builders with projected nested columns", () => {
    const nestedSchema: WasmSchema = {
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
        columns: [
          { name: "name", column_type: { type: "Text" }, nullable: false },
          { name: "email", column_type: { type: "Text" }, nullable: false },
        ],
      },
    };

    const builderJson = JSON.stringify({
      table: "comments",
      conditions: [],
      includes: {
        todo: {
          table: "todos",
          conditions: [],
          includes: {
            owner: {
              table: "users",
              conditions: [],
              includes: {},
              select: ["name"],
              orderBy: [],
              hops: [],
            },
          },
          select: ["title"],
          orderBy: [],
          hops: [],
        },
      },
      orderBy: [],
    });

    const result = parseTranslatedQuery(builderJson, nestedSchema);

    expect(result.array_subqueries).toEqual([
      {
        column_name: "todo",
        table: "todos",
        inner_column: "id",
        outer_column: "comments.todo_id",
        filters: [],
        joins: [],
        select_columns: ["title", "__jazz_include_owner"],
        order_by: [],
        limit: null,
        nested_arrays: [
          {
            column_name: "__jazz_include_owner",
            table: "users",
            inner_column: "id",
            outer_column: "todos.owner_id",
            filters: [],
            joins: [],
            select_columns: ["name"],
            order_by: [],
            limit: null,
            nested_arrays: [],
          },
        ],
      },
    ]);
  });

  it("translates include builders with mixed wildcard and magic projections", () => {
    const nestedSchema: WasmSchema = {
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
        columns: [
          { name: "name", column_type: { type: "Text" }, nullable: false },
          { name: "email", column_type: { type: "Text" }, nullable: false },
        ],
      },
    };

    const builderJson = JSON.stringify({
      table: "comments",
      conditions: [],
      includes: {
        todo: {
          table: "todos",
          conditions: [],
          includes: {
            owner: {
              table: "users",
              conditions: [],
              includes: {},
              select: ["*", "$canEdit"],
              orderBy: [],
              hops: [],
            },
          },
          select: ["*", "$canDelete"],
          orderBy: [],
          hops: [],
        },
      },
      orderBy: [],
    });

    const result = parseTranslatedQuery(builderJson, nestedSchema);

    expect(result.array_subqueries).toEqual([
      {
        column_name: "todo",
        table: "todos",
        inner_column: "id",
        outer_column: "comments.todo_id",
        filters: [],
        joins: [],
        select_columns: ["title", "owner_id", "$canDelete", "__jazz_include_owner"],
        order_by: [],
        limit: null,
        nested_arrays: [
          {
            column_name: "__jazz_include_owner",
            table: "users",
            inner_column: "id",
            outer_column: "todos.owner_id",
            filters: [],
            joins: [],
            select_columns: ["name", "email", "$canEdit"],
            order_by: [],
            limit: null,
            nested_arrays: [],
          },
        ],
      },
    ]);
  });

  it("omits implicit id from include builder projections", () => {
    const builderJson = JSON.stringify({
      table: "users",
      conditions: [],
      includes: {
        todosViaOwner: {
          table: "todos",
          conditions: [],
          includes: {},
          select: ["id"],
          orderBy: [],
          hops: [],
        },
      },
      orderBy: [],
    });

    const result = parseTranslatedQuery(builderJson, schemaWithRelations);

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

  it('keeps projected include mode for include builders that select only "id"', () => {
    const nestedSchema: WasmSchema = {
      users: {
        columns: [{ name: "name", column_type: { type: "Text" }, nullable: false }],
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
    };

    const builderJson = JSON.stringify({
      table: "comments",
      conditions: [],
      includes: {
        todo: {
          table: "todos",
          conditions: [],
          includes: {
            owner: true,
          },
          select: ["id"],
          orderBy: [],
          hops: [],
        },
      },
      orderBy: [],
    });

    const result = parseTranslatedQuery(builderJson, nestedSchema);

    expect(result.array_subqueries).toEqual([
      {
        column_name: "todo",
        table: "todos",
        inner_column: "id",
        outer_column: "comments.todo_id",
        filters: [],
        joins: [],
        select_columns: ["__jazz_include_owner"],
        order_by: [],
        limit: null,
        nested_arrays: [
          {
            column_name: "__jazz_include_owner",
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
