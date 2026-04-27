import { describe, expect, it } from "vitest";
import { parseTranslatedQuery, type WasmSchema } from "./support.js";

describe("self-referential relations", () => {
  const selfRefSchema: WasmSchema = {
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
  };

  it("translates forward self-reference", () => {
    const builderJson = JSON.stringify({
      table: "todos",
      conditions: [],
      includes: { parent: true },
      orderBy: [],
    });

    const result = parseTranslatedQuery(builderJson, selfRefSchema);

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

    const result = parseTranslatedQuery(builderJson, selfRefSchema);

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
