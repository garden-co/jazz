import { describe, expect, it } from "vitest";
import { basicSchema, parseTranslatedQuery, translateQuery, type WasmSchema } from "./support.js";

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

  it("throws when gather step does not use a forward hop", () => {
    const schema: WasmSchema = {
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

    const builderJson = JSON.stringify({
      table: "todos",
      conditions: [],
      includes: {},
      orderBy: [],
      gather: {
        max_depth: 10,
        step_table: "todos",
        step_current_column: "id",
        step_conditions: [],
        step_hops: ["todosViaParent"],
      },
    });

    expect(() => translateQuery(builderJson, schema)).toThrow(
      "gather(...) currently only supports forward hopTo(...) relations.",
    );
  });

  it("throws when gather query also includes include(...)", () => {
    const schema: WasmSchema = {
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

    const builderJson = JSON.stringify({
      table: "todos",
      conditions: [],
      includes: { parent: true },
      orderBy: [],
      gather: {
        max_depth: 10,
        step_table: "todos",
        step_current_column: "id",
        step_conditions: [],
        step_hops: ["parent"],
      },
    });

    expect(() => translateQuery(builderJson, schema)).toThrow(
      "gather(...) does not yet support include(...).",
    );
  });

  it("lowers gather query followed by hopTo(...)", () => {
    const schema: WasmSchema = {
      teams: {
        columns: [{ name: "name", column_type: { type: "Text" }, nullable: false }],
      },
      team_edges: {
        columns: [
          {
            name: "child_team",
            column_type: { type: "Uuid" },
            nullable: false,
            references: "teams",
          },
          {
            name: "parent_team",
            column_type: { type: "Uuid" },
            nullable: false,
            references: "teams",
          },
        ],
      },
    };

    const builderJson = JSON.stringify({
      table: "teams",
      conditions: [],
      includes: {},
      orderBy: [],
      hops: ["team_edgesViaChild_team"],
      gather: {
        max_depth: 10,
        step_table: "team_edges",
        step_current_column: "child_team",
        step_conditions: [],
        step_hops: ["parent_team"],
      },
    });

    const result = parseTranslatedQuery(builderJson, schema);
    expect(result.relation_ir?.type).toBe("Project");
    if (result.relation_ir?.type !== "Project") {
      throw new Error("Expected projected relation IR.");
    }
    expect(result.relation_ir.input.type).toBe("Join");
    if (result.relation_ir.input.type !== "Join") {
      throw new Error("Expected gather hop join relation IR.");
    }
    expect(result.relation_ir.input.left.type).toBe("Gather");
  });

  it("throws when hop query also includes include(...)", () => {
    const schema: WasmSchema = {
      teams: {
        columns: [{ name: "name", column_type: { type: "Text" }, nullable: false }],
      },
      team_edges: {
        columns: [
          {
            name: "child_team",
            column_type: { type: "Uuid" },
            nullable: false,
            references: "teams",
          },
          {
            name: "parent_team",
            column_type: { type: "Uuid" },
            nullable: false,
            references: "teams",
          },
        ],
      },
    };

    const builderJson = JSON.stringify({
      table: "team_edges",
      conditions: [],
      includes: { parent_team: true },
      orderBy: [],
      hops: ["parent_team"],
    });

    expect(() => translateQuery(builderJson, schema)).toThrow(
      "hopTo(...) does not yet support include(...).",
    );
  });
});
