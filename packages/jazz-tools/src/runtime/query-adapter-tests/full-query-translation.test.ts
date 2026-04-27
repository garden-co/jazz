import { describe, expect, it } from "vitest";
import {
  parseTranslatedQuery,
  toLegacyRelExprForTest,
  translateBuilderToRelationIr,
  type WasmSchema,
} from "./support.js";

describe("full query translation", () => {
  const fullSchema: WasmSchema = {
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

    const result = parseTranslatedQuery(builderJson, fullSchema);

    expect(result).toMatchObject({
      table: "todos",
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
    });
    expect(result.branches).toBeUndefined();
    expect(result.disjuncts).toBeUndefined();
    expect(result.order_by).toBeUndefined();
    expect(result.offset).toBeUndefined();
    expect(result.limit).toBeUndefined();
    expect(result.include_deleted).toBeUndefined();
    expect(result.joins).toBeUndefined();

    expect(result.relation_ir?.type).toBe("Limit");
    if (result.relation_ir?.type !== "Limit") {
      throw new Error("Expected top-level relation_ir Limit node.");
    }
    expect(result.relation_ir.limit).toBe(10);
    expect(result.relation_ir.input.type).toBe("Offset");
    if (result.relation_ir.input.type !== "Offset") {
      throw new Error("Expected relation_ir Offset input node.");
    }
    expect(result.relation_ir.input.offset).toBe(5);
    expect(result.relation_ir.input.input.type).toBe("OrderBy");
    if (result.relation_ir.input.input.type !== "OrderBy") {
      throw new Error("Expected relation_ir OrderBy input node.");
    }
    expect(result.relation_ir.input.input.terms).toEqual([
      { column: { column: "priority" }, direction: "Desc" },
      { column: { column: "title" }, direction: "Asc" },
    ]);
  });
});

it("keeps gather semantics in relation_ir payload", () => {
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
      step_hops: ["parent"],
    },
  });

  const result = parseTranslatedQuery(builderJson, schema);
  expect(result.recursive).toBeUndefined();
  expect(result.joins).toBeUndefined();
  expect(result.relation_ir?.type).toBe("Gather");
});

it("keeps hop semantics in relation_ir payload", () => {
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
    conditions: [{ column: "child_team", op: "eq", value: "00000000-0000-0000-0000-000000000001" }],
    includes: {},
    orderBy: [],
    hops: ["parent_team"],
  });

  const result = parseTranslatedQuery(builderJson, schema);
  expect(result.joins).toBeUndefined();
  expect(result.result_element_index).toBeUndefined();
  expect(result.recursive).toBeUndefined();
  expect(result.relation_ir?.type).toBe("Project");
});

it("keeps multi-hop semantics in relation_ir payload", () => {
  const schema: WasmSchema = {
    users: {
      columns: [
        { name: "name", column_type: { type: "Text" }, nullable: false },
        { name: "team_id", column_type: { type: "Uuid" }, nullable: true, references: "teams" },
      ],
    },
    teams: {
      columns: [
        { name: "name", column_type: { type: "Text" }, nullable: false },
        { name: "org_id", column_type: { type: "Uuid" }, nullable: true, references: "orgs" },
      ],
    },
    orgs: {
      columns: [{ name: "name", column_type: { type: "Text" }, nullable: false }],
    },
  };

  const builderJson = JSON.stringify({
    table: "users",
    conditions: [],
    includes: {},
    orderBy: [],
    hops: ["team", "org"],
  });

  const result = parseTranslatedQuery(builderJson, schema);
  expect(result.joins).toBeUndefined();
  expect(result.result_element_index).toBeUndefined();
  expect(result.recursive).toBeUndefined();
  expect(result.relation_ir?.type).toBe("Project");
});

it("lowers hop metadata to relation IR join + project", () => {
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
    conditions: [{ column: "child_team", op: "eq", value: "00000000-0000-0000-0000-000000000001" }],
    includes: {},
    orderBy: [],
    hops: ["parent_team"],
  });

  const ir = toLegacyRelExprForTest(translateBuilderToRelationIr(builderJson, schema));
  expect(ir.type).toBe("Project");
  if (ir.type !== "Project") {
    throw new Error("Expected project relation IR.");
  }
  expect(ir.input.type).toBe("Join");
  if (ir.input.type !== "Join") {
    throw new Error("Expected join input relation IR.");
  }
  expect(ir.input.on).toEqual([
    {
      left: { scope: "team_edges", column: "parent_team" },
      right: { scope: "__hop_0", column: "id" },
    },
  ]);
  expect(ir.columns[0]).toEqual({
    alias: "id",
    expr: { type: "Column", column: { scope: "__hop_0", column: "id" } },
  });
});

it("lowers gather metadata to relation IR gather node", () => {
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
    conditions: [{ column: "title", op: "ne", value: "archived" }],
    includes: {},
    orderBy: [],
    gather: {
      max_depth: 10,
      step_table: "todos",
      step_current_column: "id",
      step_conditions: [],
      step_hops: ["parent"],
    },
  });

  const ir = toLegacyRelExprForTest(translateBuilderToRelationIr(builderJson, schema));
  expect(ir.type).toBe("Gather");
  if (ir.type !== "Gather") {
    throw new Error("Expected gather relation IR.");
  }
  expect(ir.frontierKey).toEqual({ type: "RowId", source: "Current" });
  expect(ir.step.type).toBe("Project");
  if (ir.step.type !== "Project") {
    throw new Error("Expected gather step project relation.");
  }
  expect(ir.step.input.type).toBe("Join");
  if (ir.step.input.type !== "Join") {
    throw new Error("Expected gather step join relation.");
  }
  expect(ir.step.input.left.type).toBe("Filter");
  if (ir.step.input.left.type !== "Filter") {
    throw new Error("Expected gather step filter relation.");
  }
  expect(ir.step.input.left.predicate).toEqual({
    type: "Cmp",
    left: { scope: "todos", column: "id" },
    op: "Eq",
    right: { type: "RowId", source: "Frontier" },
  });
});

it("lowers gather metadata seeded from a hop relation", () => {
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
    includes: {},
    orderBy: [],
    gather: {
      seed: {
        table: "team_edges",
        conditions: [{ column: "child_team", op: "eq", value: "team-a" }],
        hops: ["parent_team"],
      },
      max_depth: 3,
      step_table: "team_edges",
      step_current_column: "child_team",
      step_conditions: [],
      step_hops: ["parent_team"],
    },
  });

  const ir = toLegacyRelExprForTest(translateBuilderToRelationIr(builderJson, schema));
  expect(ir.type).toBe("Gather");
  if (ir.type !== "Gather") {
    throw new Error("Expected gather relation IR.");
  }
  expect(ir.seed.type).toBe("Project");
  if (ir.seed.type !== "Project") {
    throw new Error("Expected projected seed relation IR.");
  }
  expect(ir.seed.input.type).toBe("Join");
  if (ir.seed.input.type !== "Join") {
    throw new Error("Expected joined seed relation IR.");
  }
  expect(ir.seed.input.left.type).toBe("Filter");
  if (ir.seed.input.left.type !== "Filter") {
    throw new Error("Expected filtered seed relation IR.");
  }
});

it("lowers gather metadata seeded from a union relation", () => {
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
    includes: {},
    orderBy: [],
    gather: {
      seed: {
        union: {
          inputs: [
            {
              table: "team_edges",
              conditions: [{ column: "child_team", op: "eq", value: "team-a" }],
              hops: ["parent_team"],
            },
            {
              table: "teams",
              conditions: [],
              hops: [],
              gather: {
                seed: {
                  table: "teams",
                  conditions: [{ column: "name", op: "eq", value: "admins" }],
                  hops: [],
                },
                max_depth: 2,
                step_table: "team_edges",
                step_current_column: "child_team",
                step_conditions: [],
                step_hops: ["parent_team"],
              },
            },
          ],
        },
      },
      max_depth: 4,
      step_table: "team_edges",
      step_current_column: "child_team",
      step_conditions: [],
      step_hops: ["parent_team"],
    },
  });

  const ir = toLegacyRelExprForTest(translateBuilderToRelationIr(builderJson, schema));
  expect(ir.type).toBe("Gather");
  if (ir.type !== "Gather") {
    throw new Error("Expected gather relation IR.");
  }
  expect(ir.seed.type).toBe("Union");
  if (ir.seed.type !== "Union") {
    throw new Error("Expected union seed relation IR.");
  }
  expect(ir.seed.inputs).toHaveLength(2);
  expect(ir.seed.inputs[0]?.type).toBe("Project");
  expect(ir.seed.inputs[1]?.type).toBe("Gather");
});
