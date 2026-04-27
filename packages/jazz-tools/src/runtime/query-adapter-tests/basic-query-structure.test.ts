import { describe, expect, it } from "vitest";
import { basicSchema, parseTranslatedQuery } from "./support.js";

describe("basic query structure", () => {
  it("translates empty query", () => {
    const builderJson = JSON.stringify({
      table: "todos",
      conditions: [],
      includes: {},
      orderBy: [],
    });

    const result = parseTranslatedQuery(builderJson, basicSchema);

    expect(result.table).toBe("todos");
    expect(result.array_subqueries).toEqual([]);
    expect(result.relation_ir).toEqual({ type: "TableScan", table: "todos" });
    expect(result.branches).toBeUndefined();
    expect(result.disjuncts).toBeUndefined();
    expect(result.order_by).toBeUndefined();
    expect(result.offset).toBeUndefined();
    expect(result.limit).toBeUndefined();
    expect(result.include_deleted).toBeUndefined();
    expect(result.joins).toBeUndefined();
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

    const result = parseTranslatedQuery(builderJson, basicSchema);

    expect(result.relation_ir?.type).toBe("Limit");
    if (result.relation_ir?.type !== "Limit") {
      throw new Error("Expected relation_ir Limit node.");
    }
    expect(result.relation_ir.limit).toBe(10);
    expect(result.relation_ir.input?.type).toBe("Offset");
    if (result.relation_ir.input?.type !== "Offset") {
      throw new Error("Expected relation_ir Offset node.");
    }
    expect(result.relation_ir.input.offset).toBe(5);
  });

  it("pushes select columns into the runtime query payload", () => {
    const builderJson = JSON.stringify({
      table: "todos",
      conditions: [],
      includes: {},
      select: ["title", "done"],
      orderBy: [],
    });

    const result = parseTranslatedQuery(builderJson, basicSchema);

    expect(result.select_columns).toEqual(["title", "done"]);
    expect(result.relation_ir).toEqual({ type: "TableScan", table: "todos" });
  });

  it("pushes magic select columns into the runtime query payload", () => {
    const builderJson = JSON.stringify({
      table: "todos",
      conditions: [],
      includes: {},
      select: ["title", "$canRead", "$canEdit"],
      orderBy: [],
    });

    const result = parseTranslatedQuery(builderJson, basicSchema);

    expect(result.select_columns).toEqual(["title", "$canRead", "$canEdit"]);
    expect(result.relation_ir).toEqual({ type: "TableScan", table: "todos" });
  });

  it("pushes provenance magic select columns into the runtime query payload", () => {
    const builderJson = JSON.stringify({
      table: "todos",
      conditions: [],
      includes: {},
      select: ["title", "$createdBy", "$updatedAt"],
      orderBy: [],
    });

    const result = parseTranslatedQuery(builderJson, basicSchema);

    expect(result.select_columns).toEqual(["title", "$createdBy", "$updatedAt"]);
    expect(result.relation_ir).toEqual({ type: "TableScan", table: "todos" });
  });

  it('treats select(["*"]) as selecting all columns', () => {
    const builderJson = JSON.stringify({
      table: "todos",
      conditions: [],
      includes: {},
      select: ["*"],
      orderBy: [],
    });

    const result = parseTranslatedQuery(builderJson, basicSchema);

    expect(result.select_columns).toEqual([
      "title",
      "done",
      "priority",
      "status",
      "project",
      "tags",
      "metadata",
      "created_at",
    ]);
    expect(result.relation_ir).toEqual({ type: "TableScan", table: "todos" });
  });

  it('expands mixed select(["*", "$canDelete"]) into explicit runtime columns', () => {
    const builderJson = JSON.stringify({
      table: "todos",
      conditions: [],
      includes: {},
      select: ["*", "$canDelete"],
      orderBy: [],
    });

    const result = parseTranslatedQuery(builderJson, basicSchema);

    expect(result.select_columns).toEqual([
      "title",
      "done",
      "priority",
      "status",
      "project",
      "tags",
      "metadata",
      "created_at",
      "$canDelete",
    ]);
    expect(result.relation_ir).toEqual({ type: "TableScan", table: "todos" });
  });
});
