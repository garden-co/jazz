import { describe, expect, it } from "vitest";
import { basicSchema, parseTranslatedQuery, translateQuery } from "./support.js";

describe("orderBy translation", () => {
  it("translates ascending order", () => {
    const builderJson = JSON.stringify({
      table: "todos",
      conditions: [],
      includes: {},
      orderBy: [["priority", "asc"]],
    });

    const result = parseTranslatedQuery(builderJson, basicSchema);
    expect(result.relation_ir?.type).toBe("OrderBy");
    expect(result.relation_ir?.terms).toEqual([
      { column: { column: "priority" }, direction: "Asc" },
    ]);
  });

  it("translates descending order", () => {
    const builderJson = JSON.stringify({
      table: "todos",
      conditions: [],
      includes: {},
      orderBy: [["priority", "desc"]],
    });

    const result = parseTranslatedQuery(builderJson, basicSchema);
    expect(result.relation_ir?.type).toBe("OrderBy");
    expect(result.relation_ir?.terms).toEqual([
      { column: { column: "priority" }, direction: "Desc" },
    ]);
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

    const result = parseTranslatedQuery(builderJson, basicSchema);
    expect(result.relation_ir?.type).toBe("OrderBy");
    expect(result.relation_ir?.terms).toEqual([
      { column: { column: "priority" }, direction: "Desc" },
      { column: { column: "title" }, direction: "Asc" },
    ]);
  });

  it("translates magic columns in orderBy", () => {
    const builderJson = JSON.stringify({
      table: "todos",
      conditions: [],
      includes: {},
      orderBy: [["$canEdit", "desc"]],
    });

    const result = parseTranslatedQuery(builderJson, basicSchema);
    expect(result.relation_ir?.type).toBe("OrderBy");
    expect(result.relation_ir?.terms).toEqual([
      { column: { column: "$canEdit" }, direction: "Desc" },
    ]);
  });

  it("rejects Json columns in orderBy", () => {
    const builderJson = JSON.stringify({
      table: "todos",
      conditions: [],
      includes: {},
      orderBy: [["metadata", "asc"]],
    });

    expect(() => translateQuery(builderJson, basicSchema)).toThrow(
      'JSON column "metadata" cannot be used in orderBy().',
    );
  });
});
