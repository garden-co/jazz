import { describe, expect, it } from "vitest";
import type { WasmSchema } from "jazz-tools";
import { GenericQueryBuilder } from "./generic-query-builder.js";

describe("GenericQueryBuilder", () => {
  const schema = {} as WasmSchema;

  it("serializes branch and diff metadata", () => {
    const branchId = "01963f3e-5cbe-7a62-8d7c-123456789abc";
    const query = new GenericQueryBuilder("todos", schema)
      .branch(branchId)
      .diff()
      .where({ title: "Write docs" });

    expect(JSON.parse(query._build())).toEqual({
      table: "todos",
      conditions: [{ column: "title", op: "eq", value: "Write docs" }],
      includes: {},
      orderBy: [],
      hops: [],
      branches: [branchId],
      diff: true,
    });
  });

  it("rejects non-object branch ids", () => {
    expect(() => new GenericQueryBuilder("todos", schema).branch("draft")).toThrow(
      "Invalid branch id",
    );
  });
});
