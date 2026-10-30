import { describe, expect, it } from "vitest";
import type { ColumnType } from "./drivers/types.js";
import {
  getSupportedWhereOperatorsForColumn,
  getSupportedWhereOperatorsForSchemaColumn,
} from "./where-operators.js";

function operators(name: string, columnType: ColumnType, nullable: boolean) {
  return getSupportedWhereOperatorsForColumn({ name, columnType, nullable });
}

describe("where operator discovery", () => {
  it("adds isNull for nullable scalar columns", () => {
    expect(operators("title", { type: "Text" }, true)).toEqual([
      "eq",
      "ne",
      "contains",
      "in",
      "notIn",
      "isNull",
    ]);
    expect(operators("rank", { type: "Integer" }, true)).toEqual([
      "eq",
      "ne",
      "gt",
      "gte",
      "lt",
      "lte",
      "in",
      "notIn",
      "isNull",
    ]);
  });
  it("adds isNull for nullable arrays", () => {
    expect(operators("tags", { type: "Array", element: { type: "Text" } }, true)).toEqual([
      "eq",
      "contains",
      "in",
      "notIn",
      "isNull",
    ]);
    expect(operators("tags", { type: "Array", element: { type: "Text" } }, false)).toEqual([
      "eq",
      "contains",
      "in",
      "notIn",
    ]);
  });

  it("adds isNull for nullable Bytea in both discovery APIs", () => {
    expect(operators("attachment", { type: "Bytea" }, true)).toEqual([
      "eq",
      "ne",
      "in",
      "notIn",
      "isNull",
    ]);
    expect(
      getSupportedWhereOperatorsForSchemaColumn("attachment", {
        name: "attachment",
        column_type: { type: "Bytea" },
        nullable: true,
      }),
    ).toEqual(["eq", "ne", "in", "notIn", "isNull"]);
  });

  it("does not add isNull to required, id, payload-enum, or Row columns", () => {
    expect(operators("attachment", { type: "Bytea" }, false)).toEqual(["eq", "ne", "in", "notIn"]);
    expect(operators("id", { type: "Uuid" }, true)).toEqual(["eq", "ne", "in", "notIn"]);
    expect(operators("event", { type: "EnumPayload", cases: [] }, true)).toEqual([]);
    expect(operators("author", { type: "Row", columns: [] }, true)).toEqual([]);
  });
});
