import { describe, expect, it } from "vitest";
import {
  getSupportedWhereOperatorsForColumn,
  type WhereOperatorColumn,
} from "./where-operators.js";

describe("getSupportedWhereOperatorsForColumn", () => {
  it("supports notIn everywhere it supports in", () => {
    const columns = [
      { name: "id", columnType: { type: "Uuid" }, nullable: false },
      { name: "reference", columnType: { type: "Uuid" }, nullable: true, references: "users" },
      { name: "text", columnType: { type: "Text" }, nullable: false },
      { name: "boolean", columnType: { type: "Boolean" }, nullable: false },
      { name: "integer", columnType: { type: "Integer" }, nullable: false },
      { name: "bigint", columnType: { type: "BigInt" }, nullable: false },
      { name: "double", columnType: { type: "Double" }, nullable: false },
      { name: "timestamp", columnType: { type: "Timestamp" }, nullable: false },
      { name: "uuid", columnType: { type: "Uuid" }, nullable: false },
      { name: "bytes", columnType: { type: "Bytea" }, nullable: false },
      { name: "json", columnType: { type: "Json" }, nullable: false },
      {
        name: "enum",
        columnType: { type: "Enum", variants: ["a", "b"] },
        nullable: false,
      },
      {
        name: "array",
        columnType: { type: "Array", element: { type: "Text" } },
        nullable: false,
      },
    ] satisfies WhereOperatorColumn[];

    const missingNotIn = columns
      .filter((column) => {
        const operators = getSupportedWhereOperatorsForColumn(column);
        return operators.includes("in") && !operators.includes("notIn");
      })
      .map((column) => column.name);

    expect(missingNotIn).toEqual([]);
  });
});
