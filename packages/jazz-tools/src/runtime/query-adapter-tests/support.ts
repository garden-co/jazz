import { expect } from "vitest";
import { translateBuilderToRelationIr, translateQuery } from "../query-adapter.js";
import type { WasmSchema } from "../../drivers/types.js";
import { toLegacyRelExprForTest } from "../../testing/relation-ir-test-helpers.js";

export { toLegacyRelExprForTest, translateBuilderToRelationIr, translateQuery };
export type { WasmSchema };

export function parseTranslatedQuery(builderJson: string, schema: WasmSchema): any {
  const parsed = JSON.parse(translateQuery(builderJson, schema));
  parsed.relation_ir = toLegacyRelExprForTest(parsed.relation_ir);
  return parsed;
}

export function expectFilterPredicate(result: any): any {
  expect(result.relation_ir?.type).toBe("Filter");
  if (result.relation_ir?.type !== "Filter") {
    throw new Error("Expected relation_ir Filter node.");
  }
  return result.relation_ir.predicate;
}

export const basicSchema: WasmSchema = {
  todos: {
    columns: [
      { name: "title", column_type: { type: "Text" }, nullable: false },
      { name: "done", column_type: { type: "Boolean" }, nullable: false },
      { name: "priority", column_type: { type: "Integer" }, nullable: true },
      {
        name: "status",
        column_type: { type: "Enum", variants: ["done", "in_progress", "todo"] },
        nullable: false,
      },
      { name: "project", column_type: { type: "Uuid" }, nullable: true },
      {
        name: "tags",
        column_type: { type: "Array", element: { type: "Text" } },
        nullable: false,
      },
      { name: "metadata", column_type: { type: "Json" }, nullable: true },
      { name: "created_at", column_type: { type: "Timestamp" }, nullable: true },
    ],
  },
};
