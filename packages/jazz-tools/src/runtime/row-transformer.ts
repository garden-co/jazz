/**
 * Transform WASM row results to typed TypeScript objects.
 */

import type { Value as WasmValue, WasmRow, WasmSchema } from "../drivers/types.js";
import { analyzeRelations, type Relation } from "../codegen/relation-analyzer.js";

export type { WasmValue };

export interface IncludeSpec {
  [relationName: string]: boolean | IncludeSpec;
}

type IncludePlan = {
  relation: Relation;
  nested: IncludePlan[];
};

function buildIncludePlans(
  tableName: string,
  includes: IncludeSpec,
  relationsByTable: Map<string, Relation[]>,
): IncludePlan[] {
  const relations = relationsByTable.get(tableName) || [];
  const plans: IncludePlan[] = [];

  for (const [relationName, spec] of Object.entries(includes)) {
    if (!spec) continue;

    const relation = relations.find((candidate) => candidate.name === relationName);
    if (!relation) {
      throw new Error(`Unknown relation "${relationName}" on table "${tableName}"`);
    }

    const nested =
      typeof spec === "object"
        ? buildIncludePlans(relation.toTable, spec as IncludeSpec, relationsByTable)
        : [];

    plans.push({ relation, nested });
  }

  return plans;
}

function transformIncludedValue(value: WasmValue, plan: IncludePlan, schema: WasmSchema): unknown {
  if (value.type !== "Array") {
    return unwrapValue(value);
  }

  const rows = value.value.map((entry) => {
    if (entry.type !== "Row") {
      return unwrapValue(entry);
    }
    return transformRowValues(entry.value, schema, plan.relation.toTable, plan.nested);
  });

  return plan.relation.isArray ? rows : rows[0];
}

function transformRowValues(
  values: WasmValue[],
  schema: WasmSchema,
  tableName: string,
  includePlans: IncludePlan[],
  rowId?: string,
): Record<string, unknown> {
  const table = schema.tables[tableName];
  if (!table) {
    throw new Error(`Unknown table "${tableName}" in schema`);
  }

  const obj: Record<string, unknown> = {};
  if (rowId !== undefined) {
    obj.id = rowId;
  }

  for (let i = 0; i < table.columns.length; i++) {
    const col = table.columns[i];
    const value = values[i];
    if (value !== undefined) {
      obj[col.name] = unwrapValue(value);
    }
  }

  for (let i = 0; i < includePlans.length; i++) {
    const value = values[table.columns.length + i];
    if (value === undefined) continue;
    const plan = includePlans[i];
    obj[plan.relation.name] = transformIncludedValue(value, plan, schema);
  }

  return obj;
}

/**
 * Unwrap a WasmValue to its JavaScript equivalent.
 */
export function unwrapValue(v: WasmValue): unknown {
  switch (v.type) {
    case "Text":
    case "Uuid":
      return v.value;
    case "Boolean":
      return v.value;
    case "Integer":
    case "BigInt":
    case "Double":
    case "Timestamp":
      return v.value;
    case "Bytea":
      return v.value;
    case "Null":
      return undefined;
    case "Array":
      return v.value.map(unwrapValue);
    case "Row":
      return v.value.map(unwrapValue);
  }
}

/**
 * Transform WasmRow[] to typed objects using schema column order.
 *
 * @param rows Array of WasmRow results from query
 * @param schema WasmSchema containing table definitions
 * @param tableName Name of the table being queried
 * @param includes Include tree from QueryBuilder._build() (if any)
 * @returns Array of typed objects with named properties
 */
export function transformRows<T>(
  rows: WasmRow[],
  schema: WasmSchema,
  tableName: string,
  includes: IncludeSpec = {},
): T[] {
  if (!schema.tables[tableName]) {
    throw new Error(`Unknown table "${tableName}" in schema`);
  }

  const includePlans =
    Object.keys(includes).length === 0
      ? []
      : buildIncludePlans(tableName, includes, analyzeRelations(schema));

  return rows.map((row) => {
    return transformRowValues(
      row.values as WasmValue[],
      schema,
      tableName,
      includePlans,
      row.id,
    ) as T;
  });
}
