/**
 * Transform WASM row results to typed TypeScript objects.
 */

import type { Value as WasmValue, WasmRow, WasmSchema } from "../drivers/types.js";
import type { ColumnType } from "../drivers/types.js";
import { analyzeRelations, type Relation } from "../codegen/relation-analyzer.js";
import {
  isPermissionIntrospectionColumn,
  isProvenanceMagicTimestampColumn,
  magicColumnType,
} from "../magic-columns.js";
import { normalizeIncludeEntries, type NormalizedIncludeSpec } from "./query-builder-shape.js";
import { hiddenIncludeColumnName, resolveSelectedColumns } from "./select-projection.js";

export type { WasmValue };

export interface IncludeSpec {
  [relationName: string]: unknown;
}

type IncludePlan = {
  relation: Relation;
  nested: IncludePlan[];
  projection?: readonly string[];
};

type NamedRowValues = Map<string, WasmValue> | Record<string, WasmValue>;
type RowValueWithNamedValues = {
  id?: string;
  values: WasmValue[];
  valuesByColumn?: NamedRowValues;
};
type WasmRowWithNamedValues = WasmRow & { valuesByColumn?: NamedRowValues };

function getNamedValue(
  valuesByColumn: NamedRowValues | undefined,
  name: string,
): WasmValue | undefined {
  if (!valuesByColumn) return undefined;
  if (valuesByColumn instanceof Map) {
    return valuesByColumn.get(name) ?? valuesByColumn.get(`user_${name}`);
  }
  return valuesByColumn[name] ?? valuesByColumn[`user_${name}`];
}

function hasNamedValue(valuesByColumn: NamedRowValues | undefined, name: string): boolean {
  if (!valuesByColumn) return false;
  if (valuesByColumn instanceof Map) {
    return valuesByColumn.has(name) || valuesByColumn.has(`user_${name}`);
  }
  return name in valuesByColumn || `user_${name}` in valuesByColumn;
}

function resolveBaseColumns(
  tableName: string,
  schema: WasmSchema,
  projection?: readonly string[],
): Array<{ name: string; columnType: ColumnType }> {
  const table = schema[tableName];
  if (!table) {
    throw new Error(`Unknown table "${tableName}" in schema`);
  }

  return resolveSelectedColumns(tableName, schema, projection)
    .map((columnName) => {
      const magicType = magicColumnType(columnName);
      if (magicType) {
        return { name: columnName, columnType: magicType };
      }
      if (isPermissionIntrospectionColumn(columnName)) {
        return { name: columnName, columnType: { type: "Boolean" } as const };
      }
      const column = table.columns.find((candidate) => candidate.name === columnName);
      return column ? { name: column.name, columnType: column.column_type } : null;
    })
    .filter((column): column is { name: string; columnType: ColumnType } => column !== null);
}

function toByteArray(value: unknown): Uint8Array {
  if (value instanceof Uint8Array) {
    return value;
  }

  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  }

  if (Array.isArray(value)) {
    const bytes = value.map((entry) => {
      if (typeof entry !== "number" || !Number.isInteger(entry) || entry < 0 || entry > 255) {
        throw new Error("Invalid Bytea array value. Expected integers in range 0..255.");
      }
      return entry;
    });
    return new Uint8Array(bytes);
  }

  throw new Error("Invalid Bytea value. Expected Uint8Array or byte array.");
}

function buildIncludePlans(
  tableName: string,
  includes: NormalizedIncludeSpec,
  relationsByTable: Map<string, Relation[]>,
): IncludePlan[] {
  const relations = relationsByTable.get(tableName) || [];
  const plans: IncludePlan[] = [];

  for (const [relationName, spec] of Object.entries(includes)) {
    const relation = relations.find((candidate) => candidate.name === relationName);
    if (!relation) {
      throw new Error(`Unknown relation "${relationName}" on table "${tableName}"`);
    }

    const nested = buildIncludePlans(relation.toTable, spec.includes, relationsByTable);

    plans.push({
      relation,
      nested,
      projection: spec.select.length > 0 ? spec.select : undefined,
    });
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
    // Row id is carried in the struct's `id` field
    const rowId = entry.value.id;
    const columnValues = entry.value.values;
    const valuesByColumn = (entry.value as RowValueWithNamedValues).valuesByColumn;
    return transformRowValues(
      columnValues,
      schema,
      plan.relation.toTable,
      plan.nested,
      rowId,
      plan.projection,
      valuesByColumn,
    );
  });

  return plan.relation.isArray ? rows : (rows[0] ?? null);
}

function transformRowValues(
  values: WasmValue[],
  schema: WasmSchema,
  tableName: string,
  includePlans: IncludePlan[],
  rowId?: string,
  projection?: readonly string[],
  valuesByColumn?: NamedRowValues,
): Record<string, unknown> {
  const table = schema[tableName];
  if (!table) {
    throw new Error(`Unknown table "${tableName}" in schema`);
  }

  const obj: Record<string, unknown> = {};
  if (rowId !== undefined) {
    obj.id = rowId;
  }

  const baseColumns = resolveBaseColumns(tableName, schema, projection);

  for (let i = 0; i < baseColumns.length; i++) {
    const col = baseColumns[i];
    if (!col) continue;
    const value = hasNamedValue(valuesByColumn, col.name)
      ? getNamedValue(valuesByColumn, col.name)
      : valuesByColumn
        ? undefined
        : values[i];
    if (value !== undefined) {
      obj[col.name] = unwrapValue(value, col.columnType, col.name);
    }
  }

  for (let i = 0; i < includePlans.length; i++) {
    const plan = includePlans[i];
    if (!plan) continue;
    const hiddenColumnName = hiddenIncludeColumnName(plan.relation.name);
    const value = hasNamedValue(valuesByColumn, hiddenColumnName)
      ? getNamedValue(valuesByColumn, hiddenColumnName)
      : hasNamedValue(valuesByColumn, plan.relation.name)
        ? getNamedValue(valuesByColumn, plan.relation.name)
        : valuesByColumn
          ? undefined
          : values[baseColumns.length + i];
    if (value === undefined) {
      obj[plan.relation.name] = plan.relation.isArray ? [] : null;
      continue;
    }
    obj[plan.relation.name] = transformIncludedValue(value, plan, schema);
  }

  return obj;
}

function timestampToDate(value: number, columnName?: string): Date {
  if (columnName && isProvenanceMagicTimestampColumn(columnName)) {
    return new Date(Math.trunc(value / 1_000));
  }
  return new Date(value);
}

export function unwrapValue(v: WasmValue, columnType?: ColumnType, columnName?: string): unknown {
  switch (v.type) {
    case "Text":
      if (columnType?.type === "Json") {
        try {
          return JSON.parse(v.value);
        } catch (error) {
          throw new Error(
            `Invalid stored JSON value: ${error instanceof Error ? error.message : String(error)}`,
          );
        }
      }
      return v.value;
    case "Uuid":
      return v.value;
    case "Boolean":
      return v.value;
    case "Integer":
    case "BigInt":
    case "Double":
      return v.value;
    case "Timestamp":
      return timestampToDate(v.value, columnName);
    case "Bytea":
      return toByteArray((v as { value: unknown }).value);
    case "Null":
      return null;
    case "Array":
      if (columnType?.type === "Array") {
        return v.value.map((entry) => unwrapValue(entry, columnType.element));
      }
      return v.value.map((entry) => unwrapValue(entry));
    case "Row":
      if (columnType?.type === "Row") {
        return v.value.values.map((entry, index) =>
          unwrapValue(entry, columnType.columns[index]?.column_type),
        );
      }
      return v.value.values.map((entry) => unwrapValue(entry));
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
  projection?: readonly string[],
): T[] {
  if (!schema[tableName]) {
    throw new Error(`Unknown table "${tableName}" in schema`);
  }

  const includePlans =
    Object.keys(includes).length === 0
      ? []
      : buildIncludePlans(tableName, normalizeIncludeEntries(includes), analyzeRelations(schema));

  return rows.map((row: WasmRowWithNamedValues) => {
    return transformRowValues(
      row.values as WasmValue[],
      schema,
      tableName,
      includePlans,
      row.id,
      projection,
      row.valuesByColumn,
    ) as T;
  });
}

/**
 * Transform aggregate result rows. Aggregate outputs are not table rows — the
 * fields are the aggregate aliases (plus any group columns), so the table
 * column transform does not apply. Integer aggregate values (count) decode as
 * bigint on the wire and surface as plain numbers.
 */
export function transformAggregateRows<T>(rows: WasmRow[], aliases: readonly string[]): T[] {
  return rows.map((row: WasmRowWithNamedValues) => {
    const obj: Record<string, unknown> = {};
    if (row.id !== undefined) obj.id = row.id;
    const values = row.values as WasmValue[];
    aliases.forEach((alias, index) => {
      const value = hasNamedValue(row.valuesByColumn, alias)
        ? getNamedValue(row.valuesByColumn, alias)
        : values[index];
      if (value !== undefined) {
        const unwrapped = unwrapValue(value, undefined, alias);
        obj[alias] = typeof unwrapped === "bigint" ? bigintToNumber(unwrapped, alias) : unwrapped;
      }
    });
    return obj as T;
  });
}

function bigintToNumber(value: bigint, fieldName: string): number {
  if (value > BigInt(Number.MAX_SAFE_INTEGER) || value < -BigInt(Number.MAX_SAFE_INTEGER)) {
    throw new Error(`Aggregate value "${fieldName}" exceeds Number.MAX_SAFE_INTEGER`);
  }
  return Number(value);
}

export function transformRow<T>(
  row: WasmRow,
  schema: WasmSchema,
  tableName: string,
  includes: IncludeSpec = {},
  projection?: readonly string[],
): T {
  const transformed = transformRows<T>([row], schema, tableName, includes, projection)[0];
  if (transformed === undefined) {
    throw new Error(`Failed to transform row for table "${tableName}"`);
  }
  return transformed;
}
