/**
 * Transform WASM row results to typed TypeScript objects.
 */

import type { Value as WasmValue, WasmRow, WasmSchema } from "../drivers/types.js";
import type { ColumnType } from "../drivers/types.js";
import { analyzeRelations, type Relation } from "../codegen/relation-analyzer.js";
import {
  isPermissionIntrospectionColumn,
  isProvenanceMagicColumn,
  magicColumnType,
} from "../magic-columns.js";
import { normalizeIncludeEntries, type NormalizedIncludeSpec } from "./query-builder-shape.js";
import { hiddenIncludeColumnName, resolveSelectedColumns } from "./select-projection.js";

export type { WasmValue };

export interface IncludeSpec {
  [relationName: string]: unknown;
}

type BaseColumn = { name: string; columnType: ColumnType };

type IncludePlan = {
  relation: Relation;
  nested: IncludePlan[];
  projection?: readonly string[];
  nestedRelationNames: readonly string[];
  /** Resolved on the first included row, then reused for every later row. */
  baseColumns?: BaseColumn[];
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
): BaseColumn[] {
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
    .filter((column): column is BaseColumn => column !== null);
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
      nestedRelationNames: nested.map((plan) => plan.relation.name),
      projection: spec.select.length > 0 ? spec.select : undefined,
    });
  }

  return plans;
}

type ReadColumnTransformRegistry = Readonly<
  Record<string, Readonly<Record<string, { from(value: unknown): unknown }>> | undefined>
>;

export function applyColumnTransforms(
  row: Record<string, unknown>,
  transforms?: Readonly<Record<string, { from(value: unknown): unknown }>>,
  excludedColumns: readonly string[] = [],
): Record<string, unknown> {
  if (!transforms) return row;
  for (const column in transforms) {
    if (
      !Object.hasOwn(transforms, column) ||
      !(column in row) ||
      excludedColumns.includes(column)
    ) {
      continue;
    }
    row[column] = transforms[column]!.from(row[column]);
  }
  return row;
}

function applyTableColumnTransforms(
  row: Record<string, unknown>,
  tableName: string,
  transformsByTable: ReadColumnTransformRegistry | undefined,
  excludedColumns: readonly string[],
): Record<string, unknown> {
  return applyColumnTransforms(row, transformsByTable?.[tableName], excludedColumns);
}

function transformIncludedValue(
  value: WasmValue,
  plan: IncludePlan,
  schema: WasmSchema,
  transformsByTable?: ReadColumnTransformRegistry,
): unknown {
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
    plan.baseColumns ??= resolveBaseColumns(plan.relation.toTable, schema, plan.projection);
    return transformRowValues(
      columnValues,
      schema,
      plan.relation.toTable,
      plan.nested,
      plan.baseColumns,
      rowId,
      valuesByColumn,
      transformsByTable,
      plan.nestedRelationNames,
    );
  });

  return plan.relation.isArray ? rows : (rows[0] ?? null);
}

function transformRowValues(
  values: WasmValue[],
  schema: WasmSchema,
  tableName: string,
  includePlans: IncludePlan[],
  baseColumns: BaseColumn[],
  rowId?: string,
  valuesByColumn?: NamedRowValues,
  transformsByTable?: ReadColumnTransformRegistry,
  includedRelationNames: readonly string[] = [],
  applyRootTransforms = true,
): Record<string, unknown> {
  const table = schema[tableName];
  if (!table) {
    throw new Error(`Unknown table "${tableName}" in schema`);
  }

  const obj: Record<string, unknown> = {};
  if (rowId !== undefined) {
    obj.id = rowId;
  }

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
    obj[plan.relation.name] = transformIncludedValue(value, plan, schema, transformsByTable);
  }

  return applyRootTransforms
    ? applyTableColumnTransforms(obj, tableName, transformsByTable, includedRelationNames)
    : obj;
}

function timestampToDate(value: number, _columnName?: string): Date {
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
      if (columnType?.type === "Row" && columnName && isProvenanceMagicColumn(columnName)) {
        return Object.fromEntries(
          columnType.columns.map((column, index) => [
            column.name,
            unwrapValue(v.value.values[index]!, column.column_type, `${columnName}.${column.name}`),
          ]),
        );
      }
      if (columnType?.type === "Row") {
        return v.value.values.map((entry, index) =>
          unwrapValue(entry, columnType.columns[index]?.column_type),
        );
      }
      return v.value.values.map((entry) => unwrapValue(entry));
  }
}

type PreparedRowTransform = {
  includePlans: IncludePlan[];
  includedRelationNames: readonly string[];
  baseColumns?: BaseColumn[];
};

function prepareRowTransform(
  schema: WasmSchema,
  tableName: string,
  includes: IncludeSpec,
): PreparedRowTransform {
  if (!schema[tableName]) {
    throw new Error(`Unknown table "${tableName}" in schema`);
  }
  const includePlans =
    Object.keys(includes).length === 0
      ? []
      : buildIncludePlans(tableName, normalizeIncludeEntries(includes), analyzeRelations(schema));
  return {
    includePlans,
    includedRelationNames: includePlans.map((plan) => plan.relation.name),
  };
}

function transformPreparedRow<T>(
  prepared: PreparedRowTransform,
  row: WasmRowWithNamedValues,
  schema: WasmSchema,
  tableName: string,
  projection: readonly string[] | undefined,
  transformsByTable: ReadColumnTransformRegistry | undefined,
  applyRootTransforms: boolean,
): T {
  prepared.baseColumns ??= resolveBaseColumns(tableName, schema, projection);
  return transformRowValues(
    row.values as WasmValue[],
    schema,
    tableName,
    prepared.includePlans,
    prepared.baseColumns,
    row.id,
    row.valuesByColumn,
    transformsByTable,
    prepared.includedRelationNames,
    applyRootTransforms,
  ) as T;
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
  transformsByTable?: ReadColumnTransformRegistry,
  applyRootTransforms = true,
): T[] {
  const prepared = prepareRowTransform(schema, tableName, includes);
  return rows.map((row: WasmRowWithNamedValues) =>
    transformPreparedRow<T>(
      prepared,
      row,
      schema,
      tableName,
      projection,
      transformsByTable,
      applyRootTransforms,
    ),
  );
}

/**
 * Build a per-row transform for a long-lived consumer such as a subscription.
 *
 * Equivalent to calling {@link transformRow} with the same arguments for each
 * row, but include plans (including the relation analysis of the whole
 * schema) and resolved column lists are built once, on the first row, and
 * reused. The schema must not be mutated while the transformer is in use.
 */
export function createRowTransformer<T>(
  schema: WasmSchema,
  tableName: string,
  includes: IncludeSpec = {},
  projection?: readonly string[],
  transformsByTable?: ReadColumnTransformRegistry,
  applyRootTransforms = true,
): (row: WasmRow) => T {
  let prepared: PreparedRowTransform | undefined;
  return (row) => {
    prepared ??= prepareRowTransform(schema, tableName, includes);
    return transformPreparedRow<T>(
      prepared,
      row,
      schema,
      tableName,
      projection,
      transformsByTable,
      applyRootTransforms,
    );
  };
}

export function transformRow<T>(
  row: WasmRow,
  schema: WasmSchema,
  tableName: string,
  includes: IncludeSpec = {},
  projection?: readonly string[],
  transformsByTable?: ReadColumnTransformRegistry,
  applyRootTransforms = true,
): T {
  const transformed = transformRows<T>(
    [row],
    schema,
    tableName,
    includes,
    projection,
    transformsByTable,
    applyRootTransforms,
  )[0];
  if (transformed === undefined) {
    throw new Error(`Failed to transform row for table "${tableName}"`);
  }
  return transformed;
}
