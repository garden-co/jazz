/**
 * Translate QueryBuilder JSON to WASM Query format.
 *
 * QueryBuilder produces a simple JSON structure:
 * { table, conditions, includes, orderBy, limit, offset }
 *
 * WASM runtime expects a more complex structure:
 * { table, branches, disjuncts, order_by, offset, include_deleted, array_subqueries, joins }
 */

import type { ColumnType, WasmSchema } from "../drivers/types.js";
import { analyzeRelations, type Relation } from "../codegen/relation-analyzer.js";

/**
 * Structure produced by QueryBuilder._build()
 */
interface BuilderOutput {
  table: string;
  conditions: Array<{ column: string; op: string; value: unknown }>;
  includes: Record<string, boolean | object>;
  orderBy: Array<[string, "asc" | "desc"]>;
  limit?: number;
  offset?: number;
}

function getColumnType(
  schema: WasmSchema,
  table: string,
  column: string,
): ColumnType["type"] | undefined {
  // All tables have an implicit UUID primary key `id`.
  if (column === "id") return "Uuid";
  const tableSchema = schema.tables[table];
  if (!tableSchema) return undefined;
  const col = tableSchema.columns.find((c) => c.name === column);
  return col?.column_type?.type;
}

/**
 * Map public QueryBuilder columns to runtime/internal column names.
 */
function toRuntimeColumn(column: string): string {
  // Runtime indices use "_id" for the implicit row id column.
  return column === "id" ? "_id" : column;
}

/**
 * Translate a JavaScript value to WasmValue format.
 */
function toWasmValue(value: unknown, columnType?: ColumnType["type"]): object {
  if (value === null || value === undefined) {
    return { Null: null };
  }
  if (typeof value === "boolean") {
    return { Boolean: value };
  }
  if (typeof value === "number") {
    if (columnType === "Timestamp") {
      return { Timestamp: value };
    }
    // Use Integer for all numbers - WASM will handle type coercion
    return { Integer: value };
  }
  if (typeof value === "string") {
    if (columnType === "Uuid") {
      return { Uuid: value };
    }
    return { Text: value };
  }
  throw new Error(`Unsupported value type: ${typeof value}`);
}

/**
 * Translate operator string to Condition enum variant.
 */
function toCondition(
  cond: { column: string; op: string; value: unknown },
  schema: WasmSchema,
  table: string,
): object {
  const columnType = getColumnType(schema, table, cond.column);
  const value = toWasmValue(cond.value, columnType);
  const runtimeColumn = toRuntimeColumn(cond.column);

  switch (cond.op) {
    case "eq":
      return { Eq: { column: runtimeColumn, value } };
    case "ne":
      return { Ne: { column: runtimeColumn, value } };
    case "gt":
      return { Gt: { column: runtimeColumn, value } };
    case "gte":
      return { Ge: { column: runtimeColumn, value } };
    case "lt":
      return { Lt: { column: runtimeColumn, value } };
    case "lte":
      return { Le: { column: runtimeColumn, value } };
    case "isNull":
      return { IsNull: { column: runtimeColumn } };
    case "contains":
      return { Contains: { column: runtimeColumn, value } };
    case "in":
      // Handle IN operator with array of values
      if (Array.isArray(cond.value)) {
        return {
          In: { column: runtimeColumn, values: cond.value.map((v) => toWasmValue(v, columnType)) },
        };
      }
      throw new Error(`"in" operator requires an array value`);
    default:
      throw new Error(`Unknown operator: ${cond.op}`);
  }
}

/**
 * Translate includes to array_subqueries for the WASM query format.
 *
 * @param includes Object mapping relation names to boolean or nested includes
 * @param tableName Current table name
 * @param relations Map from table name to relations on that table
 * @returns Array of array_subquery objects
 */
function toArraySubqueries(
  includes: Record<string, boolean | object>,
  tableName: string,
  relations: Map<string, Relation[]>,
): object[] {
  const tableRels = relations.get(tableName) || [];
  const subqueries: object[] = [];

  for (const [relName, spec] of Object.entries(includes)) {
    if (!spec) continue;

    const rel = tableRels.find((r) => r.name === relName);
    if (!rel) {
      throw new Error(`Unknown relation "${relName}" on table "${tableName}"`);
    }

    // Build the subquery based on relation type
    if (rel.type === "forward") {
      // Forward relation: todos.owner_id -> users.id
      // We join from the FK column to the target table's id
      subqueries.push({
        column_name: relName,
        table: rel.toTable,
        inner_column: "id",
        outer_column: `${tableName}.${rel.fromColumn}`,
        filters: [],
        joins: [],
        select_columns: null,
        order_by: [],
        limit: null,
        nested_arrays:
          typeof spec === "object"
            ? toArraySubqueries(spec as Record<string, boolean | object>, rel.toTable, relations)
            : [],
      });
    } else {
      // Reverse relation: users -> todos via todos.owner_id
      // We join from the target table's FK column to our id
      subqueries.push({
        column_name: relName,
        table: rel.toTable,
        inner_column: rel.toColumn,
        outer_column: `${tableName}.id`,
        filters: [],
        joins: [],
        select_columns: null,
        order_by: [],
        limit: null,
        nested_arrays:
          typeof spec === "object"
            ? toArraySubqueries(spec as Record<string, boolean | object>, rel.toTable, relations)
            : [],
      });
    }
  }

  return subqueries;
}

/**
 * Translate QueryBuilder JSON to WASM Query JSON.
 *
 * @param builderJson JSON string from QueryBuilder._build()
 * @param schema WasmSchema for relation analysis
 * @returns JSON string for WASM runtime query()
 */
export function translateQuery(builderJson: string, schema: WasmSchema): string {
  const builder: BuilderOutput = JSON.parse(builderJson);
  const relations = analyzeRelations(schema);

  const query = {
    table: builder.table,
    branches: [],
    disjuncts: [
      {
        conditions: builder.conditions.map((cond) => toCondition(cond, schema, builder.table)),
      },
    ],
    order_by: builder.orderBy.map(([col, dir]) => [
      col,
      dir === "desc" ? "Descending" : "Ascending",
    ]),
    offset: builder.offset ?? 0,
    limit: builder.limit ?? null,
    include_deleted: false,
    array_subqueries: toArraySubqueries(builder.includes, builder.table, relations),
    joins: [],
  };

  return JSON.stringify(query);
}
