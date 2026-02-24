/**
 * Translate QueryBuilder JSON to WASM Query format.
 *
 * QueryBuilder produces a compact JSON structure:
 * { table, conditions, includes, orderBy, limit, offset, hops?, gather? }
 *
 * Runtime semantics are driven by `relation_ir`. The wire payload keeps only
 * fields required for execution (`table`, `relation_ir`, and `array_subqueries`).
 */

import type { ColumnType, WasmSchema } from "../drivers/types.js";
import { analyzeRelations, type Relation } from "../codegen/relation-analyzer.js";
import type {
  RelColumnRef,
  RelExpr,
  RelJoinCondition,
  RelPredicateExpr,
  RelProjectColumn,
} from "../ir.js";

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
  hops?: string[];
  gather?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };
}

function relColumn(column: string, scope?: string): RelColumnRef {
  return scope ? { scope, column } : { column };
}

function relationColumnsForTable(
  table: string,
  scope: string,
  schema: WasmSchema,
): RelProjectColumn[] {
  const tableSchema = schema.tables[table];
  if (!tableSchema) {
    throw new Error(`Unknown table "${table}" in relation projection.`);
  }
  return [
    {
      alias: "id",
      expr: { type: "Column", column: relColumn("id", scope) },
    },
    ...tableSchema.columns.map((column) => ({
      alias: column.name,
      expr: { type: "Column", column: relColumn(column.name, scope) } as const,
    })),
  ];
}

function getColumnType(schema: WasmSchema, table: string, column: string): ColumnType | undefined {
  // All tables have an implicit UUID primary key `id`.
  if (column === "id") return { type: "Uuid" };
  const tableSchema = schema.tables[table];
  if (!tableSchema) return undefined;
  const col = tableSchema.columns.find((c) => c.name === column);
  return col?.column_type;
}

function stripQualifier(column: string): string {
  const parts = column.split(".");
  return parts[parts.length - 1] ?? column;
}

/**
 * Translate a JavaScript value to WasmValue format.
 */
function toWasmValue(value: unknown, columnType: ColumnType): object {
  if (value === null || value === undefined) {
    return { Null: null };
  }
  if (Array.isArray(value)) {
    if (columnType.type !== "Array") {
      throw new Error("Unexpected array value for scalar column");
    }
    return {
      Array: value.map((item) => toWasmValue(item, columnType.element)),
    };
  }
  if (typeof value === "boolean") {
    return { Boolean: value };
  }
  if (typeof value === "number") {
    if (columnType?.type === "Timestamp") {
      return { Timestamp: value };
    }
    // Use Integer for all numbers - WASM will handle type coercion
    return { Integer: value };
  }
  if (typeof value === "string") {
    if (columnType?.type === "Uuid") {
      return { Uuid: value };
    }
    if (columnType?.type === "Enum" && !columnType.variants.includes(value)) {
      throw new Error(
        `Invalid enum value "${value}". Expected one of: ${columnType.variants.join(", ")}`,
      );
    }
    return { Text: value };
  }
  throw new Error(`Unsupported value type: ${typeof value}`);
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

function conditionToRelPredicate(
  cond: { column: string; op: string; value: unknown },
  schema: WasmSchema,
  table: string,
  scope?: string,
): RelPredicateExpr {
  const columnRef = relColumn(stripQualifier(cond.column), scope);
  const column = stripQualifier(cond.column);
  const columnType = getColumnType(schema, table, column);
  if (!columnType) {
    throw new Error(`Unknown column "${column}" in table "${table}"`);
  }
  const valueTypeForCondition =
    cond.op === "contains" && columnType.type === "Array" ? columnType.element : columnType;
  const rightLiteral =
    isFrontierRowIdToken(cond.value) && cond.op === "eq"
      ? { type: "RowId" as const, source: "Frontier" as const }
      : {
          type: "Literal" as const,
          value: toWasmValue(cond.value, valueTypeForCondition),
        };
  switch (cond.op) {
    case "eq":
      return { type: "Cmp", left: columnRef, op: "Eq", right: rightLiteral };
    case "ne":
      return {
        type: "Cmp",
        left: columnRef,
        op: "Ne",
        right: { type: "Literal", value: cond.value },
      };
    case "gt":
      return {
        type: "Cmp",
        left: columnRef,
        op: "Gt",
        right: { type: "Literal", value: cond.value },
      };
    case "gte":
      return {
        type: "Cmp",
        left: columnRef,
        op: "Ge",
        right: { type: "Literal", value: cond.value },
      };
    case "lt":
      return {
        type: "Cmp",
        left: columnRef,
        op: "Lt",
        right: { type: "Literal", value: cond.value },
      };
    case "lte":
      return {
        type: "Cmp",
        left: columnRef,
        op: "Le",
        right: { type: "Literal", value: cond.value },
      };
    case "isNull":
      return { type: "IsNull", column: columnRef };
    case "contains":
      return { type: "Contains", left: columnRef, value: rightLiteral };
    case "in":
      if (!Array.isArray(cond.value)) {
        throw new Error('"in" operator requires an array value');
      }
      return {
        type: "In",
        left: columnRef,
        values: cond.value.map((value) => ({
          type: "Literal",
          value: toWasmValue(value, columnType),
        })),
      };
    default:
      throw new Error(`Unknown operator: ${cond.op}`);
  }
}

function isFrontierRowIdToken(value: unknown): value is { __jazz_ir_frontier_row_id: true } {
  if (typeof value !== "object" || value === null) {
    return false;
  }
  const marker = value as { __jazz_ir_frontier_row_id?: unknown };
  return marker.__jazz_ir_frontier_row_id === true;
}

function conditionsToRelPredicate(
  conditions: Array<{ column: string; op: string; value: unknown }>,
  schema: WasmSchema,
  table: string,
  scope?: string,
): RelPredicateExpr {
  if (conditions.length === 0) {
    return { type: "True" };
  }
  if (conditions.length === 1) {
    return conditionToRelPredicate(conditions[0], schema, table, scope);
  }
  return {
    type: "And",
    exprs: conditions.map((condition) => conditionToRelPredicate(condition, schema, table, scope)),
  };
}

function applyFilter(input: RelExpr, predicate: RelPredicateExpr): RelExpr {
  if (predicate.type === "True") {
    return input;
  }
  return { type: "Filter", input, predicate };
}

function lowerHopsToRelExpr(
  input: RelExpr,
  seedTable: string,
  hops: readonly string[],
  relations: Map<string, Relation[]>,
  schema: WasmSchema,
): RelExpr {
  if (hops.length === 0) {
    return input;
  }

  let currentExpr = input;
  let currentTable = seedTable;
  let currentScope = seedTable;

  for (let i = 0; i < hops.length; i += 1) {
    const hopName = hops[i];
    const tableRelations = relations.get(currentTable) ?? [];
    const relation = tableRelations.find((candidate) => candidate.name === hopName);
    if (!relation) {
      throw new Error(`Unknown relation "${hopName}" on table "${currentTable}"`);
    }

    const hopAlias = `__hop_${i}`;
    const joinOn: RelJoinCondition =
      relation.type === "forward"
        ? {
            left: relColumn(relation.fromColumn, currentScope),
            right: relColumn("id", hopAlias),
          }
        : {
            left: relColumn("id", currentScope),
            right: relColumn(relation.toColumn, hopAlias),
          };
    currentExpr = {
      type: "Join",
      left: currentExpr,
      right: { type: "TableScan", table: relation.toTable },
      on: [joinOn],
      joinKind: "Inner",
    };

    currentTable = relation.toTable;
    currentScope = hopAlias;
  }

  return {
    type: "Project",
    input: currentExpr,
    columns: relationColumnsForTable(currentTable, currentScope, schema),
  };
}

function gatherToRelExpr(
  gather: NonNullable<BuilderOutput["gather"]>,
  seedTable: string,
  seedExpr: RelExpr,
  relations: Map<string, Relation[]>,
  schema: WasmSchema,
): RelExpr {
  if (!schema.tables[gather.step_table]) {
    throw new Error(`Unknown gather step table "${gather.step_table}"`);
  }
  if (!Number.isInteger(gather.max_depth) || gather.max_depth <= 0) {
    throw new Error("gather(...) max_depth must be a positive integer.");
  }

  const stepHops = Array.isArray(gather.step_hops)
    ? gather.step_hops.filter((hop): hop is string => typeof hop === "string")
    : [];
  if (stepHops.length !== 1) {
    throw new Error("gather(...) currently requires exactly one hopTo(...) step.");
  }

  const stepRelations = relations.get(gather.step_table) ?? [];
  const hopName = stepHops[0];
  const hopRelation = stepRelations.find((rel) => rel.name === hopName);
  if (!hopRelation) {
    throw new Error(`Unknown relation "${hopName}" on table "${gather.step_table}"`);
  }
  if (hopRelation.type !== "forward") {
    throw new Error("gather(...) currently only supports forward hopTo(...) relations.");
  }
  if (hopRelation.toTable !== seedTable) {
    throw new Error(
      `gather(...) step must hop back to "${seedTable}" rows, got "${hopRelation.toTable}".`,
    );
  }

  const stepBase: RelExpr = { type: "TableScan", table: gather.step_table };
  const stepConditions = Array.isArray(gather.step_conditions) ? gather.step_conditions : [];
  const stepScope = gather.step_table;
  const stepPredicateConditions = [
    ...stepConditions,
    {
      column: stripQualifier(gather.step_current_column),
      op: "eq",
      value: { __jazz_ir_frontier_row_id: true },
    },
  ];
  const stepPredicate = conditionsToRelPredicate(
    stepPredicateConditions,
    schema,
    gather.step_table,
    stepScope,
  );
  const stepFiltered = applyFilter(stepBase, stepPredicate);

  const recursiveHopAlias = "__recursive_hop_0";
  const stepJoined: RelExpr = {
    type: "Join",
    left: stepFiltered,
    right: { type: "TableScan", table: hopRelation.toTable },
    on: [
      {
        left: relColumn(hopRelation.fromColumn, gather.step_table),
        right: relColumn("id", recursiveHopAlias),
      },
    ],
    joinKind: "Inner",
  };

  const stepProjected: RelExpr = {
    type: "Project",
    input: stepJoined,
    columns: relationColumnsForTable(seedTable, recursiveHopAlias, schema),
  };

  return {
    type: "Gather",
    seed: seedExpr,
    step: stepProjected,
    frontierKey: { type: "RowId", source: "Current" },
    maxDepth: gather.max_depth,
    dedupeKey: [{ type: "RowId", source: "Current" }],
  };
}

/**
 * Translate QueryBuilder JSON to relation IR.
 *
 * This emits the canonical compositional form:
 * - hopTo => Join + Project
 * - gather => Gather with step Join + Project
 */
export function translateBuilderToRelationIr(builderJson: string, schema: WasmSchema): RelExpr {
  const builder: BuilderOutput = JSON.parse(builderJson);
  const relations = analyzeRelations(schema);
  const hops = Array.isArray(builder.hops)
    ? builder.hops.filter((hop): hop is string => typeof hop === "string")
    : [];

  if (builder.gather && Object.keys(builder.includes ?? {}).length > 0) {
    throw new Error("gather(...) does not yet support include(...).");
  }
  if (hops.length > 0 && Object.keys(builder.includes ?? {}).length > 0) {
    throw new Error("hopTo(...) does not yet support include(...).");
  }

  let relation: RelExpr = { type: "TableScan", table: builder.table };
  relation = applyFilter(
    relation,
    conditionsToRelPredicate(builder.conditions ?? [], schema, builder.table, builder.table),
  );

  if (builder.gather) {
    relation = gatherToRelExpr(builder.gather, builder.table, relation, relations, schema);
  }
  relation = lowerHopsToRelExpr(relation, builder.table, hops, relations, schema);

  if (Array.isArray(builder.orderBy) && builder.orderBy.length > 0) {
    relation = {
      type: "OrderBy",
      input: relation,
      terms: builder.orderBy.map(([column, direction]) => ({
        column: relColumn(column),
        direction: direction === "desc" ? "Desc" : "Asc",
      })),
    };
  }

  if (typeof builder.offset === "number" && builder.offset > 0) {
    relation = {
      type: "Offset",
      input: relation,
      offset: builder.offset,
    };
  }
  if (typeof builder.limit === "number") {
    relation = {
      type: "Limit",
      input: relation,
      limit: builder.limit,
    };
  }

  return relation;
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
  const relation = translateBuilderToRelationIr(builderJson, schema);
  const query = {
    table: builder.table,
    array_subqueries: toArraySubqueries(builder.includes ?? {}, builder.table, relations),
    relation_ir: relation,
  };

  return JSON.stringify(query);
}
