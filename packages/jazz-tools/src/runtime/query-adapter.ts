/**
 * Translate QueryBuilder JSON to WASM Query format.
 *
 * QueryBuilder produces a simple JSON structure:
 * { table, conditions, includes, orderBy, limit, offset, gather? }
 *
 * WASM runtime expects a more complex structure:
 * { table, branches, disjuncts, order_by, offset, include_deleted, array_subqueries, joins, recursive? }
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

interface RecursiveOutput {
  table: string;
  inner_column: string;
  outer_column: string;
  select_columns: string[] | null;
  filters: object[];
  joins: JoinOutput[];
  result_element_index?: number;
  max_depth: number;
}

interface JoinOutput {
  table: string;
  alias: string | null;
  on: [string, string];
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
 * Map public QueryBuilder columns to runtime/internal column names.
 */
function toRuntimeColumn(column: string): string {
  // Runtime indices use "_id" for the implicit row id column.
  return column === "id" ? "_id" : column;
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
  const column = stripQualifier(cond.column);
  const columnType = getColumnType(schema, table, column);
  if (!columnType) {
    throw new Error(`Unknown column "${column}" in table "${table}"`);
  }
  const valueTypeForCondition =
    cond.op === "contains" && columnType.type === "Array" ? columnType.element : columnType;
  const value = toWasmValue(cond.value, valueTypeForCondition);
  const runtimeColumn = toRuntimeColumn(column);

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

function conditionToRelPredicate(
  cond: { column: string; op: string; value: unknown },
  scope?: string,
): RelPredicateExpr {
  const columnRef = relColumn(stripQualifier(cond.column), scope);
  const rightLiteral =
    isFrontierRowIdToken(cond.value) && cond.op === "eq"
      ? { type: "RowId" as const, source: "Frontier" as const }
      : { type: "Literal" as const, value: cond.value };
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
      return { type: "Contains", left: columnRef, value: { type: "Literal", value: cond.value } };
    case "in":
      if (!Array.isArray(cond.value)) {
        throw new Error('"in" operator requires an array value');
      }
      return {
        type: "In",
        left: columnRef,
        values: cond.value.map((value) => ({ type: "Literal", value })),
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
  scope?: string,
): RelPredicateExpr {
  if (conditions.length === 0) {
    return { type: "True" };
  }
  if (conditions.length === 1) {
    return conditionToRelPredicate(conditions[0], scope);
  }
  return {
    type: "And",
    exprs: conditions.map((condition) => conditionToRelPredicate(condition, scope)),
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
  const stepPredicate = conditionsToRelPredicate(stepPredicateConditions, stepScope);
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
  if (hops.length > 0 && builder.gather) {
    throw new Error("gather(...).hopTo(...) is not yet supported.");
  }
  if (hops.length > 0 && Object.keys(builder.includes ?? {}).length > 0) {
    throw new Error("hopTo(...) does not yet support include(...).");
  }

  let relation: RelExpr = { type: "TableScan", table: builder.table };
  relation = applyFilter(
    relation,
    conditionsToRelPredicate(builder.conditions ?? [], builder.table),
  );

  if (builder.gather) {
    relation = gatherToRelExpr(builder.gather, builder.table, relation, relations, schema);
  } else {
    relation = lowerHopsToRelExpr(relation, builder.table, hops, relations, schema);
  }

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

interface BuilderCondition {
  column: string;
  op: string;
  value: unknown;
}

interface LinearJoinInfo {
  baseTable: string;
  baseScope: string;
  conditions: BuilderCondition[];
  joins: JoinOutput[];
}

interface RuntimeCorePlan {
  table: string;
  conditions: BuilderCondition[];
  joins: JoinOutput[];
  resultElementIndex: number | null;
  recursive?: RecursiveOutput;
}

interface QueryEnvelope {
  core: RelExpr;
  orderBy: Array<[string, "Ascending" | "Descending"]>;
  offset: number;
  limit: number | null;
}

function builderCmpOp(op: string): string {
  switch (op) {
    case "Eq":
      return "eq";
    case "Ne":
      return "ne";
    case "Gt":
      return "gt";
    case "Ge":
      return "gte";
    case "Lt":
      return "lt";
    case "Le":
      return "lte";
    default:
      throw new Error(`Unsupported relation comparison op "${op}" for runtime query lowering.`);
  }
}

function builderColumn(column: RelColumnRef): string {
  return column.scope ? `${column.scope}.${column.column}` : column.column;
}

function flattenPredicateTerms(predicate: RelPredicateExpr): RelPredicateExpr[] {
  if (predicate.type === "And") {
    return predicate.exprs.flatMap((expr) => flattenPredicateTerms(expr));
  }
  return [predicate];
}

function predicateTermToBuilderCondition(predicate: RelPredicateExpr): BuilderCondition | null {
  switch (predicate.type) {
    case "Cmp":
      if (predicate.right.type !== "Literal") {
        throw new Error("Only literal Cmp values are supported in runtime query lowering.");
      }
      return {
        column: builderColumn(predicate.left),
        op: builderCmpOp(predicate.op),
        value: predicate.right.value,
      };
    case "IsNull":
      return {
        column: builderColumn(predicate.column),
        op: "isNull",
        value: true,
      };
    case "IsNotNull":
      return {
        column: builderColumn(predicate.column),
        op: "isNull",
        value: false,
      };
    case "In":
      if (predicate.values.some((value) => value.type !== "Literal")) {
        throw new Error("Only literal IN values are supported in runtime query lowering.");
      }
      return {
        column: builderColumn(predicate.left),
        op: "in",
        value: predicate.values.map((value) =>
          value.type === "Literal" ? value.value : undefined,
        ),
      };
    case "Contains":
      if (predicate.value.type !== "Literal") {
        throw new Error("Only literal CONTAINS values are supported in runtime query lowering.");
      }
      return {
        column: builderColumn(predicate.left),
        op: "contains",
        value: predicate.value.value,
      };
    case "True":
      return null;
    default:
      throw new Error(
        `Predicate "${predicate.type}" is not supported in runtime query condition lowering.`,
      );
  }
}

function relationPredicateToBuilderConditions(predicate: RelPredicateExpr): BuilderCondition[] {
  const terms = flattenPredicateTerms(predicate);
  return terms
    .map((term) => predicateTermToBuilderCondition(term))
    .filter((condition): condition is BuilderCondition => Boolean(condition));
}

function extractLinearJoinInfo(expr: RelExpr): LinearJoinInfo {
  switch (expr.type) {
    case "TableScan":
      return {
        baseTable: expr.table,
        baseScope: expr.table,
        conditions: [],
        joins: [],
      };
    case "Filter": {
      const inner = extractLinearJoinInfo(expr.input);
      return {
        ...inner,
        conditions: [...inner.conditions, ...relationPredicateToBuilderConditions(expr.predicate)],
      };
    }
    case "Join": {
      if (expr.right.type !== "TableScan") {
        throw new Error("Runtime query lowering currently requires table-scan join RHS.");
      }
      const left = extractLinearJoinInfo(expr.left);
      const firstJoin = expr.on[0];
      if (!firstJoin) {
        throw new Error("Runtime query lowering requires explicit join conditions.");
      }
      const leftScope = firstJoin.left.scope ?? left.baseScope;
      const rightScope = firstJoin.right.scope ?? expr.right.table;
      return {
        ...left,
        baseScope: rightScope,
        joins: [
          ...left.joins,
          {
            table: expr.right.table,
            alias: rightScope === expr.right.table ? null : rightScope,
            on: [
              `${leftScope}.${firstJoin.left.column}`,
              `${rightScope}.${firstJoin.right.column}`,
            ],
          },
        ],
      };
    }
    default:
      throw new Error(`Runtime query lowering cannot linearize relation node type "${expr.type}".`);
  }
}

function extractStepScan(
  expr: RelExpr,
  predicates: RelPredicateExpr[] = [],
): { table: string; predicates: RelPredicateExpr[] } {
  if (expr.type === "TableScan") {
    return { table: expr.table, predicates };
  }
  if (expr.type === "Filter") {
    return extractStepScan(expr.input, [...predicates, expr.predicate]);
  }
  throw new Error("Gather step must start from a filtered table scan.");
}

function parseGatherCore(
  core: Extract<RelExpr, { type: "Gather" }>,
  schema: WasmSchema,
): RuntimeCorePlan {
  const seed = extractLinearJoinInfo(core.seed);
  if (seed.joins.length > 0) {
    throw new Error("Gather seed cannot include joins in runtime query lowering.");
  }

  if (core.step.type !== "Project" || core.step.input.type !== "Join") {
    throw new Error("Gather step must lower to Project(Join(...)) shape.");
  }
  const stepJoin = core.step.input;
  if (stepJoin.right.type !== "TableScan") {
    throw new Error("Gather step join RHS must be a table scan.");
  }

  const stepScan = extractStepScan(stepJoin.left);
  let frontierColumn: string | undefined;
  const stepConditions: BuilderCondition[] = [];
  for (const predicate of stepScan.predicates.flatMap((expr) => flattenPredicateTerms(expr))) {
    if (
      predicate.type === "Cmp" &&
      predicate.op === "Eq" &&
      predicate.right.type === "RowId" &&
      predicate.right.source === "Frontier"
    ) {
      frontierColumn = predicate.left.column;
      continue;
    }
    stepConditions.push(...relationPredicateToBuilderConditions(predicate));
  }

  if (!frontierColumn) {
    throw new Error("Gather step predicate must include a frontier row-id comparison.");
  }

  const firstJoin = stepJoin.on[0];
  if (!firstJoin) {
    throw new Error("Gather step join requires an explicit ON predicate.");
  }
  const leftScope = firstJoin.left.scope ?? stepScan.table;
  const rightScope = firstJoin.right.scope ?? stepJoin.right.table;

  return {
    table: seed.baseTable,
    conditions: seed.conditions,
    joins: [],
    resultElementIndex: null,
    recursive: {
      table: stepScan.table,
      inner_column: toRuntimeColumn(stripQualifier(frontierColumn)),
      outer_column: "_id",
      select_columns: null,
      filters: stepConditions.map((condition) => toCondition(condition, schema, stepScan.table)),
      joins: [
        {
          table: stepJoin.right.table,
          alias: rightScope === stepJoin.right.table ? null : rightScope,
          on: [`${leftScope}.${firstJoin.left.column}`, `${rightScope}.${firstJoin.right.column}`],
        },
      ],
      result_element_index: 1,
      max_depth: core.maxDepth,
    },
  };
}

function parseRuntimeCorePlan(core: RelExpr, schema: WasmSchema): RuntimeCorePlan {
  if (core.type === "Gather") {
    return parseGatherCore(core, schema);
  }

  const linear =
    core.type === "Project" ? extractLinearJoinInfo(core.input) : extractLinearJoinInfo(core);
  return {
    table: linear.baseTable,
    conditions: linear.conditions,
    joins: linear.joins,
    resultElementIndex: core.type === "Project" ? linear.joins.length : null,
  };
}

function unwrapQueryEnvelope(expr: RelExpr): QueryEnvelope {
  let current = expr;
  let orderBy: Array<[string, "Ascending" | "Descending"]> = [];
  let offset = 0;
  let limit: number | null = null;

  while (true) {
    switch (current.type) {
      case "OrderBy":
        if (orderBy.length === 0) {
          orderBy = current.terms.map((term) => [
            term.column.column,
            term.direction === "Desc" ? "Descending" : "Ascending",
          ]);
        }
        current = current.input;
        break;
      case "Offset":
        offset = current.offset;
        current = current.input;
        break;
      case "Limit":
        limit = current.limit;
        current = current.input;
        break;
      default:
        return {
          core: current,
          orderBy,
          offset,
          limit,
        };
    }
  }
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
  const envelope = unwrapQueryEnvelope(relation);
  const corePlan = parseRuntimeCorePlan(envelope.core, schema);

  const query = {
    table: corePlan.table,
    branches: [],
    disjuncts: [
      {
        conditions: corePlan.conditions.map((cond) => toCondition(cond, schema, corePlan.table)),
      },
    ],
    order_by: envelope.orderBy,
    offset: envelope.offset,
    limit: envelope.limit,
    include_deleted: false,
    array_subqueries: toArraySubqueries(builder.includes ?? {}, corePlan.table, relations),
    joins: corePlan.joins,
    ...(corePlan.resultElementIndex !== null
      ? { result_element_index: corePlan.resultElementIndex }
      : {}),
    ...(corePlan.recursive ? { recursive: corePlan.recursive } : {}),
  };

  return JSON.stringify(query);
}
