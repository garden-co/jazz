import { APP_COLUMN_PREFIX, type Value } from "../../_extern/model-query.js";
import { uuidFromString } from "../../support/hash.js";
import { SortedMap, cmpStr } from "../../support/collections.js";
import {
  structEq,
  type JsonValue,
  type RelationColumnRef,
  type RelationExpr,
  type RelationOrderBy,
  type RelationPredicate,
  type RelationProjectColumn,
  type RelationQuery,
  type RelationUnionArm,
  type RelationValueRef,
} from "./ast.js";
import {
  filter,
  limit as withLimit,
  offset as withOffset,
  orderBy,
  queryFrom,
} from "./builders.js";
import {
  authorClaimPathKey,
  providerClaimOperandKey,
  type JoinVia,
  type Operand,
  type OrderBy,
  type OrderDirection,
  type Predicate,
} from "./policy_and_request_contracts.js";
import type { Query } from "./ast.js";
import { QueryError } from "./validation.js";

interface RelationFacadeJoin {
  left_scope: string;
  left_column: string;
  right_scope: string;
  right_column: string;
}

interface RelationFacadePlan {
  tablesByScope: Map<string, string>;
  filtersByScope: Map<string, Predicate[]>;
  joins: RelationFacadeJoin[];
  outputScope?: string;
  outputProjection?: RelationProjectColumn[];
  orderBy: [string, OrderDirection][];
  limit?: number;
  offset: number;
}

const newPlan = (): RelationFacadePlan => ({
  tablesByScope: new Map(),
  filtersByScope: new Map(),
  joins: [],
  orderBy: [],
  offset: 0,
});

type OutputStep = { kind: "Offset" | "Limit"; value: number };

/** Compose one output window around an already-composed inner window. */
function foldOutputStep(
  [offset, limit]: [number, number | undefined],
  step: OutputStep,
): [number, number | undefined] {
  if (step.kind === "Offset")
    return [offset + step.value, limit === undefined ? undefined : Math.max(0, limit - step.value)];
  return [offset, limit === undefined ? step.value : Math.min(limit, step.value)];
}

const unificationError = (message: string) =>
  new QueryError({ kind: "UnsupportedRelationQuery", detail: message });
const isFalse = (p: Predicate) => p.kind === "Any" && p.predicates.length === 0;
const FALSE = (): Predicate => ({ kind: "Any", predicates: [] });
const utf8Len = (s: string) => new TextEncoder().encode(s).length;
const validUnionLabel = (label: string, seen: Set<string>) => {
  if (!label || utf8Len(label) > 4096 || label.includes("\0") || seen.has(label)) return false;
  seen.add(label);
  return true;
};

/** Normalize the supported relation facade subset into the ordinary query shape. */
export function relationQueryToQuery(query: RelationQuery): Query {
  const parts = relationUnionParts(query.rel);
  if (parts) {
    if (!parts.inputs.length) throw unificationError("union requires at least one labeled input");
    const labels = new Set<string>();
    let output: string | undefined;
    for (const arm of parts.inputs) {
      if (!validUnionLabel(arm.label, labels))
        throw unificationError("union arm labels must be 1..=4096 bytes, NUL-free, and unique");
      const armQuery = relationQueryToQuery({ rel: arm.input });
      if (output !== undefined && output !== armQuery.table)
        throw unificationError("union inputs must output the same table");
      output ??= armQuery.table;
    }
    const out = queryFrom(output!);
    out.relation = structuredClone(query);
    return out;
  }
  const gathered = relationGatherToQuery(query.rel);
  if (gathered) return gathered;

  const plan = newPlan();
  collectRelationFacade(query.rel, plan);
  const outputScope = plan.outputScope;
  if (outputScope === undefined)
    throw unificationError("relation query must end in a project over one output table");
  const outputTable = plan.tablesByScope.get(outputScope);
  if (outputTable === undefined) throw unificationError("relation query output scope is unknown");
  const out = queryFrom(outputTable);
  out.relation = structuredClone(query);
  for (const f of plan.filtersByScope.get(outputScope) ?? []) filter(out, f);
  plan.filtersByScope.delete(outputScope);
  if (plan.joins.length) out.joins.push(relationPathJoinFromOutput(plan, outputScope));
  if (plan.filtersByScope.size)
    throw unificationError("relation query filters on non-adjacent scopes are not unified yet");
  for (const [column, direction] of plan.orderBy) orderBy(out, column, direction);
  if (plan.limit !== undefined) withLimit(out, plan.limit);
  if (plan.offset !== 0) withOffset(out, plan.offset);
  return out;
}

/** The terminal relation projection and its source scope. */
export function relationOutputProjection(
  relation: RelationQuery,
): [string, RelationProjectColumn[]] {
  if (relationUnionParts(relation.rel)) {
    const found = relationProjectionFromExpr(relation.rel);
    if (!found) throw unificationError("relation query must define an output projection");
    validateRelationProjectionAliases(found[1]);
    // A UNION always retains its arm projections.
    rejectReservedRelationAliases(found[1]);
    return found;
  }
  const plan = newPlan();
  collectRelationFacade(relation.rel, plan);
  if (plan.outputScope === undefined)
    throw unificationError("relation query must end in a project over one output table");
  if (!plan.outputProjection)
    throw unificationError("relation query must define an output projection");
  return [plan.outputScope, plan.outputProjection];
}

/** Explicit output aliases, or undefined when a supported Gather keeps the table's row shape. */
export function relationOutputProjectionIfPresent(
  relation: RelationQuery,
): RelationProjectColumn[] | undefined {
  if (peelRelationOutputSteps(relation.rel).base.kind === "Gather") return undefined;
  return relationOutputProjection(relation)[1];
}

/** Projection for every leaf of a retained relation UNION, keyed by composed arm path. */
export function relationUnionLeafProjections(
  relation: RelationQuery,
): SortedMap<string, RelationProjectColumn[]> {
  const out = new SortedMap<string, RelationProjectColumn[]>(cmpStr);
  collectUnionLeafProjections(relation.rel, undefined, undefined, out);
  return out;
}

function collectUnionLeafProjections(
  expr: RelationExpr,
  label: string | undefined,
  inherited: RelationProjectColumn[] | undefined,
  out: SortedMap<string, RelationProjectColumn[]>,
): void {
  const insert = (columns: RelationProjectColumn[]) => {
    if (out.set(label!, columns) !== undefined)
      throw unificationError("UNION leaf projection labels must be unique");
  };
  switch (expr.kind) {
    case "Union":
      for (const arm of expr.inputs) {
        const armLabel = label === undefined ? arm.label : composeUnionArmPath(label, arm.label);
        collectUnionLeafProjections(arm.input, armLabel, inherited && [...inherited], out);
      }
      return;
    case "Project":
      if (relationExprContainsUnion(expr.input))
        return collectUnionLeafProjections(expr.input, label, inherited ?? expr.columns, out);
      if (label === undefined) throw unificationError("UNION leaf projection has no arm label");
      return insert(inherited ?? expr.columns);
    case "Filter":
    case "OrderBy":
    case "Offset":
    case "Limit":
      return collectUnionLeafProjections(expr.input, label, inherited, out);
    default: {
      if (label === undefined) throw unificationError("UNION leaf has no output projection");
      const columns = inherited ?? relationProjectionFromExpr(expr)?.[1];
      if (!columns) throw unificationError("UNION leaf has no output projection");
      return insert(columns);
    }
  }
}

function relationExprContainsUnion(expr: RelationExpr): boolean {
  switch (expr.kind) {
    case "Union":
      return true;
    case "Project":
    case "Filter":
    case "OrderBy":
    case "Offset":
    case "Limit":
      return relationExprContainsUnion(expr.input);
    case "Join":
      return relationExprContainsUnion(expr.left) || relationExprContainsUnion(expr.right);
    default:
      return false;
  }
}

/** Encode a nested UNION arm path without relying on a user label separator. */
export const composeUnionArmPath = (prefix: string, label: string): string =>
  `${utf8Len(prefix)}:${prefix}${utf8Len(label)}:${label}`;

function relationProjectionFromExpr(
  expr: RelationExpr,
): [string, RelationProjectColumn[]] | undefined {
  switch (expr.kind) {
    case "Project": {
      const scope = expr.columns.find(
        (c) => c.expr.kind === "Column" && c.expr.column.scope !== undefined,
      );
      if (scope && scope.expr.kind === "Column") return [scope.expr.column.scope!, expr.columns];
      return relationProjectionFromExpr(expr.input);
    }
    case "Filter":
    case "OrderBy":
    case "Offset":
    case "Limit":
      return relationProjectionFromExpr(expr.input);
    case "Union":
      return expr.inputs.length ? relationProjectionFromExpr(expr.inputs[0].input) : undefined;
    default:
      return undefined;
  }
}

// Engine carrier names a retained projection alias must not take.
const RESERVED_RELATION_ALIASES = ["row_uuid", "tx_time", "tx_node_id"];
const reservedAliasPrefixes = () => ["$", APP_COLUMN_PREFIX, "__", "left.", "right."];

/** Structural alias rules shared by every relation projection. */
export function validateRelationProjectionAliases(columns: RelationProjectColumn[]): void {
  const aliases = new Set<string>();
  for (const { alias } of columns) {
    if (!alias || alias.includes("\0") || aliases.has(alias))
      throw unificationError("relation project aliases must be non-empty, NUL-free, and unique");
    aliases.add(alias);
  }
}

/** Reject aliases of a retained projection that would replace an engine carrier. */
export function rejectReservedRelationAliases(columns: RelationProjectColumn[]): void {
  for (const { alias } of columns) {
    if (
      RESERVED_RELATION_ALIASES.includes(alias) ||
      reservedAliasPrefixes().some((p) => alias.startsWith(p))
    )
      throw new QueryError({
        kind: "UnsupportedRelationQuery",
        detail:
          `relation project alias ${JSON.stringify(alias)} is a reserved internal name; aliases must not ` +
          `be row_uuid, tx_time or tx_node_id, or start with "$", "${APP_COLUMN_PREFIX}", "__", "left." or "right."`,
      });
  }
}

/** Order used when materializing a retained public UNION ALL result set. */
export function relationUnionPresentationOrder(relation: RelationQuery): OrderBy[] | undefined {
  return relationUnionParts(relation.rel)?.orderBy?.map((t) => ({
    column: t.column.column,
    direction: t.direction,
  }));
}

/** A retained UNION ALL and the terminal operators that must run over the whole union. */
export interface RelationUnionParts {
  inputs: RelationUnionArm[];
  orderBy?: RelationOrderBy[];
  offset?: number;
  limit?: number;
}
const hasTerminalSteps = (p: RelationUnionParts) =>
  p.orderBy !== undefined || p.offset !== undefined || p.limit !== undefined;

/** Split a public union into arms and its global order/window, in builder order only. */
export function relationUnionParts(expr: RelationExpr): RelationUnionParts | undefined {
  switch (expr.kind) {
    case "Union":
      return { inputs: structuredClone(expr.inputs) };
    case "Filter":
    case "Project": {
      const parts = relationUnionParts(expr.input);
      if (!parts || hasTerminalSteps(parts)) return undefined;
      parts.inputs = parts.inputs.map((arm) => ({
        label: arm.label,
        input:
          expr.kind === "Filter"
            ? { kind: "Filter", input: arm.input, predicate: structuredClone(expr.predicate) }
            : { kind: "Project", input: arm.input, columns: structuredClone(expr.columns) },
      }));
      return parts;
    }
    case "OrderBy": {
      const parts = relationUnionParts(expr.input);
      if (!parts || hasTerminalSteps(parts)) return undefined;
      parts.orderBy = structuredClone(expr.terms);
      return parts;
    }
    case "Offset": {
      const parts = relationUnionParts(expr.input);
      if (!parts || parts.offset !== undefined || parts.limit !== undefined) return undefined;
      parts.offset = expr.offset;
      return parts;
    }
    case "Limit": {
      const parts = relationUnionParts(expr.input);
      if (!parts || parts.limit !== undefined) return undefined;
      parts.limit = expr.limit;
      return parts;
    }
    default:
      return undefined;
  }
}

/** Normalize the canonical public `gather` shape into ordinary recursive reachability. */
function relationGatherToQuery(rel: RelationExpr): Query | undefined {
  const { base, filters, orderBy: terms, offset, limit } = peelRelationOutputSteps(rel);
  if (base.kind !== "Gather") return undefined;
  const currentRow = { kind: "RowId", row: "Current" };
  if (!structEq(base.frontier_key, currentRow) || !structEq(base.dedupe_key, [currentRow]))
    throw unificationError("gather requires current-row frontier and dedupe keys");

  const [seedTable, seedFilters] = relationGatherSeed(base.seed);
  const [edgeTable, edgeMemberColumn, edgeParentColumn, edgeFilters] = relationGatherStep(
    base.step,
    seedTable,
  );

  const query = queryFrom(seedTable);
  for (const f of filters) {
    const converted = relationPredicateToQueryPredicate(f);
    if (!converted) continue;
    const [scope, p] = converted;
    if (scope && scope !== seedTable)
      throw unificationError("gather output filters must be scoped to the gathered table");
    filter(query, p);
  }
  query.reachable.push({
    // Each candidate output row is its own access row; the closure acts as a membership filter.
    access_table: seedTable,
    access_row_column: "id",
    access_team_column: "id",
    access_team_target: "RowId",
    from: { kind: "Literal", value: { kind: "Uuid", value: new Uint8Array(16) } },
    access_filters: [],
    edge_table: edgeTable,
    edge_member_column: edgeMemberColumn,
    edge_parent_column: edgeParentColumn,
    edge_filters: edgeFilters,
    bound: { ...base.bound },
    seed: { table: seedTable, team_column: "id", filters: seedFilters },
  });
  for (const order of terms) {
    if (order.column.scope !== undefined && order.column.scope !== seedTable)
      throw unificationError("gather order_by must be scoped to the gathered table");
    orderBy(query, order.column.column, order.direction);
  }
  if (limit !== undefined) withLimit(query, limit);
  if (offset !== 0) withOffset(query, offset);
  return query;
}

function peelRelationOutputSteps(expr: RelationExpr) {
  const filters: RelationPredicate[] = [];
  const terms: RelationOrderBy[] = [];
  const steps: OutputStep[] = [];
  let current = expr;
  for (;;) {
    if (filters.length && (current.kind === "Offset" || current.kind === "Limit"))
      throw unificationError("gather output filters cannot wrap limit or offset");
    if (current.kind === "Filter") filters.push(current.predicate);
    else if (current.kind === "OrderBy") terms.push(...current.terms);
    else if (current.kind === "Offset") steps.push({ kind: "Offset", value: current.offset });
    else if (current.kind === "Limit") steps.push({ kind: "Limit", value: current.limit });
    else break;
    current = current.input;
  }
  const [offset, limit] = steps
    .reverse()
    .reduce(foldOutputStep, [0, undefined] as [number, number | undefined]);
  return { base: current, filters, orderBy: terms, offset, limit };
}

function relationGatherSeed(seed: RelationExpr): [string, Predicate[]] {
  const filters: Predicate[] = [];
  let current = seed;
  while (current.kind === "Filter") {
    const converted = relationPredicateToQueryPredicate(current.predicate);
    const input: RelationExpr = current.input;
    if (converted) {
      if (input.kind !== "TableScan")
        throw unificationError("gather seed filters must be directly over one table scan");
      if (converted[0] !== (input.alias ?? input.table))
        throw unificationError("gather seed filters must be scoped to the seed table");
      filters.push(converted[1]);
    }
    current = input;
  }
  if (current.kind !== "TableScan")
    throw unificationError("gather seed must be a table scan with optional filters");
  if (current.alias !== undefined)
    throw unificationError("gather seed aliases are not unified yet");
  return [current.table, filters];
}

function relationGatherStep(
  step: RelationExpr,
  seedTable: string,
): [string, string, string, Predicate[]] {
  if (step.kind !== "Project")
    throw unificationError("gather step must project its forward-hop target");
  const join = step.input;
  if (join.kind !== "Join" || join.join_kind !== "Inner")
    throw unificationError("gather step must be an inner forward-hop join");
  const scan = relationGatherStepScan(join.left);
  if (scan.kind !== "TableScan")
    throw new Error("relation_gather_step_scan only returns table scans");
  const edgeScope = scan.alias ?? scan.table;
  const right = join.right;
  if (right.kind !== "TableScan" || right.alias === undefined)
    throw unificationError("gather step target must use a scoped table scan");
  const targetAlias = right.alias;
  if (right.table !== seedTable)
    throw unificationError("gather step must return rows from the seed table");
  if (join.on.length !== 1)
    throw unificationError("gather step requires exactly one forward-hop join condition");
  const condition = join.on[0];
  if (
    condition.left.scope !== edgeScope ||
    condition.right.scope !== targetAlias ||
    condition.right.column !== "id"
  )
    throw unificationError("gather step join must connect its table FK to the target row id");
  if (step.columns.some((c) => c.expr.kind !== "Column" || c.expr.column.scope !== targetAlias))
    throw unificationError("gather step must project only its forward-hop target");
  // The next frontier is the hop target's row id; an `id` alias must still be that id.
  if (
    step.columns.some(
      (c) => c.alias === "id" && !(c.expr.kind === "Column" && c.expr.column.column === "id"),
    )
  )
    throw unificationError("gather step must not project another target column as id");

  const isFrontierCmp = (p: RelationPredicate) =>
    p.kind === "Cmp" &&
    p.op === "Eq" &&
    p.right.kind === "RowId" &&
    p.right.row === "Frontier" &&
    p.left.scope === edgeScope;
  const filters = relationGatherStepFilters(join.left);
  const frontier = filters.find(isFrontierCmp);
  if (!frontier || frontier.kind !== "Cmp")
    throw unificationError("gather step must compare one edge column to the frontier row id");
  const edgeFilters: Predicate[] = [];
  for (const p of filters) {
    if (isFrontierCmp(p)) continue;
    const converted = relationPredicateToQueryPredicate(p);
    if (!converted) continue;
    if (converted[0] !== edgeScope)
      throw unificationError("gather step filters must be scoped to the edge table");
    edgeFilters.push(converted[1]);
  }
  return [scan.table, frontier.left.column, condition.left.column, edgeFilters];
}

function relationGatherStepScan(input: RelationExpr): RelationExpr {
  let current = input;
  while (current.kind === "Filter") current = current.input;
  if (current.kind === "TableScan") return current;
  throw unificationError("gather step filters must be directly over one table scan");
}

function relationGatherStepFilters(input: RelationExpr): RelationPredicate[] {
  const out: RelationPredicate[] = [];
  const flatten = (p: RelationPredicate): void => {
    if (p.kind === "And") p.predicates.forEach(flatten);
    else if (p.kind !== "True") out.push(structuredClone(p));
  };
  let current = input;
  while (current.kind === "Filter") {
    flatten(current.predicate);
    current = current.input;
  }
  return out;
}

function relationPathJoinFromOutput(plan: RelationFacadePlan, outputScope: string): JoinVia {
  const path: [string, RelationFacadeJoin][] = [];
  let current = outputScope;
  let previous: string | undefined;
  for (;;) {
    const incident = plan.joins
      .filter((j) => j.left_scope === current || j.right_scope === current)
      .filter(
        (j) => previous === undefined || (j.left_scope !== previous && j.right_scope !== previous),
      );
    if (!incident.length) break;
    if (incident.length > 1)
      throw unificationError("relation query joins must form one output-rooted path in this slice");
    const join = incident[0];
    const next = join.left_scope === current ? join.right_scope : join.left_scope;
    path.push([next, join]);
    previous = current;
    current = next;
  }
  if (path.length !== plan.joins.length)
    throw unificationError(
      "relation query joins must connect directly to the output scope in this slice",
    );
  if (!path.length) throw unificationError("relation query join path is empty");
  return buildRelationPathJoin(plan, outputScope, path, 0);
}

function buildRelationPathJoin(
  plan: RelationFacadePlan,
  leftScope: string,
  path: [string, RelationFacadeJoin][],
  index: number,
): JoinVia {
  const entry = path[index];
  if (!entry) throw unificationError("relation query join path ended unexpectedly");
  const [rightScope, relationJoin] = entry;
  const [leftColumn, rightColumn] = relationJoinColumns(relationJoin, leftScope, rightScope);
  const joinTable = plan.tablesByScope.get(rightScope);
  if (joinTable === undefined) throw unificationError("relation query join scope is unknown");
  const filters = plan.filtersByScope.get(rightScope) ?? [];
  plan.filtersByScope.delete(rightScope);
  const join: JoinVia = {
    table: joinTable,
    on_column: rightColumn,
    target: "Column",
    source_column: leftColumn !== "id" ? leftColumn : undefined,
    correlated_filters: [],
    filters,
    nested_joins: [],
  };
  if (index + 1 < path.length)
    join.nested_joins.push(buildRelationPathJoin(plan, rightScope, path, index + 1));
  return join;
}

function relationJoinColumns(
  join: RelationFacadeJoin,
  leftScope: string,
  rightScope: string,
): [string, string] {
  if (join.left_scope === leftScope && join.right_scope === rightScope)
    return [join.left_column, join.right_column];
  if (join.right_scope === leftScope && join.left_scope === rightScope)
    return [join.right_column, join.left_column];
  throw unificationError("relation query join path contains a non-adjacent edge");
}

function collectRelationFacade(expr: RelationExpr, plan: RelationFacadePlan): void {
  switch (expr.kind) {
    case "TableScan": {
      const scope = expr.alias ?? expr.table;
      const existing = plan.tablesByScope.get(scope);
      plan.tablesByScope.set(scope, expr.table);
      if (existing !== undefined && existing !== expr.table)
        throw unificationError("relation query reuses an alias for different tables");
      return;
    }
    case "Filter": {
      collectRelationFacade(expr.input, plan);
      const converted = relationPredicateToQueryPredicate(expr.predicate);
      if (converted) {
        const list = plan.filtersByScope.get(converted[0]) ?? [];
        list.push(converted[1]);
        plan.filtersByScope.set(converted[0], list);
      }
      return;
    }
    case "Join":
      if (expr.join_kind !== "Inner")
        throw unificationError("left relation joins are not unified yet");
      collectRelationFacade(expr.left, plan);
      collectRelationFacade(expr.right, plan);
      for (const c of expr.on)
        plan.joins.push({
          left_scope: relationScope(c.left),
          left_column: c.left.column,
          right_scope: relationScope(c.right),
          right_column: c.right.column,
        });
      return;
    case "Project": {
      collectRelationFacade(expr.input, plan);
      let outputScope: string | undefined;
      validateRelationProjectionAliases(expr.columns);
      for (const c of expr.columns) {
        if (c.expr.kind === "RowId") {
          if (c.expr.row === "Current") continue;
          throw unificationError("outer/frontier row-id relation projections are not unified yet");
        }
        const scope = relationScope(c.expr.column);
        if (outputScope !== undefined && outputScope !== scope)
          throw unificationError("relation query project must select one output scope");
        outputScope ??= scope;
      }
      plan.outputScope = outputScope;
      plan.outputProjection = structuredClone(expr.columns);
      return;
    }
    case "OrderBy": {
      collectRelationFacade(expr.input, plan);
      const outputScope = plan.outputScope;
      if (outputScope === undefined)
        throw unificationError("relation order_by requires projected output scope");
      for (const t of expr.terms) {
        if (relationScope(t.column) !== outputScope)
          throw unificationError("relation order_by on non-output scope is not unified yet");
        plan.orderBy.push([t.column.column, t.direction]);
      }
      return;
    }
    case "Offset":
    case "Limit":
      collectRelationFacade(expr.input, plan);
      [plan.offset, plan.limit] = foldOutputStep([plan.offset, plan.limit], {
        kind: expr.kind,
        value: expr.kind === "Offset" ? expr.offset : expr.limit,
      });
      return;
    default:
      throw unificationError("union/gather/distinct relation query lowering is not unified yet");
  }
}

export function relationScope(column: RelationColumnRef): string {
  if (column.scope === undefined)
    throw unificationError("relation column refs must be scoped for unified lowering");
  return column.scope;
}

const CMP_TO_PREDICATE = { Eq: "Eq", Ne: "Ne", Lt: "Lt", Le: "Lte", Gt: "Gt", Ge: "Gte" } as const;

/** A comparison/null/membership/containment leaf, or undefined for any other predicate. */
function relationLeafPredicate(
  p: RelationPredicate,
  column: (c: RelationColumnRef) => Operand,
): Predicate | undefined {
  switch (p.kind) {
    case "Cmp": {
      const left = column(p.left);
      return { kind: CMP_TO_PREDICATE[p.op], left, right: relationValueToOperand(p.right) };
    }
    case "IsNull":
      return { kind: "IsNull", operand: column(p.column) };
    case "IsNotNull":
      return { kind: "Not", predicate: { kind: "IsNull", operand: column(p.column) } };
    case "In": {
      const left = column(p.left);
      return { kind: "In", left, values: p.values.map(relationValueToOperand) };
    }
    case "Contains": {
      const left = column(p.left);
      return { kind: "Contains", left, right: relationValueToOperand(p.right) };
    }
    default:
      return undefined;
  }
}

function relationPredicateToQueryPredicate(p: RelationPredicate): [string, Predicate] | undefined {
  let scope = "";
  const leaf = relationLeafPredicate(p, (c) => {
    scope = relationScope(c);
    return { kind: "Column", name: c.column };
  });
  if (leaf) return [scope, leaf];
  switch (p.kind) {
    case "EnumMatch":
      return [
        relationScope(p.column),
        {
          kind: "EnumMatch",
          column: p.column.column,
          case: p.case,
          payload: relationPayloadPredicateToPredicate(p.payload),
        },
      ];
    case "And":
    case "Or":
      return relationPredicateList(p.predicates, p.kind === "And");
    case "Not": {
      const inner = relationPredicateToQueryPredicate(p.predicate);
      if (!inner) return ["", FALSE()];
      if (isFalse(inner[1])) return undefined;
      return [inner[0], { kind: "Not", predicate: inner[1] }];
    }
    case "True":
      return undefined;
    case "False":
      return ["", FALSE()];
    default:
      throw new Error("leaf predicates are converted above");
  }
}

function relationPayloadPredicateToPredicate(p: RelationPredicate): Predicate {
  const leaf = relationLeafPredicate(p, (c) => {
    if (c.scope !== undefined)
      throw unificationError("payload enum predicate fields must be unscoped");
    return { kind: "Column", name: c.column };
  });
  if (leaf) return leaf;
  switch (p.kind) {
    case "And":
      return { kind: "All", predicates: p.predicates.map(relationPayloadPredicateToPredicate) };
    case "Or":
      return { kind: "Any", predicates: p.predicates.map(relationPayloadPredicateToPredicate) };
    case "Not":
      return { kind: "Not", predicate: relationPayloadPredicateToPredicate(p.predicate) };
    case "True":
      return { kind: "All", predicates: [] };
    case "False":
      return FALSE();
    case "EnumMatch":
      throw unificationError("nested payload enum matches are not supported");
    default:
      throw new Error("leaf predicates are converted above");
  }
}

function relationPredicateList(
  predicates: RelationPredicate[],
  isAnd: boolean,
): [string, Predicate] | undefined {
  let scope: string | undefined;
  const items: Predicate[] = [];
  for (const child of predicates) {
    const converted = relationPredicateToQueryPredicate(child);
    if (!converted) {
      if (isAnd) continue;
      return undefined;
    }
    const [childScope, p] = converted;
    if (isFalse(p)) {
      if (isAnd) return ["", FALSE()];
      continue;
    }
    if (childScope) {
      if (scope !== undefined && scope !== childScope)
        throw unificationError("relation predicate composition across scopes is not unified yet");
      scope ??= childScope;
    }
    items.push(p);
  }
  if (scope === undefined) return isAnd ? undefined : ["", FALSE()];
  return [scope, { kind: isAnd ? "All" : "Any", predicates: items }];
}

function relationValueToOperand(v: RelationValueRef): Operand {
  switch (v.kind) {
    case "Literal":
      return { kind: "Literal", value: jsonValueToRecordValue(v.value) };
    case "Param":
      return { kind: "Param", name: v.name };
    case "SessionRef": {
      const author = authorClaimPathKey(v.path);
      if (author !== undefined) return { kind: "Claim", name: author };
      if (v.path.length === 1) return { kind: "Claim", name: v.path[0] };
      if (v.path.length === 2 && v.path[0] === "claims")
        return { kind: "Claim", name: providerClaimOperandKey(v.path[1]) };
      throw unificationError('session refs support session.user and session.claims["name"]');
    }
    default:
      throw unificationError(
        "outer/frontier/row-id relation predicate operands are not unified yet",
      );
  }
}

function jsonValueToRecordValue(v: JsonValue): Value {
  if (v === null) return { kind: "Nullable", value: undefined };
  if (typeof v === "boolean") return { kind: "Bool", value: v };
  if (typeof v === "number") {
    if (!Number.isFinite(v)) throw unificationError("relation literal is not finite");
    if (Number.isInteger(v)) return v >= 0 ? { kind: "U64", value: v } : { kind: "I64", value: v };
    return { kind: "F64", value: v };
  }
  if (typeof v === "string") return { kind: "String", value: v };
  if (Array.isArray(v)) return { kind: "Array", value: v.map(jsonValueToRecordValue) };
  return runtimeValueObjectToRecordValue(v);
}

function runtimeValueObjectToRecordValue(map: { [key: string]: JsonValue }): Value {
  const type = map.type;
  if (typeof type !== "string")
    throw unificationError("object relation literals must use the runtime value envelope");
  const value = map.value;
  const str = (): string => {
    if (typeof value !== "string")
      throw unificationError("string relation literal requires a string value");
    return value;
  };
  switch (type) {
    case "Null":
      return { kind: "Nullable", value: undefined };
    case "Boolean":
      if (typeof value !== "boolean")
        throw unificationError("Boolean relation literal requires a boolean value");
      return { kind: "Bool", value };
    case "Text":
      return { kind: "String", value: str() };
    case "Uuid": {
      const text = str();
      if (
        !/^[0-9a-fA-F]{8}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{12}$/.test(
          text,
        )
      )
        throw unificationError("invalid Uuid relation literal");
      return { kind: "Uuid", value: uuidFromString(text) };
    }
    case "Bytea": {
      if (!Array.isArray(value))
        throw unificationError("Bytea relation literal requires an array value");
      return {
        kind: "Bytes",
        value: Uint8Array.from(value, (b) => {
          if (typeof b !== "number" || !Number.isInteger(b) || b < 0 || b > 255)
            throw unificationError("Bytea relation literal values must be bytes");
          return b;
        }),
      };
    }
    case "Integer":
      if (typeof value !== "number")
        throw unificationError("Integer relation literal requires an integer value");
      if (!Number.isInteger(value) || value < -(2 ** 31) || value >= 2 ** 31)
        throw unificationError("Integer relation literal requires a signed 32-bit integer");
      return { kind: "I32", value };
    case "Timestamp":
      if (typeof value !== "number")
        throw unificationError("integer relation literal requires a numeric value");
      if (!Number.isInteger(value) || value < 0)
        throw unificationError("integer relation literal must be non-negative");
      return { kind: "U64", value };
    case "BigInt": {
      const n =
        typeof value === "number" && Number.isInteger(value)
          ? BigInt(value)
          : typeof value === "string" && /^[+-]?\d+$/.test(value)
            ? BigInt(value)
            : undefined;
      if (typeof value !== "number" && typeof value !== "string")
        throw unificationError("BigInt relation literal requires an integer value");
      if (n === undefined || n < -(2n ** 63n) || n >= 2n ** 63n)
        throw unificationError("BigInt relation literal requires a signed 64-bit integer");
      return { kind: "I64", value: n };
    }
    case "Double":
      if (typeof value !== "number")
        throw unificationError("double relation literal requires a numeric value");
      if (!Number.isFinite(value)) throw unificationError("double relation literal must be finite");
      return { kind: "F64", value };
    case "Array":
      if (!Array.isArray(value))
        throw unificationError("Array relation literal requires an array value");
      return { kind: "Array", value: value.map(jsonValueToRecordValue) };
    default:
      throw unificationError(`unsupported runtime relation literal type ${type}`);
  }
}
