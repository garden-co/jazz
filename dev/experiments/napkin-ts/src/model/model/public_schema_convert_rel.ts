// ExistsRel (relation-IR) policy lowering for public schema conversion.
import {
  qJoinVia,
  type Query,
  type Predicate,
  type Operand,
  type JoinVia,
  type JoinCorrelation,
} from "../../_extern/model-core.js";
import type { Schema, TableName } from "./public_api/types/schema.js";
import type {
  ColumnRef,
  PredicateExpr as RelPredicateExpr,
  RelExpr,
  ValueRef as RelValueRef,
} from "./public_api/relation_ir.js";
import type { PolicyExpr } from "./public_api/policy.js";
import { err, policyPath } from "./public_schema_convert.js";
import {
  convertPolicyLiteral,
  convertSessionPathOperand,
} from "./public_schema_convert_predicate.js";

type LoweredRelValue =
  | { kind: "Operand"; operand: Operand }
  | { kind: "OuterRow"; column: string }
  | { kind: "FrontierRow" };

type LoweredRelPredicate = { predicate: Predicate; column?: string; value?: LoweredRelValue };
const bare = (predicate: Predicate): LoweredRelPredicate => ({ predicate });
const on = (column: string, predicate: Predicate): LoweredRelPredicate => ({ predicate, column });

type PendingReachable = {
  from: Operand;
  seed?: any;
  edge_table: string;
  edge_member_column: string;
  edge_parent_column: string;
  edge_filters: Predicate[];
  max_depth: number;
};

type LoweredRel = {
  // Compiler-only paths into `joins`; null marks an ambiguous repeated scope.
  scope_paths: Map<string, number[] | null>;
  table?: string;
  filters: LoweredRelPredicate[];
  joins: JoinVia[];
  reachable: any[];
  pending_reachable?: PendingReachable;
};

const ALL: Predicate = { kind: "All", predicates: [] };
const isOuter = (f: LoweredRelPredicate) => f.value?.kind === "OuterRow";
const targetFor = (column: string) => (column === "id" ? "RowId" : "Column");

function validateExistsRelJoinConditions(table: TableName, path: string, rel: RelExpr): void {
  switch (rel.kind) {
    case "TableScan":
      return;
    case "Filter":
    case "Project":
      return validateExistsRelJoinConditions(table, path, rel.input);
    case "Union": {
      const labels = new Set<string>();
      for (const arm of rel.inputs) {
        const len = new TextEncoder().encode(arm.label).length;
        if (!arm.label || len > 4096 || arm.label.includes("\0") || labels.has(arm.label))
          throw err(
            policyPath(table, path),
            "core schema ExistsRel union labels must be unique, NUL-free, and 1..=4096 bytes",
          );
        labels.add(arm.label);
        validateExistsRelJoinConditions(table, path, arm.input);
      }
      return;
    }
    case "Join":
      if (rel.on.length > 1)
        throw err(
          policyPath(table, path),
          "core schema ExistsRel joins support exactly one column equality",
        );
      validateExistsRelJoinConditions(table, path, rel.left);
      return validateExistsRelJoinConditions(table, path, rel.right);
    case "Gather":
      validateExistsRelJoinConditions(table, path, rel.seed);
      return validateExistsRelJoinConditions(table, path, rel.step);
  }
}

export function validateExistsRelPolicyJoinConditions(
  table: TableName,
  path: string,
  expr: PolicyExpr,
): void {
  switch (expr.type) {
    case "ExistsRel":
      return validateExistsRelJoinConditions(table, path, expr.rel);
    case "And":
    case "Or":
      expr.exprs.forEach((e, i) =>
        validateExistsRelPolicyJoinConditions(table, `${path}.${expr.type}[${i}]`, e),
      );
      return;
    case "Not":
      return validateExistsRelPolicyJoinConditions(table, `${path}.Not`, expr.expr);
  }
}

function relHasOuterReferences(rel: RelExpr): boolean {
  const valueIsOuter = (v: RelValueRef) =>
    v.kind === "OuterColumn" || (v.kind === "RowId" && v.ref === "Outer");
  const predHasOuter = (p: RelPredicateExpr): boolean => {
    switch (p.kind) {
      case "Cmp":
      case "Contains":
        return valueIsOuter(p.right);
      case "In":
        return p.values.some(valueIsOuter);
      case "And":
      case "Or":
        return p.exprs.some(predHasOuter);
      case "Not":
        return predHasOuter(p.expr);
      case "EnumMatch":
        return predHasOuter(p.payload);
      default:
        return false;
    }
  };
  switch (rel.kind) {
    case "TableScan":
      return false;
    case "Filter":
      return relHasOuterReferences(rel.input) || predHasOuter(rel.predicate);
    case "Project":
      return relHasOuterReferences(rel.input);
    case "Join":
      return relHasOuterReferences(rel.left) || relHasOuterReferences(rel.right);
    case "Union":
      return rel.inputs.some((a) => relHasOuterReferences(a.input));
    case "Gather":
      return true;
  }
}

const joinHasOuterCorrelations = (j: JoinVia): boolean =>
  j.correlated_filters.length > 0 || j.nested_joins.some(joinHasOuterCorrelations);

export function uncorrelatedExistsJoin(
  table: string,
  filters: Predicate[],
  nested_joins: JoinVia[],
): JoinVia {
  return {
    table,
    on_column: "",
    target: "Uncorrelated",
    correlated_filters: [],
    filters,
    nested_joins,
  };
}

/** Orient an inner-join tree from the source correlated to the protected row. */
function rootExistsRelAtCorrelation(table: TableName, path: string, lowered: LoweredRel): void {
  if (lowered.reachable.length || lowered.pending_reachable) return;
  while (!lowered.filters.some(isOuter)) {
    const index = lowered.joins.findIndex(joinHasOuterCorrelations);
    if (index < 0) break;
    const [next] = lowered.joins.splice(index, 1);
    const sourceColumn = next.source_column;
    next.source_column = undefined;
    if (sourceColumn === undefined)
      throw err(
        policyPath(table, path),
        "core schema ExistsRel nested join is missing its source column",
      );
    const previous = lowered.table;
    lowered.table = next.table;
    if (previous === undefined)
      throw err(
        policyPath(table, path),
        "core schema ExistsRel correlation requires a row-producing relation",
      );
    const reverse: JoinVia = {
      table: previous,
      target: targetFor(sourceColumn),
      on_column: sourceColumn,
      source_column: next.on_column,
      correlated_filters: [],
      filters: lowered.filters.map((f) => f.predicate),
      nested_joins: lowered.joins,
    };
    lowered.filters = [
      ...next.filters.map(bare),
      ...next.correlated_filters.map(
        (c: JoinCorrelation): LoweredRelPredicate => ({
          predicate: ALL,
          column: c.join_column,
          value: { kind: "OuterRow", column: c.source_column },
        }),
      ),
    ];
    next.nested_joins.push(reverse);
    lowered.joins = next.nested_joins;
    lowered.scope_paths.clear();
  }
}

export function appendExistsRelPolicyClause(
  schema: Schema,
  table: TableName,
  path: string,
  query: Query,
  rel: RelExpr,
): Query {
  const p = policyPath(table, path);
  const lowered = lowerExistsRel(schema, table, path, rel);
  rootExistsRelAtCorrelation(table, path, lowered);
  let ci = lowered.filters.findIndex((f) => isOuter(f) && f.column === "id");
  if (ci < 0) ci = lowered.filters.findIndex(isOuter);
  if (ci < 0) {
    if (
      relHasOuterReferences(rel) ||
      lowered.reachable.length ||
      lowered.pending_reachable ||
      lowered.joins.some(joinHasOuterCorrelations)
    )
      throw err(
        p,
        "uncorrelated ExistsRel cannot contain unresolved outer references or reachability",
      );
    if (lowered.table === undefined)
      throw err(p, "uncorrelated ExistsRel requires a table relation");
    query.joins.push(
      uncorrelatedExistsJoin(
        lowered.table,
        lowered.filters.map((f) => f.predicate),
        lowered.joins,
      ),
    );
    return query;
  }
  const [correlation] = lowered.filters.splice(ci, 1);
  const correlationColumn = correlation.column;
  if (correlationColumn === undefined)
    throw err(p, "core schema ExistsRel policies must correlate a concrete column");
  if (correlation.value?.kind !== "OuterRow")
    throw err(p, "core schema ExistsRel policies must correlate to an outer row reference");
  const sourceColumn = correlation.value.column;

  const correlated: JoinCorrelation[] = [];
  const remaining: Predicate[] = [];
  for (const f of lowered.filters) {
    if (f.column !== undefined && f.value?.kind === "OuterRow")
      correlated.push({ join_column: f.column, source_column: f.value.column });
    else remaining.push(f.predicate);
  }
  for (const r of lowered.reachable)
    if (r.access_row_column === "__pending_outer_row") {
      r.access_row_column = correlationColumn;
      r.access_filters.push(...remaining);
    }
  const pending = lowered.pending_reachable;
  lowered.pending_reachable = undefined;
  if (pending)
    lowered.reachable.push({
      access_table: table,
      access_row_column: "id",
      access_team_column: sourceColumn,
      access_team_target: targetFor(sourceColumn),
      from: pending.from,
      access_filters: remaining.slice(),
      edge_table: pending.edge_table,
      edge_member_column: pending.edge_member_column,
      edge_parent_column: pending.edge_parent_column,
      edge_filters: pending.edge_filters,
      bound: { kind: "MaxDepth", depth: pending.max_depth },
      seed: pending.seed,
    });
  if (lowered.reachable.length) {
    query.reachable.push(...lowered.reachable);
    return query;
  }
  const loweredTable = lowered.table;
  if (loweredTable === undefined)
    throw err(p, "core schema scalar Gather output must be consumed by an access join");

  if (lowered.joins.length) {
    for (const nested of lowered.joins) {
      if (nested.nested_joins.some((c) => c.correlated_filters.length))
        throw err(
          p,
          "core schema ExistsRel does not yet support outer correlations beyond one nested join",
        );
      const nestedSource = nested.source_column;
      if (nestedSource === undefined)
        throw err(p, "core schema ExistsRel nested join is missing its source column");
      for (const c of nested.correlated_filters.splice(0)) {
        if (c.join_column !== nested.on_column)
          throw err(p, "core schema ExistsRel nested outer correlation must use its join key");
        correlated.push({ join_column: nestedSource, source_column: c.source_column });
      }
    }
    query.joins.push({
      table: loweredTable,
      on_column: correlationColumn,
      target: targetFor(correlationColumn),
      source_column: sourceColumn,
      correlated_filters: correlated,
      filters: remaining,
      nested_joins: lowered.joins,
    });
    return query;
  }
  const filters = lowered.filters.map((f) => f.predicate);
  if (correlationColumn === "id")
    return qJoinVia(query, loweredTable, "id", filters, {
      target: "RowId",
      sourceColumn,
      correlatedFilters: correlated,
    });
  return qJoinVia(query, loweredTable, correlationColumn, filters, {
    sourceColumn,
    correlatedFilters: correlated,
  });
}

function resolveRelColumnRoute(
  table: TableName,
  path: string,
  lowered: LoweredRel,
  column: ColumnRef,
): number[] {
  if (column.scope === undefined) return [];
  const route = lowered.scope_paths.get(column.scope);
  if (route === null)
    throw err(
      policyPath(table, path),
      `ExistsRel column scope '${column.scope}' is ambiguous; use distinct aliases`,
    );
  if (route === undefined)
    throw err(
      policyPath(table, path),
      `ExistsRel column references unknown scope '${column.scope}'`,
    );
  return route.slice();
}

/** Assign conjuncts to their source before converting scoped columns to local names. */
function appendScopedRelPredicate(
  table: TableName,
  path: string,
  lowered: LoweredRel,
  predicate: RelPredicateExpr,
): void {
  if (predicate.kind === "And") {
    for (const c of predicate.exprs) appendScopedRelPredicate(table, path, lowered, c);
    return;
  }
  const routes = new Map<string, number[]>();
  for (const c of collectRelPredicateColumns(predicate)) {
    const r = resolveRelColumnRoute(table, path, lowered, c);
    routes.set(r.join(","), r);
  }
  if (routes.size > 1)
    throw err(
      policyPath(table, path),
      "ExistsRel Boolean filters spanning multiple sources are not supported",
    );
  const route = [...routes.values()][0] ?? [];
  const filters = relPredicateToPolicy(table, path, predicate);
  if (!route.length) {
    lowered.filters.push(...filters);
    return;
  }
  let joins = lowered.joins;
  route.forEach((index, position) => {
    const join = joins[index];
    if (position + 1 === route.length) {
      for (const f of filters) {
        if (f.column !== undefined && f.value?.kind === "OuterRow")
          join.correlated_filters.push({ join_column: f.column, source_column: f.value.column });
        else join.filters.push(f.predicate);
      }
    }
    joins = join.nested_joins;
  });
}

function collectRelPredicateColumns(p: RelPredicateExpr, out: ColumnRef[] = []): ColumnRef[] {
  switch (p.kind) {
    case "And":
    case "Or":
      for (const c of p.exprs) collectRelPredicateColumns(c, out);
      break;
    case "Not":
      collectRelPredicateColumns(p.expr, out);
      break;
    case "Cmp":
    case "Contains":
    case "In":
      out.push(p.left);
      break;
    case "IsNull":
    case "IsNotNull":
    case "EnumMatch":
      out.push(p.column);
  }
  return out;
}

function lowerExistsRel(schema: Schema, table: TableName, path: string, rel: RelExpr): LoweredRel {
  const p = policyPath(table, path);
  switch (rel.kind) {
    case "TableScan":
      return {
        scope_paths: new Map([[rel.alias ?? rel.table, []]]),
        table: rel.table,
        filters: [],
        joins: [],
        reachable: [],
      };
    case "Filter": {
      const lowered = lowerExistsRel(schema, table, path, rel.input);
      appendScopedRelPredicate(table, path, lowered, rel.predicate);
      return lowered;
    }
    case "Project":
      return lowerExistsRel(schema, table, path, rel.input);
    case "Gather": {
      const current = (k: any) => k.kind === "RowId" && k.ref === "Current";
      if (!current(rel.frontier_key) || rel.dedupe_key.length !== 1 || !current(rel.dedupe_key[0]))
        throw err(
          p,
          "Gather keyed on anything but the current row id is not supported in policies yet",
        );
      return lowerGatherRel(schema, table, path, rel.seed, rel.step, rel.bound);
    }
    case "Join": {
      if (rel.join_kind !== "Inner")
        throw err(p, "core schema ExistsRel policies only support inner joins");
      const left = lowerExistsRel(schema, table, path, rel.left);
      const right = lowerExistsRel(schema, table, path, rel.right);
      if (right.pending_reachable)
        throw err(p, "Gather on the right side of a join is not supported in policies yet");
      const cond = rel.on[0];
      if (!cond) throw err(p, "core schema ExistsRel joins require a column equality");
      const sourceRoute = resolveRelColumnRoute(table, path, left, cond.left);
      const targetRoute = resolveRelColumnRoute(table, path, right, cond.right);
      if (targetRoute.length)
        throw err(p, "core schema ExistsRel join target must be the right relation's root source");
      const pending = left.pending_reachable;
      if (pending) {
        left.pending_reachable = undefined;
        if (cond.left.column !== "id")
          throw err(p, "core schema ExistsRel reachable joins must join from reachable id");
        if (right.table === undefined)
          throw err(p, "core schema ExistsRel reachable access must be a row-producing relation");
        const reachable = {
          access_table: right.table,
          access_row_column: "__pending_outer_row",
          access_team_column: cond.right.column,
          access_team_target: targetFor(cond.right.column),
          from: pending.from,
          access_filters: [
            ...right.filters.map((f) => f.predicate),
            ...left.filters.map((f) => f.predicate),
          ],
          edge_table: pending.edge_table,
          edge_member_column: pending.edge_member_column,
          edge_parent_column: pending.edge_parent_column,
          edge_filters: pending.edge_filters,
          bound: { kind: "MaxDepth", depth: pending.max_depth },
          seed: pending.seed,
        };
        return {
          scope_paths: right.scope_paths,
          table: left.table,
          filters: [],
          joins: left.joins,
          reachable: [...left.reachable, ...right.reachable, reachable],
        };
      }
      const correlated: JoinCorrelation[] = [];
      const filters: Predicate[] = [];
      for (const f of right.filters) {
        if (f.column !== undefined && f.value?.kind === "OuterRow")
          correlated.push({ join_column: f.column, source_column: f.value.column });
        else filters.push(f.predicate);
      }
      if (right.table === undefined)
        throw err(p, "core schema ExistsRel joins require a row-producing right relation");
      const join: JoinVia = {
        table: right.table,
        on_column: cond.right.column,
        target: targetFor(cond.right.column),
        source_column: cond.left.column,
        correlated_filters: correlated,
        filters,
        nested_joins: right.joins,
      };
      let joins = left.joins;
      for (const i of sourceRoute) joins = joins[i].nested_joins;
      const joinRoute = [...sourceRoute, joins.length];
      joins.push(join);
      for (const [scope, route] of right.scope_paths) {
        const full = route === null ? null : [...joinRoute, ...route];
        left.scope_paths.set(scope, left.scope_paths.has(scope) ? null : full);
      }
      left.reachable.push(...right.reachable);
      return left;
    }
    case "Union":
      throw err(p, "core schema ExistsRel policies do not support Union yet");
  }
}

function lowerGatherRel(
  schema: Schema,
  table: TableName,
  path: string,
  seedRel: RelExpr,
  step: RelExpr,
  bound: any,
): LoweredRel {
  const [from, seed0] = lowerGatherSeed(table, path, seedRel);
  const [edgeTable, outputTable, memberColumn, parentColumn, edgeFilters] = lowerGatherStep(
    table,
    path,
    step,
  );
  const outputScope =
    outputTable ??
    schema.get(edgeTable)?.columns.columns.find((c) => c.name === parentColumn)?.references;
  const seed =
    outputTable === seed0.table &&
    seed0.user_column === "id" &&
    seed0.team_column === "id" &&
    !seed0.filters.length
      ? undefined
      : seed0;
  if (bound.kind === "Fixpoint")
    throw err(
      policyPath(table, path),
      "server shell public schema conversion does not support Gather Fixpoint yet",
    );
  return {
    scope_paths: new Map(outputScope !== undefined ? [[outputScope, []]] : []),
    table: outputTable,
    filters: [],
    joins: [],
    reachable: [],
    pending_reachable: {
      from,
      seed,
      edge_table: edgeTable,
      edge_member_column: memberColumn,
      edge_parent_column: parentColumn,
      edge_filters: edgeFilters,
      max_depth: bound.depth,
    },
  };
}

function lowerGatherSeed(table: TableName, path: string, seed: RelExpr): [Operand, any] {
  const p = policyPath(table, path);
  const [input0, teamColumn, projectedFilters] = unwrapSeedProjection(table, path, seed);
  const [input, filters0] = unwrapRelFilter(input0);
  if (input.kind !== "TableScan")
    throw err(p, "Gather seeded reachability requires a filtered seed table scan");
  const lowered = relPredicatesToPolicy(table, path, [...filters0, ...projectedFilters]);
  const si = lowered.findIndex(
    (f) =>
      f.predicate.kind === "Eq" && f.value?.kind === "Operand" && f.value.operand.kind === "Claim",
  );
  if (si < 0) throw err(p, "Gather same-table seeds require one claim-keyed equality filter");
  const sf = lowered[si];
  if (sf.column === undefined)
    throw err(p, "Gather seed claim filter must name a seed-table column");
  if (sf.value?.kind !== "Operand" || sf.value.operand.kind !== "Claim")
    throw err(p, "Gather seed claim filter must compare against a claim");
  const userClaim: string = sf.value.operand.name;
  return [
    { kind: "Claim", name: userClaim },
    {
      table: input.table,
      user_column: sf.column,
      user_claim: userClaim,
      team_column: teamColumn ?? "id",
      filters: lowered.filter((_, i) => i !== si).map((f) => f.predicate),
    },
  ];
}

function unwrapSeedProjection(
  table: TableName,
  path: string,
  seed: RelExpr,
): [RelExpr, string | undefined, RelPredicateExpr[]] {
  const p = policyPath(table, path);
  if (seed.kind !== "Project") return [seed, undefined, []];
  const column = seed.columns.find((c) => c.alias === "id");
  if (!column) throw err(p, "Gather projected seed must expose an id column");
  if (column.expr.kind !== "Column")
    throw err(p, "Gather projected seed id must be a column projection");
  const projected = column.expr.column;
  const [projectInput, filters] = unwrapRelFilter(seed.input);
  if (projectInput.kind === "Join") {
    const [left, team, seedScope] = unwrapJoinedSeedProjection(
      table,
      path,
      projectInput,
      projected,
    );
    const cols = filters.flatMap((f) => collectRelPredicateColumns(f));
    if (cols.some((c) => c.scope !== undefined && c.scope !== seedScope))
      throw err(p, "Gather seed filters on the hop target are not supported in policies yet");
    return [left, team, filters];
  }
  return [seed.input, projected.column, []];
}

function unwrapJoinedSeedProjection(
  table: TableName,
  path: string,
  join: Extract<RelExpr, { kind: "Join" }>,
  projected: ColumnRef,
): [RelExpr, string, string] {
  const p = policyPath(table, path);
  const right = join.right;
  if (right.kind !== "TableScan")
    throw err(p, "Gather projected seed hop must join to a table scan");
  const seedScan = unwrapRelFilter(join.left)[0];
  if (seedScan.kind !== "TableScan")
    throw err(p, "Gather projected seed hop must start from a filtered table scan");
  const seedScope = seedScan.alias ?? seedScan.table;
  const targetScope = right.alias ?? right.table;
  if (join.on.length !== 1)
    throw err(
      p,
      "Gather seed hops without exactly one join equality are not supported in policies yet",
    );
  const [cond] = join.on;
  if (
    join.join_kind !== "Inner" ||
    cond.right.column !== "id" ||
    (cond.right.scope !== undefined && cond.right.scope !== targetScope) ||
    (cond.left.scope !== undefined && cond.left.scope !== seedScope)
  )
    throw err(
      p,
      "Gather seed hops other than an inner join from a seed FK to the target id are not supported in policies yet",
    );
  if (projected.scope !== right.alias || projected.column !== "id")
    throw err(p, "Gather projected seed id must come from the hop target row id");
  return [join.left, cond.left.column, seedScope];
}

function lowerGatherStep(
  table: TableName,
  path: string,
  step: RelExpr,
): [string, string | undefined, string, string, Predicate[]] {
  const p = policyPath(table, path);
  if (step.kind !== "Project") throw err(p, "Gather policies require projected recursive hops");
  const input = step.input;
  let edgeRel: RelExpr;
  let outputTable: string | undefined;
  let parentColumn: string;
  let projectedScope: string | undefined;
  if (input.kind === "Join") {
    const right = input.right;
    if (right.kind !== "TableScan")
      throw err(p, "Gather recursive hop must join to the output table");
    if (input.on.length !== 1)
      throw err(
        p,
        "Gather recursive hops without exactly one join equality are not supported in policies yet",
      );
    const [cond] = input.on;
    const outputScope = right.alias ?? right.table;
    if (
      input.join_kind !== "Inner" ||
      cond.right.column !== "id" ||
      (cond.right.scope !== undefined && cond.right.scope !== outputScope)
    )
      throw err(
        p,
        "Gather recursive hops other than an inner join from an edge FK to the output id are not supported in policies yet",
      );
    const relabelsId = step.columns.some(
      (c) =>
        c.alias === "id" &&
        !(
          c.expr.kind === "Column" &&
          c.expr.column.column === "id" &&
          (c.expr.column.scope === undefined || c.expr.column.scope === outputScope)
        ),
    );
    if (relabelsId)
      throw err(
        p,
        "Gather recursive hops projecting anything but the output row id as id are not supported in policies yet",
      );
    edgeRel = input.left;
    outputTable = right.table;
    parentColumn = cond.left.column;
    projectedScope = cond.left.scope;
  } else {
    const projected = step.columns.find((c) => c.alias === "id");
    if (!projected) throw err(p, "Gather scalar recursive hop must project its parent key as id");
    if (projected.expr.kind !== "Column")
      throw err(p, "Gather scalar recursive hop id must be a column projection");
    edgeRel = input;
    parentColumn = projected.expr.column.column;
    projectedScope = projected.expr.column.scope;
  }
  const [edgeInput, filters] = unwrapRelFilter(edgeRel);
  if (edgeInput.kind !== "TableScan")
    throw err(p, "Gather recursive hop must start from an edge table scan");
  const edgeScope = edgeInput.alias ?? edgeInput.table;
  if (projectedScope !== undefined && projectedScope !== edgeScope)
    throw err(
      p,
      "Gather recursive hop keys from outside the edge table are not supported in policies yet",
    );
  const lowered = relPredicatesToPolicy(table, path, filters);
  const fi = lowered.findIndex((f) => f.value?.kind === "FrontierRow");
  if (fi < 0) throw err(p, "Gather recursive hop requires a frontier equality");
  const member = lowered[fi].column;
  if (member === undefined) throw err(p, "Gather frontier equality must name an edge column");
  return [
    edgeInput.table,
    outputTable,
    member,
    parentColumn,
    lowered.filter((_, i) => i !== fi).map((f) => f.predicate),
  ];
}

const unwrapRelFilter = (rel: RelExpr): [RelExpr, RelPredicateExpr[]] =>
  rel.kind === "Filter" ? [rel.input, [rel.predicate]] : [rel, []];

const CMP_KINDS = { Eq: "Eq", Ne: "Ne", Lt: "Lt", Le: "Lte", Gt: "Gt", Ge: "Gte" } as const;

function relPredicateToPolicy(
  table: TableName,
  path: string,
  p: RelPredicateExpr,
): LoweredRelPredicate[] {
  const ppath = policyPath(table, path);
  const parts = (x: RelPredicateExpr) =>
    relPredicateToPolicy(table, path, x).map((l) => l.predicate);
  const col = (c: ColumnRef): Operand => ({ kind: "Column", name: c.column });
  switch (p.kind) {
    case "True":
      return [];
    case "False":
      return [bare({ kind: "Any", predicates: [] })];
    case "And":
      return p.exprs.flatMap((c) => relPredicateToPolicy(table, path, c));
    case "Or":
      return [
        bare({
          kind: "Any",
          predicates: p.exprs.map((c) => ({ kind: "All", predicates: parts(c) })),
        }),
      ];
    case "Not":
      return [bare({ kind: "Not", predicate: { kind: "All", predicates: parts(p.expr) } })];
    case "Cmp": {
      if (p.right.kind === "Literal" && p.right.value.type === "Null") {
        const isNull: Predicate = { kind: "IsNull", operand: col(p.left) };
        if (p.op === "Eq") return [on(p.left.column, isNull)];
        if (p.op === "Ne") return [on(p.left.column, { kind: "Not", predicate: isNull })];
        throw err(
          ppath,
          "core schema ExistsRel NULL comparisons only support equality and inequality",
        );
      }
      const value = relValueToPolicyOperand(table, path, p.right);
      let predicate: Predicate;
      if (value.kind === "Operand")
        predicate = { kind: CMP_KINDS[p.op], left: col(p.left), right: value.operand };
      else if (p.op === "Eq") predicate = ALL;
      else
        throw err(ppath, "core schema ExistsRel special row-id comparisons only support equality");
      return [{ predicate, column: p.left.column, value }];
    }
    case "IsNull":
      return [on(p.column.column, { kind: "IsNull", operand: col(p.column) })];
    case "IsNotNull":
      return [
        on(p.column.column, { kind: "Not", predicate: { kind: "IsNull", operand: col(p.column) } }),
      ];
    case "Contains": {
      const v = relValueToPolicyOperand(table, path, p.right);
      if (v.kind !== "Operand")
        throw err(ppath, "core schema ExistsRel Contains does not support row-id operands");
      return [on(p.left.column, { kind: "Contains", left: col(p.left), right: v.operand })];
    }
    case "EnumMatch":
      return [
        on(p.column.column, {
          kind: "EnumMatch",
          column: p.column.column,
          case: p.case,
          payload: { kind: "All", predicates: parts(p.payload) },
        }),
      ];
    case "In": {
      const values = p.values.map((v) => {
        const l = relValueToPolicyOperand(table, path, v);
        if (l.kind !== "Operand")
          throw err(ppath, "core schema ExistsRel In does not support row-id operands");
        return l.operand;
      });
      return [on(p.left.column, { kind: "In", left: col(p.left), values })];
    }
  }
}

const relPredicatesToPolicy = (table: TableName, path: string, ps: RelPredicateExpr[]) =>
  ps.flatMap((p) => relPredicateToPolicy(table, path, p));

function relValueToPolicyOperand(table: TableName, path: string, v: RelValueRef): LoweredRelValue {
  switch (v.kind) {
    case "Literal":
      return {
        kind: "Operand",
        operand: { kind: "Literal", value: convertPolicyLiteral(table, path, v.value) },
      };
    case "SessionRef":
      return { kind: "Operand", operand: convertSessionPathOperand(table, path, v.path) };
    case "OuterColumn":
      return { kind: "OuterRow", column: v.column.column };
    case "RowId":
      if (v.ref === "Outer") return { kind: "OuterRow", column: "id" };
      if (v.ref === "Frontier") return { kind: "FrontierRow" };
      throw err(
        policyPath(table, path),
        "core schema ExistsRel policies do not support Current row-id operands here",
      );
  }
}
