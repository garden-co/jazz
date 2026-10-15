import { decodeValue, encodeValue, type Value } from "../../_extern/model-query.js";
import type { Uuid } from "../../support/hash.js";
import { PostcardError, type Reader, type Writer } from "../../support/postcard.js";
import type { Query } from "./ast.js";
import { PolicyExpressionBudget, PolicyExpressionLimitError } from "./policy_limits.js";

export interface PolicyBranch {
  filters: Predicate[];
  joins: JoinVia[];
  reachable: ReachableVia[];
  inherits: InheritsVia[];
}

/**
 * All policy-only alternatives a policy query represents. `Any([])` alone is the
 * schema converter's constant-false base; empty filters are a true base.
 */
export function policyAlternativesFromQuery(query: Query): PolicyBranch[] {
  const { filters, joins, policy_branches, reachable, inherits } = query;
  const f0 = filters[0];
  const baseIsConverterFalse =
    filters.length === 1 &&
    f0.kind === "Any" &&
    f0.predicates.length === 0 &&
    joins.length === 0 &&
    reachable.length === 0 &&
    inherits.length === 0;
  const alternatives: PolicyBranch[] = [];
  if (!baseIsConverterFalse) alternatives.push({ filters, joins, reachable, inherits });
  alternatives.push(...policy_branches);
  return alternatives;
}

export function policySingleAlternativeFromQuery(query: Query): PolicyBranch {
  const alternatives = policyAlternativesFromQuery(query);
  if (alternatives.length !== 1)
    throw new Error(
      "expected exactly one policy alternative; use alternatives_from_query to preserve disjunctions",
    );
  return alternatives[0];
}

export type ShapeId = Uuid;
export type BindingId = Uuid;

export type JoinMode = "Inner" | "Holes";
const JOIN_MODES: JoinMode[] = ["Inner", "Holes"];

export interface Include {
  path: string;
  join_mode: JoinMode;
  require: boolean;
}
export const newInclude = (path: string): Include => ({ path, join_mode: "Inner", require: false });

export type ArraySubqueryRequirement = "Optional" | "AtLeastOne" | "MatchCorrelationCardinality";
const ARRAY_REQUIREMENTS: ArraySubqueryRequirement[] = [
  "Optional",
  "AtLeastOne",
  "MatchCorrelationCardinality",
];

export interface ArraySubquery {
  column_name: string;
  table: string;
  inner_column: string;
  outer_column: string;
  filters: Predicate[];
  select?: string[];
  order_by: OrderBy[];
  limit?: number;
  offset: number;
  requirement: ArraySubqueryRequirement;
  nested_arrays: ArraySubquery[];
}

export function newArraySubquery(
  column_name: string,
  table: string,
  innerColumn: string,
  outerColumn: string,
): ArraySubquery {
  const local = (column: string) => {
    const i = column.lastIndexOf(".");
    return i < 0 ? column : column.slice(i + 1);
  };
  return {
    column_name,
    table,
    inner_column: local(innerColumn),
    outer_column: local(outerColumn),
    filters: [],
    order_by: [],
    offset: 0,
    requirement: "Optional",
    nested_arrays: [],
  };
}

export type OrderDirection = "Asc" | "Desc";
const DIRECTIONS: OrderDirection[] = ["Asc", "Desc"];

export interface OrderBy {
  column: string;
  direction: OrderDirection;
}

export interface AggregateQuery {
  aggregates: Aggregate[];
  group_by?: string;
}
export const newAggregateQuery = (aggregates: Iterable<Aggregate>): AggregateQuery => ({
  aggregates: [...aggregates],
});

export type AggregateFunction = "Count" | "Sum" | "Avg" | "Min" | "Max";
const AGGREGATE_FUNCTIONS: AggregateFunction[] = ["Count", "Sum", "Avg", "Min", "Max"];

export interface Aggregate {
  function: AggregateFunction;
  column?: string;
  alias: string;
}

export const Aggregate = {
  count: (): Aggregate => ({ function: "Count", alias: "count" }),
  sum: (column: string): Aggregate => ({ function: "Sum", alias: `sum_${column}`, column }),
  avg: (column: string): Aggregate => ({ function: "Avg", alias: `avg_${column}`, column }),
  min: (column: string): Aggregate => ({ function: "Min", alias: `min_${column}`, column }),
  max: (column: string): Aggregate => ({ function: "Max", alias: `max_${column}`, column }),
};

export interface JoinVia {
  table: string;
  on_column: string;
  target: JoinTarget;
  source_column?: string;
  source_lookup?: JoinSourceLookup;
  correlated_filters: JoinCorrelation[];
  filters: Predicate[];
  nested_joins: JoinVia[];
}
export interface JoinCorrelation {
  join_column: string;
  source_column: string;
}
export interface JoinSourceLookup {
  table: string;
  row_id_source_column: string;
  value_column: string;
}
export type JoinTarget = "Column" | "RowId" | "Uncorrelated";
const JOIN_TARGETS: JoinTarget[] = ["Column", "RowId", "Uncorrelated"];

export interface ReachableVia {
  access_table: string;
  access_row_column: string;
  access_team_column: string;
  access_team_target: JoinTarget;
  from: Operand;
  access_filters: Predicate[];
  edge_table: string;
  edge_member_column: string;
  edge_parent_column: string;
  edge_filters: Predicate[];
  bound: RecursionBound;
  seed?: ReachableSeed;
}
export interface ReachableSeed {
  table: string;
  user_column?: string;
  user_claim?: string;
  team_column: string;
  filters: Predicate[];
}

export interface InheritsVia {
  parent_column: string;
  operation: InheritsOperation;
  max_depth?: number;
}
export type InheritsOperation = "Select" | "Insert" | "Update" | "Delete";
const INHERITS_OPERATIONS: InheritsOperation[] = ["Select", "Insert", "Update", "Delete"];

export type RecursionBound = { kind: "Fixpoint" } | { kind: "MaxDepth"; depth: number };
export const defaultMaxDepth = (): RecursionBound => ({ kind: "MaxDepth", depth: 8 });
/** `Fixpoint` falls back to the conservative evaluator loop cap; `MaxDepth(0)` stays zero. */
export const depthSteps = (b: RecursionBound): number => (b.kind === "Fixpoint" ? 128 : b.depth);

export type Predicate =
  | { kind: "All" | "Any"; predicates: Predicate[] }
  | { kind: "Not"; predicate: Predicate }
  | { kind: "Eq" | "Ne" | "Gt" | "Gte" | "Lt" | "Lte" | "Contains"; left: Operand; right: Operand }
  | { kind: "In"; left: Operand; values: Operand[] }
  | { kind: "EnumMatch"; column: string; case: string; payload: Predicate }
  | { kind: "IsNull"; operand: Operand };

export type Operand =
  | { kind: "Column" | "Param" | "Claim"; name: string }
  | { kind: "Literal"; value: Value };

/** Every operand of `p`, descending through All/Any/Not/EnumMatch, in a fixed order. */
export function forEachOperand(p: Predicate, f: (o: Operand) => void): void {
  anyOperand(p, (o) => {
    f(o);
    return false;
  });
}

export function anyOperand(p: Predicate, f: (o: Operand) => boolean): boolean {
  switch (p.kind) {
    case "All":
    case "Any":
      return p.predicates.some((c) => anyOperand(c, f));
    case "Not":
      return anyOperand(p.predicate, f);
    case "EnumMatch":
      return anyOperand(p.payload, f);
    case "In":
      return f(p.left) || p.values.some(f);
    case "IsNull":
      return f(p.operand);
    default:
      return f(p.left) || f(p.right);
  }
}

/** Each top-level row-condition predicate of the query and its policy branches. */
export function forEachCondition(
  query: Query,
  f: (p: Predicate, replace: (n: Predicate) => void) => void,
): void {
  const visit = (list: Predicate[]) => list.forEach((p, i) => f(p, (n) => (list[i] = n)));
  const visitJoin = (join: JoinVia): void => {
    visit(join.filters);
    join.nested_joins.forEach(visitJoin);
  };
  const visitIn = (filters: Predicate[], joins: JoinVia[], reachable: ReachableVia[]) => {
    visit(filters);
    joins.forEach(visitJoin);
    for (const r of reachable) {
      visit(r.access_filters);
      visit(r.edge_filters);
      if (r.seed) visit(r.seed.filters);
    }
  };
  visitIn(query.filters, query.joins, query.reachable);
  for (const b of query.policy_branches) visitIn(b.filters, b.joins, b.reachable);
}

/** Collision-proof internal namespace for raw identity-provider claims. */
export const PROVIDER_CLAIM_PREFIX = "\0claims:";
const PROVIDER_CLAIM_PATH_V1 = "\0claim-path-v1:";
const utf8Len = (s: string) => new TextEncoder().encode(s).length;

export const providerClaimKey = (name: string): string => providerClaimOperandKey(name);
export const providerClaimOperandKey = (name: string): string => `${PROVIDER_CLAIM_PREFIX}${name}`;

export function providerClaimPathOperandKey(segments: string[]): string {
  if (segments.length === 1) return providerClaimOperandKey(segments[0]);
  let key = PROVIDER_CLAIM_PATH_V1;
  for (const s of segments) key += `${utf8Len(s)}:${s}`;
  return key;
}

export function operandClaimPath(name: string): string[] {
  if (name.startsWith(PROVIDER_CLAIM_PATH_V1)) {
    let rest = new TextEncoder().encode(name.slice(PROVIDER_CLAIM_PATH_V1.length));
    const dec = new TextDecoder("utf-8", { fatal: true });
    const segments = ["claims"];
    while (rest.length) {
      const colon = rest.indexOf(0x3a);
      if (colon < 0) break;
      const lenText = dec.decode(rest.subarray(0, colon));
      if (!/^\+?\d+$/.test(lenText)) break;
      const len = Number(lenText);
      const tail = rest.subarray(colon + 1);
      if (len > tail.length) break;
      let segment: string;
      try {
        segment = dec.decode(tail.subarray(0, len));
      } catch {
        break;
      }
      segments.push(segment);
      rest = tail.subarray(len);
    }
    if (
      !rest.length &&
      segments.length > 2 &&
      providerClaimPathOperandKey(segments.slice(1)) === name
    )
      return segments;
  }
  if (
    ["user.account", "user.identity", "user.identity.issuer", "user.identity.subject"].includes(
      name,
    )
  )
    return name.split(".");
  return name.startsWith(PROVIDER_CLAIM_PREFIX)
    ? ["claims", name.slice(PROVIDER_CLAIM_PREFIX.length)]
    : [name];
}

/** The finite structured author vocabulary, without interpreting provider claim names as paths. */
export function authorClaimPathKey(path: string[]): string | undefined {
  const name = path.join(".");
  const valid = [
    "user",
    "user.account",
    "user.identity",
    "user.identity.issuer",
    "user.identity.subject",
  ].includes(name);
  return valid && path.every((p) => !p.includes(".")) ? name : undefined;
}

// ---- postcard codecs (serde derive equivalents) ----

const PREDICATE_KINDS = [
  "All",
  "Any",
  "Not",
  "Eq",
  "Ne",
  "In",
  "Gt",
  "Gte",
  "Lt",
  "Lte",
  "Contains",
  "EnumMatch",
  "IsNull",
] as const;
const OPERAND_KINDS = ["Column", "Param", "Claim", "Literal"] as const;

const optStr = (w: Writer, s: string | undefined) => w.option(s, (w, v) => w.str(v));
const readOptStr = (r: Reader) => r.option((r) => r.str());
const enumIndex = <T>(all: readonly T[], v: T) => all.indexOf(v);
const readEnum = <T>(r: Reader, all: readonly T[], what: string): T =>
  all[r.variant(all.length, what)];

export function encodeOperand(w: Writer, o: Operand): void {
  w.variant(OPERAND_KINDS.indexOf(o.kind));
  if (o.kind === "Literal") encodeValue(w, o.value);
  else w.str(o.name);
}

export function decodeOperand(r: Reader): Operand {
  const kind = readEnum(r, OPERAND_KINDS, "Operand");
  return kind === "Literal" ? { kind, value: decodeValue(r) } : { kind, name: r.str() };
}

export function encodePredicate(w: Writer, p: Predicate): void {
  w.variant(PREDICATE_KINDS.indexOf(p.kind));
  switch (p.kind) {
    case "All":
    case "Any":
      w.seq(p.predicates, encodePredicate);
      break;
    case "Not":
      encodePredicate(w, p.predicate);
      break;
    case "In":
      encodeOperand(w, p.left);
      w.seq(p.values, encodeOperand);
      break;
    case "EnumMatch":
      w.str(p.column).str(p.case);
      encodePredicate(w, p.payload);
      break;
    case "IsNull":
      encodeOperand(w, p.operand);
      break;
    default:
      encodeOperand(w, p.left);
      encodeOperand(w, p.right);
  }
}

// Nesting depth and node budget of the predicate tree being decoded; depth 0 = idle.
let decodeDepth = 0;
let decodeBudget = new PolicyExpressionBudget();

const budgeted = <T>(f: () => T): T => {
  try {
    return f();
  } catch (e) {
    if (e instanceof PolicyExpressionLimitError) throw new PostcardError(e.message);
    throw e;
  }
};

/** Counts every node against the outermost predicate's budget before reading its contents. */
export function decodePredicate(r: Reader): Predicate {
  decodeDepth++;
  try {
    budgeted(() => decodeBudget.enter(decodeDepth));
    const kind = readEnum(r, PREDICATE_KINDS, "Predicate");
    switch (kind) {
      case "All":
      case "Any": {
        const n = r.varint();
        budgeted(() => decodeBudget.reserve(n));
        const predicates: Predicate[] = [];
        for (let i = 0; i < n; i++) predicates.push(decodePredicate(r));
        return { kind, predicates };
      }
      case "Not":
        return { kind, predicate: decodePredicate(r) };
      case "In":
        return { kind, left: decodeOperand(r), values: r.seq(decodeOperand) };
      case "EnumMatch":
        return { kind, column: r.str(), case: r.str(), payload: decodePredicate(r) };
      case "IsNull":
        return { kind, operand: decodeOperand(r) };
      default:
        return { kind, left: decodeOperand(r), right: decodeOperand(r) };
    }
  } finally {
    if (--decodeDepth === 0) decodeBudget = new PolicyExpressionBudget();
  }
}

export function encodeInclude(w: Writer, i: Include): void {
  w.str(i.path).variant(enumIndex(JOIN_MODES, i.join_mode)).bool(i.require);
}
export function decodeInclude(r: Reader): Include {
  return { path: r.str(), join_mode: readEnum(r, JOIN_MODES, "JoinMode"), require: r.bool() };
}

export function encodeOrderBy(w: Writer, o: OrderBy): void {
  w.str(o.column).variant(enumIndex(DIRECTIONS, o.direction));
}
export function decodeOrderBy(r: Reader): OrderBy {
  return { column: r.str(), direction: readEnum(r, DIRECTIONS, "OrderDirection") };
}

export function encodeAggregateQuery(w: Writer, a: AggregateQuery): void {
  w.seq(a.aggregates, (w, g) => {
    w.variant(enumIndex(AGGREGATE_FUNCTIONS, g.function));
    optStr(w, g.column);
    w.str(g.alias);
  });
  optStr(w, a.group_by);
}
export function decodeAggregateQuery(r: Reader): AggregateQuery {
  return {
    aggregates: r.seq((r) => ({
      function: readEnum(r, AGGREGATE_FUNCTIONS, "AggregateFunction"),
      column: readOptStr(r),
      alias: r.str(),
    })),
    group_by: readOptStr(r),
  };
}

export function encodeJoinVia(w: Writer, j: JoinVia): void {
  w.str(j.table).str(j.on_column).variant(enumIndex(JOIN_TARGETS, j.target));
  optStr(w, j.source_column);
  w.option(j.source_lookup, (w, l) =>
    w.str(l.table).str(l.row_id_source_column).str(l.value_column),
  );
  w.seq(j.correlated_filters, (w, c) => w.str(c.join_column).str(c.source_column));
  w.seq(j.filters, encodePredicate);
  w.seq(j.nested_joins, encodeJoinVia);
}
export function decodeJoinVia(r: Reader): JoinVia {
  return {
    table: r.str(),
    on_column: r.str(),
    target: readEnum(r, JOIN_TARGETS, "JoinTarget"),
    source_column: readOptStr(r),
    source_lookup: r.option((r) => ({
      table: r.str(),
      row_id_source_column: r.str(),
      value_column: r.str(),
    })),
    correlated_filters: r.seq((r) => ({ join_column: r.str(), source_column: r.str() })),
    filters: r.seq(decodePredicate),
    nested_joins: r.seq(decodeJoinVia),
  };
}

export function encodeRecursionBound(w: Writer, b: RecursionBound): void {
  if (b.kind === "Fixpoint") w.variant(0);
  else w.variant(1).varint(b.depth);
}
export function decodeRecursionBound(r: Reader): RecursionBound {
  return r.variant(2, "RecursionBound") === 0
    ? { kind: "Fixpoint" }
    : { kind: "MaxDepth", depth: r.varint() };
}

export function encodeReachableVia(w: Writer, v: ReachableVia): void {
  w.str(v.access_table).str(v.access_row_column).str(v.access_team_column);
  w.variant(enumIndex(JOIN_TARGETS, v.access_team_target));
  encodeOperand(w, v.from);
  w.seq(v.access_filters, encodePredicate);
  w.str(v.edge_table).str(v.edge_member_column).str(v.edge_parent_column);
  w.seq(v.edge_filters, encodePredicate);
  encodeRecursionBound(w, v.bound);
  w.option(v.seed, (w, s) => {
    w.str(s.table);
    optStr(w, s.user_column);
    optStr(w, s.user_claim);
    w.str(s.team_column);
    w.seq(s.filters, encodePredicate);
  });
}
export function decodeReachableVia(r: Reader): ReachableVia {
  return {
    access_table: r.str(),
    access_row_column: r.str(),
    access_team_column: r.str(),
    access_team_target: readEnum(r, JOIN_TARGETS, "JoinTarget"),
    from: decodeOperand(r),
    access_filters: r.seq(decodePredicate),
    edge_table: r.str(),
    edge_member_column: r.str(),
    edge_parent_column: r.str(),
    edge_filters: r.seq(decodePredicate),
    bound: decodeRecursionBound(r),
    seed: r.option((r) => ({
      table: r.str(),
      user_column: readOptStr(r),
      user_claim: readOptStr(r),
      team_column: r.str(),
      filters: r.seq(decodePredicate),
    })),
  };
}

export function encodeInheritsVia(w: Writer, i: InheritsVia): void {
  w.str(i.parent_column).variant(enumIndex(INHERITS_OPERATIONS, i.operation));
  w.option(i.max_depth, (w, d) => w.varint(d));
}
export function decodeInheritsVia(r: Reader): InheritsVia {
  return {
    parent_column: r.str(),
    operation: readEnum(r, INHERITS_OPERATIONS, "InheritsOperation"),
    max_depth: r.option((r) => r.varint()),
  };
}

export function encodePolicyBranch(w: Writer, b: PolicyBranch): void {
  w.seq(b.filters, encodePredicate);
  w.seq(b.joins, encodeJoinVia);
  w.seq(b.reachable, encodeReachableVia);
  w.seq(b.inherits, encodeInheritsVia);
}
export function decodePolicyBranch(r: Reader): PolicyBranch {
  return {
    filters: r.seq(decodePredicate),
    joins: r.seq(decodeJoinVia),
    reachable: r.seq(decodeReachableVia),
    inherits: r.seq(decodeInheritsVia),
  };
}

export function encodeArraySubquery(w: Writer, s: ArraySubquery): void {
  w.str(s.column_name).str(s.table).str(s.inner_column).str(s.outer_column);
  w.seq(s.filters, encodePredicate);
  w.option(s.select, (w, cols) => w.seq(cols, (w, c) => w.str(c)));
  w.seq(s.order_by, encodeOrderBy);
  w.option(s.limit, (w, n) => w.varint(n));
  w.varint(s.offset).variant(enumIndex(ARRAY_REQUIREMENTS, s.requirement));
  w.seq(s.nested_arrays, encodeArraySubquery);
}
export function decodeArraySubquery(r: Reader): ArraySubquery {
  return {
    column_name: r.str(),
    table: r.str(),
    inner_column: r.str(),
    outer_column: r.str(),
    filters: r.seq(decodePredicate),
    select: r.option((r) => r.seq((r) => r.str())),
    order_by: r.seq(decodeOrderBy),
    limit: r.option((r) => r.varint()),
    offset: r.varint(),
    requirement: readEnum(r, ARRAY_REQUIREMENTS, "ArraySubqueryRequirement"),
    nested_arrays: r.seq(decodeArraySubquery),
  };
}
