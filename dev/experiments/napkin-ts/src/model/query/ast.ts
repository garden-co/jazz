import { uuidFromString } from "../../support/hash.js";
import type { Reader, Writer } from "../../support/postcard.js";
import type {
  AggregateQuery,
  ArraySubquery,
  Include,
  InheritsVia,
  JoinVia,
  OrderBy,
  OrderDirection,
  PolicyBranch,
  Predicate,
  ReachableVia,
  RecursionBound,
} from "./policy_and_request_contracts.js";
import {
  decodeAggregateQuery,
  decodeArraySubquery,
  decodeInclude,
  decodeInheritsVia,
  decodeJoinVia,
  decodeOrderBy,
  decodePolicyBranch,
  decodePredicate,
  decodeReachableVia,
  encodeAggregateQuery,
  encodeArraySubquery,
  encodeInclude,
  encodeInheritsVia,
  encodeJoinVia,
  encodeOrderBy,
  encodePolicyBranch,
  encodePredicate,
  encodeReachableVia,
} from "./policy_and_request_contracts.js";
import { decodeRelationQueryWire, encodeRelationQueryWire } from "./relation_codec.js";

/** Namespace used for query shape and binding UUIDv5 ids. */
export const QUERY_NAMESPACE = uuidFromString("5d39e9ed-88f3-5b58-b8db-8786b02f5d2f");

export interface Query {
  table: string;
  filters: Predicate[];
  joins: JoinVia[];
  flat_join?: FlatJoin;
  policy_branches: PolicyBranch[];
  reachable: ReachableVia[];
  inherits: InheritsVia[];
  includes: Include[];
  array_subqueries: ArraySubquery[];
  select?: string[];
  order_by: OrderBy[];
  aggregate?: AggregateQuery;
  limit?: number;
  offset: number;
  relation?: RelationQuery;
}

export interface FlatJoin {
  root_alias?: string;
  sources: FlatJoinSource[];
}
export interface FlatJoinSource {
  table: string;
  alias?: string;
  on: FlatJoinOn;
}
export interface FlatJoinOn {
  left: string;
  right: string;
}

export interface RelationQuery {
  rel: RelationExpr;
}

export type RelationExpr =
  | { kind: "TableScan"; table: string; alias?: string }
  | { kind: "Filter"; input: RelationExpr; predicate: RelationPredicate }
  | { kind: "Union"; inputs: RelationUnionArm[] }
  | {
      kind: "Join";
      left: RelationExpr;
      right: RelationExpr;
      on: RelationJoinCondition[];
      join_kind: RelationJoinKind;
    }
  | { kind: "Project"; input: RelationExpr; columns: RelationProjectColumn[] }
  | {
      kind: "Gather";
      seed: RelationExpr;
      step: RelationExpr;
      frontier_key: RelationKeyRef;
      bound: RecursionBound;
      dedupe_key: RelationKeyRef[];
    }
  | { kind: "Distinct"; input: RelationExpr; key: RelationKeyRef[] }
  | { kind: "OrderBy"; input: RelationExpr; terms: RelationOrderBy[] }
  | { kind: "Offset"; input: RelationExpr; offset: number }
  | { kind: "Limit"; input: RelationExpr; limit: number };

export interface RelationUnionArm {
  label: string;
  input: RelationExpr;
}

export type RelationPredicate =
  | { kind: "Cmp"; left: RelationColumnRef; op: RelationCmpOp; right: RelationValueRef }
  | { kind: "IsNull" | "IsNotNull"; column: RelationColumnRef }
  | { kind: "In"; left: RelationColumnRef; values: RelationValueRef[] }
  | { kind: "Contains"; left: RelationColumnRef; right: RelationValueRef }
  | { kind: "EnumMatch"; column: RelationColumnRef; case: string; payload: RelationPredicate }
  | { kind: "And" | "Or"; predicates: RelationPredicate[] }
  | { kind: "Not"; predicate: RelationPredicate }
  | { kind: "True" | "False" };

export const RELATION_CMP_OPS = ["Eq", "Ne", "Lt", "Le", "Gt", "Ge"] as const;
export type RelationCmpOp = (typeof RELATION_CMP_OPS)[number];

export interface RelationColumnRef {
  scope?: string;
  column: string;
}

export type JsonValue =
  | null
  | boolean
  | number
  | string
  | JsonValue[]
  | { [key: string]: JsonValue };

export type RelationValueRef =
  | { kind: "Literal"; value: JsonValue }
  | { kind: "Param"; name: string }
  | { kind: "SessionRef"; path: string[] }
  | { kind: "OuterColumn" | "FrontierColumn"; column: RelationColumnRef }
  | { kind: "RowId"; row: RelationRowIdRef };

export const RELATION_ROW_ID_REFS = ["Current", "Outer", "Frontier"] as const;
export type RelationRowIdRef = (typeof RELATION_ROW_ID_REFS)[number];
export type RelationJoinKind = "Inner" | "Left";

export interface RelationJoinCondition {
  left: RelationColumnRef;
  right: RelationColumnRef;
}

/** Shared by `RelationKeyRef` and `RelationProjectExpr`. */
export type RelationKeyRef =
  | { kind: "Column"; column: RelationColumnRef }
  | { kind: "RowId"; row: RelationRowIdRef };
export type RelationProjectExpr = RelationKeyRef;

export interface RelationProjectColumn {
  alias: string;
  expr: RelationProjectExpr;
}
export interface RelationOrderBy {
  column: RelationColumnRef;
  direction: OrderDirection;
}

/** Structural equality (Rust `PartialEq` derives over the query AST). */
export function structEq(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  if (a == null || b == null) return a == b;
  if (typeof a !== "object" || typeof b !== "object") return false;
  if (a instanceof Uint8Array || b instanceof Uint8Array) {
    if (!(a instanceof Uint8Array && b instanceof Uint8Array) || a.length !== b.length)
      return false;
    return a.every((x, i) => x === b[i]);
  }
  if (Array.isArray(a) !== Array.isArray(b)) return false;
  const ka = Object.keys(a).filter((k) => (a as any)[k] !== undefined);
  const kb = Object.keys(b).filter((k) => (b as any)[k] !== undefined);
  if (ka.length !== kb.length) return false;
  return ka.every((k) => structEq((a as any)[k], (b as any)[k]));
}

export const cloneQuery = (q: Query): Query => structuredClone(q);

// Postcard form of `Query`, in serde field order. The relation travels as its typed wire tree.
export function encodeQuery(w: Writer, q: Query): void {
  w.str(q.table);
  w.seq(q.filters, encodePredicate);
  w.seq(q.joins, encodeJoinVia);
  w.option(q.flat_join, encodeFlatJoin);
  w.seq(q.policy_branches, encodePolicyBranch);
  w.seq(q.reachable, encodeReachableVia);
  w.seq(q.inherits, encodeInheritsVia);
  w.seq(q.includes, encodeInclude);
  w.seq(q.array_subqueries, encodeArraySubquery);
  w.option(q.select, (w, s) => w.seq(s, (w, c) => w.str(c)));
  w.seq(q.order_by, encodeOrderBy);
  w.option(q.aggregate, encodeAggregateQuery);
  w.option(q.limit, (w, n) => w.varint(n));
  w.varint(q.offset);
  w.option(q.relation, encodeRelationQueryWire);
}

export function decodeQuery(r: Reader): Query {
  return {
    table: r.str(),
    filters: r.seq(decodePredicate),
    joins: r.seq(decodeJoinVia),
    flat_join: r.option(decodeFlatJoin),
    policy_branches: r.seq(decodePolicyBranch),
    reachable: r.seq(decodeReachableVia),
    inherits: r.seq(decodeInheritsVia),
    includes: r.seq(decodeInclude),
    array_subqueries: r.seq(decodeArraySubquery),
    select: r.option((r) => r.seq((r) => r.str())),
    order_by: r.seq(decodeOrderBy),
    aggregate: r.option(decodeAggregateQuery),
    limit: r.option((r) => r.varint()),
    offset: r.varint(),
    relation: r.option(decodeRelationQueryWire),
  };
}

function encodeFlatJoin(w: Writer, f: FlatJoin): void {
  w.option(f.root_alias, (w, s) => w.str(s));
  w.seq(f.sources, (w, s) => {
    w.str(s.table);
    w.option(s.alias, (w, a) => w.str(a));
    w.str(s.on.left).str(s.on.right);
  });
}

function decodeFlatJoin(r: Reader): FlatJoin {
  return {
    root_alias: r.option((r) => r.str()),
    sources: r.seq((r) => ({
      table: r.str(),
      alias: r.option((r) => r.str()),
      on: { left: r.str(), right: r.str() },
    })),
  };
}
