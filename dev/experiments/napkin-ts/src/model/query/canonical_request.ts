import {
  recordDescriptorEq,
  type ColumnType,
  type RuntimeSchema,
  type Value,
} from "../../_extern/model-query.js";
import { utf8 } from "../../support/bytes.js";
import {
  structEq,
  type FlatJoin,
  type JsonValue,
  type Query,
  type RelationExpr,
  type RelationKeyRef,
  type RelationPredicate,
  type RelationQuery,
  type RelationValueRef,
  type RelationColumnRef,
} from "./ast.js";
import type {
  Aggregate,
  ArraySubquery,
  InheritsVia,
  JoinCorrelation,
  JoinVia,
  Operand,
  PolicyBranch,
  Predicate,
  ReachableVia,
} from "./policy_and_request_contracts.js";
import { QueryError, plannerColumnType, schemaTable } from "./validation.js";

type Out = number[];
type Key = number[];

const cmpKey = (a: Key, b: Key): number => {
  const n = Math.min(a.length, b.length);
  for (let i = 0; i < n; i++) if (a[i] !== b[i]) return a[i] - b[i];
  return a.length - b.length;
};
const sortByKey = <T>(items: T[], key: (t: T) => Key): void => {
  const keyed = items.map((t) => [key(t), t] as const).sort((a, b) => cmpKey(a[0], b[0]));
  keyed.forEach(([, t], i) => (items[i] = t));
};
const dedup = <T>(items: T[]): T[] => items.filter((t, i) => i === 0 || !structEq(items[i - 1], t));
const sortStrings = (s: string[]) => dedup(s.sort((a, b) => cmpKey([...utf8(a)], [...utf8(b)])));

// ---- normalization ----

export function normalizeQuery(input: Query): Query {
  const query = structuredClone(input);
  sortByKey(query.filters, canonicalPredicateKey);
  normalizeJoins(query.joins);
  for (const branch of query.policy_branches) {
    sortByKey(branch.filters, canonicalPredicateKey);
    normalizeJoins(branch.joins);
    normalizeReachable(branch.reachable);
    sortByKey(branch.inherits, canonicalInheritsKey);
    branch.inherits = dedup(branch.inherits);
  }
  sortByKey(query.policy_branches, canonicalPolicyBranchKey);
  normalizeReachable(query.reachable);
  sortByKey(query.inherits, canonicalInheritsKey);
  query.inherits = dedup(query.inherits);
  query.includes.sort((a, b) =>
    a.path !== b.path
      ? cmpKey([...utf8(a.path)], [...utf8(b.path)])
      : a.join_mode !== b.join_mode
        ? a.join_mode === "Inner"
          ? -1
          : 1
        : Number(a.require) - Number(b.require),
  );
  query.includes = dedup(query.includes);
  query.array_subqueries.forEach(normalizeArraySubquery);
  sortByKey(query.array_subqueries, canonicalArraySubqueryKey);
  query.array_subqueries = dedup(query.array_subqueries);
  if (query.select) query.select = sortStrings(query.select);
  if (query.aggregate) sortByKey(query.aggregate.aggregates, canonicalAggregateKey);
  return query;
}

function normalizeJoins(joins: JoinVia[]): void {
  for (const join of joins) {
    sortByKey(join.filters, canonicalPredicateKey);
    sortByKey(join.correlated_filters, canonicalJoinCorrelationKey);
    normalizeJoins(join.nested_joins);
  }
  sortByKey(joins, canonicalJoinKey);
}

function normalizeReachable(reachable: ReachableVia[]): void {
  for (const r of reachable) {
    sortByKey(r.access_filters, canonicalPredicateKey);
    sortByKey(r.edge_filters, canonicalPredicateKey);
    if (r.seed) sortByKey(r.seed.filters, canonicalPredicateKey);
  }
  sortByKey(reachable, canonicalReachableKey);
}

function normalizeArraySubquery(s: ArraySubquery): void {
  sortByKey(s.filters, canonicalPredicateKey);
  if (s.select) s.select = sortStrings(s.select);
  s.nested_arrays.forEach(normalizeArraySubquery);
  sortByKey(s.nested_arrays, canonicalArraySubqueryKey);
  s.nested_arrays = dedup(s.nested_arrays);
}

// ---- canonical keys ----

function canonicalFlatJoinKey(f: FlatJoin): Key {
  const b: Out = [];
  putOptStr(b, f.root_alias);
  putLen(b, f.sources.length);
  for (const s of f.sources) {
    putStr(b, s.table);
    putOptStr(b, s.alias);
    putStr(b, s.on.left);
    putStr(b, s.on.right);
  }
  return b;
}

function canonicalPolicyBranchKey(branch: PolicyBranch, schema?: RuntimeSchema): Key {
  const b: Out = [];
  putKeys(b, branch.filters, canonicalPredicateKey);
  putKeys(b, branch.joins, canonicalJoinKey);
  putKeys(b, branch.reachable, (r) => canonicalReachableKey(r, schema));
  putKeys(b, branch.inherits, canonicalInheritsKey);
  return b;
}

const AGGREGATE_TAGS = { Count: "c", Sum: "s", Avg: "a", Min: "n", Max: "x" };

export function canonicalAggregateKey(a: Aggregate): Key {
  const b: Out = [AGGREGATE_TAGS[a.function].charCodeAt(0)];
  if (a.column !== undefined) putStr(b, a.column);
  putStr(b, a.alias);
  return b;
}

const ch = (c: string) => c.charCodeAt(0);

function canonicalArraySubqueryKey(s: ArraySubquery): Key {
  const b: Out = [];
  putStr(b, s.column_name);
  putStr(b, s.table);
  putStr(b, s.inner_column);
  putStr(b, s.outer_column);
  putKeys(b, s.filters, canonicalPredicateKey);
  if (s.select) {
    b.push(ch("s"));
    putLen(b, s.select.length);
    s.select.forEach((c) => putStr(b, c));
  }
  putOrderBy(b, s.order_by);
  if (s.limit !== undefined) {
    b.push(ch("l"));
    putLen(b, s.limit);
  }
  if (s.offset !== 0) {
    b.push(ch("f"));
    putLen(b, s.offset);
  }
  b.push(ch(s.requirement === "Optional" ? "?" : s.requirement === "AtLeastOne" ? "+" : "="));
  if (s.nested_arrays.length) {
    b.push(ch("n"));
    putKeys(b, s.nested_arrays, canonicalArraySubqueryKey);
  }
  return b;
}

function putOrderBy(b: Out, orderBy: { column: string; direction: string }[]): void {
  if (!orderBy.length) return;
  b.push(ch("o"));
  putLen(b, orderBy.length);
  for (const o of orderBy) {
    putStr(b, o.column);
    b.push(ch(o.direction === "Asc" ? "a" : "d"));
  }
}

/** With a schema, a seeded user column's type is part of the key. */
function canonicalReachableKey(r: ReachableVia, schema?: RuntimeSchema): Key {
  const seedValueType =
    schema && r.seed?.user_column !== undefined
      ? plannerColumnType(schemaTable(schema, r.seed.table), r.seed.user_column)
      : undefined;
  const b: Out = [];
  putStr(b, r.access_table);
  putStr(b, r.access_row_column);
  putStr(b, r.access_team_column);
  putJoinTarget(b, r.access_team_target);
  putBytes(b, canonicalOperandKey(r.from));
  putKeys(b, r.access_filters, canonicalPredicateKey);
  putStr(b, r.edge_table);
  putStr(b, r.edge_member_column);
  putStr(b, r.edge_parent_column);
  if (r.bound.kind === "Fixpoint") b.push(ch("f"));
  else {
    b.push(ch("d"));
    putLen(b, r.bound.depth);
  }
  for (const f of r.edge_filters) putBytes(b, canonicalPredicateKey(f));
  if (r.seed) {
    const seed = r.seed;
    b.push(ch("s"));
    putStr(b, seed.table);
    if (seed.user_column !== undefined && seed.user_claim !== undefined) {
      b.push(ch("u"));
      putStr(b, seed.user_column);
      putStr(b, seed.user_claim);
      if (seedValueType !== undefined) {
        b.push(ch("t"));
        putColumnType(b, seedValueType);
      }
    }
    putStr(b, seed.team_column);
    putKeys(b, seed.filters, canonicalPredicateKey);
  }
  return b;
}

function putJoinTarget(b: Out, t: string): void {
  if (t === "RowId") b.push(ch("r"));
  else if (t === "Uncorrelated") b.push(ch("e"));
}

const INHERITS_TAGS = { Select: "s", Insert: "i", Update: "u", Delete: "d" };

function canonicalInheritsKey(i: InheritsVia): Key {
  const b: Out = [];
  putStr(b, i.parent_column);
  b.push(ch(INHERITS_TAGS[i.operation]));
  if (i.max_depth !== undefined) {
    b.push(ch("d"));
    putLen(b, i.max_depth);
  } else b.push(ch("u"));
  return b;
}

function canonicalJoinKey(j: JoinVia): Key {
  const b: Out = [];
  putStr(b, j.table);
  putStr(b, j.on_column);
  putJoinTarget(b, j.target);
  if (j.source_column !== undefined) {
    b.push(ch("s"));
    putStr(b, j.source_column);
  }
  if (j.source_lookup) {
    b.push(ch("l"));
    putStr(b, j.source_lookup.table);
    putStr(b, j.source_lookup.row_id_source_column);
    putStr(b, j.source_lookup.value_column);
  }
  if (j.correlated_filters.length) {
    b.push(ch("c"));
    putKeys(b, j.correlated_filters, canonicalJoinCorrelationKey);
  }
  if (j.nested_joins.length) {
    b.push(ch("j"));
    putKeys(b, j.nested_joins, canonicalJoinKey);
  }
  for (const f of j.filters) putBytes(b, canonicalPredicateKey(f));
  return b;
}

function canonicalJoinCorrelationKey(c: JoinCorrelation): Key {
  const b: Out = [];
  putStr(b, c.join_column);
  putStr(b, c.source_column);
  return b;
}

const PREDICATE_TAGS = {
  Gt: "g",
  Gte: "G",
  Lt: "t",
  Lte: "T",
  Contains: "c",
  Eq: "e",
  Ne: "n",
} as const;

function canonicalPredicateKey(p: Predicate): Key {
  const b: Out = [];
  switch (p.kind) {
    case "All":
    case "Any":
      b.push(ch(p.kind === "All" ? "A" : "O"));
      putSortedKeys(b, p.predicates.map(canonicalPredicateKey));
      break;
    case "Not":
      b.push(ch("!"));
      putBytes(b, canonicalPredicateKey(p.predicate));
      break;
    case "In":
      b.push(ch("i"));
      putBytes(b, canonicalOperandKey(p.left));
      putSortedKeys(b, p.values.map(canonicalOperandKey));
      break;
    case "IsNull":
      b.push(ch("0"));
      putBytes(b, canonicalOperandKey(p.operand));
      break;
    case "EnumMatch":
      b.push(ch("m"));
      putStr(b, p.column);
      putStr(b, p.case);
      putBytes(b, canonicalPredicateKey(p.payload));
      break;
    default: {
      b.push(ch(PREDICATE_TAGS[p.kind]));
      const operands = [canonicalOperandKey(p.left), canonicalOperandKey(p.right)];
      // Symmetric comparisons do not depend on operand order.
      if (p.kind === "Eq" || p.kind === "Ne") operands.sort(cmpKey);
      operands.forEach((o) => putBytes(b, o));
    }
  }
  return b;
}

function canonicalOperandKey(o: Operand): Key {
  const b: Out = [];
  if (o.kind === "Literal") {
    b.push(ch("l"));
    putValue(b, o.value);
  } else {
    b.push(ch(o.kind === "Column" ? "c" : o.kind === "Param" ? "p" : "a"));
    putStr(b, o.name);
  }
  return b;
}

export function canonicalQueryBytesForSchema(q: Query, schema: RuntimeSchema): Uint8Array {
  const b: Out = [...utf8("jazz-query-v0")];
  putStr(b, q.table);
  putKeys(b, q.filters, canonicalPredicateKey);
  putKeys(b, q.joins, canonicalJoinKey);
  if (q.flat_join) {
    b.push(ch("j"));
    putBytes(b, canonicalFlatJoinKey(q.flat_join));
  }
  if (q.policy_branches.length) {
    b.push(ch("b"));
    putKeys(b, q.policy_branches, (x) => canonicalPolicyBranchKey(x, schema));
  }
  if (q.reachable.length) {
    b.push(ch("r"));
    putKeys(b, q.reachable, (x) => canonicalReachableKey(x, schema));
  }
  if (q.inherits.length) {
    b.push(ch("i"));
    putKeys(b, q.inherits, canonicalInheritsKey);
  }
  putLen(b, q.includes.length);
  for (const inc of q.includes) {
    putStr(b, inc.path);
    b.push(ch(inc.join_mode === "Inner" ? "i" : "h"), inc.require ? 1 : 0);
  }
  if (q.array_subqueries.length) {
    b.push(ch("y"));
    putKeys(b, q.array_subqueries, canonicalArraySubqueryKey);
  }
  if (q.select) {
    b.push(ch("s"));
    putLen(b, q.select.length);
    q.select.forEach((c) => putStr(b, c));
  }
  putOrderBy(b, q.order_by);
  if (q.aggregate) {
    b.push(ch("a"));
    putKeys(b, q.aggregate.aggregates, canonicalAggregateKey);
    putOptStr(b, q.aggregate.group_by);
  }
  if (q.limit !== undefined || q.offset !== 0) {
    b.push(ch("p"));
    if (q.limit !== undefined) {
      b.push(1);
      putLen(b, q.limit);
    } else b.push(0);
    putLen(b, q.offset);
  }
  if (q.relation) {
    b.push(ch("u"));
    putBytes(b, canonicalRelationQueryKey(q.relation));
  }
  return Uint8Array.from(b);
}

// ---- relation identity: the serde JSON tree, written with sorted object keys ----

const colJson = (c: RelationColumnRef): JsonValue => ({ scope: c.scope ?? null, column: c.column });
const keyJson = (k: RelationKeyRef): JsonValue =>
  k.kind === "Column" ? { Column: colJson(k.column) } : { RowId: k.row };

function valueRefJson(v: RelationValueRef): JsonValue {
  switch (v.kind) {
    case "Literal":
      return { Literal: v.value };
    case "Param":
      return { Param: v.name };
    case "SessionRef":
      return { SessionRef: v.path };
    case "RowId":
      return { RowId: v.row };
    default:
      return { [v.kind]: colJson(v.column) };
  }
}

function predicateJson(p: RelationPredicate): JsonValue {
  switch (p.kind) {
    case "Cmp":
      return { Cmp: { left: colJson(p.left), op: p.op, right: valueRefJson(p.right) } };
    case "IsNull":
    case "IsNotNull":
      return { [p.kind]: { column: colJson(p.column) } };
    case "In":
      return { In: { left: colJson(p.left), values: p.values.map(valueRefJson) } };
    case "Contains":
      return { Contains: { left: colJson(p.left), right: valueRefJson(p.right) } };
    case "EnumMatch":
      return {
        EnumMatch: { column: colJson(p.column), case: p.case, payload: predicateJson(p.payload) },
      };
    case "And":
    case "Or":
      return { [p.kind]: p.predicates.map(predicateJson) };
    case "Not":
      return { Not: predicateJson(p.predicate) };
    default:
      return p.kind;
  }
}

function exprJson(e: RelationExpr): JsonValue {
  switch (e.kind) {
    case "TableScan":
      return {
        TableScan: e.alias !== undefined ? { table: e.table, alias: e.alias } : { table: e.table },
      };
    case "Filter":
      return { Filter: { input: exprJson(e.input), predicate: predicateJson(e.predicate) } };
    case "Union":
      return {
        Union: { inputs: e.inputs.map((a) => ({ label: a.label, input: exprJson(a.input) })) },
      };
    case "Join":
      return {
        Join: {
          left: exprJson(e.left),
          right: exprJson(e.right),
          on: e.on.map((c) => ({ left: colJson(c.left), right: colJson(c.right) })),
          join_kind: e.join_kind,
        },
      };
    case "Project":
      return {
        Project: {
          input: exprJson(e.input),
          columns: e.columns.map((c) => ({ alias: c.alias, expr: keyJson(c.expr) })),
        },
      };
    case "Gather":
      return {
        Gather: {
          seed: exprJson(e.seed),
          step: exprJson(e.step),
          frontier_key: keyJson(e.frontier_key),
          bound: e.bound.kind === "Fixpoint" ? "Fixpoint" : { MaxDepth: e.bound.depth },
          dedupe_key: e.dedupe_key.map(keyJson),
        },
      };
    case "Distinct":
      return { Distinct: { input: exprJson(e.input), key: e.key.map(keyJson) } };
    case "OrderBy":
      return {
        OrderBy: {
          input: exprJson(e.input),
          terms: e.terms.map((t) => ({ column: colJson(t.column), direction: t.direction })),
        },
      };
    case "Offset":
      return { Offset: { input: exprJson(e.input), offset: e.offset } };
    case "Limit":
      return { Limit: { input: exprJson(e.input), limit: e.limit } };
  }
}

function canonicalRelationQueryKey(q: RelationQuery): Key {
  const write = (v: JsonValue, out: Out): void => {
    if (v === null) out.push(ch("n"));
    else if (typeof v === "boolean") out.push(ch(v ? "t" : "f"));
    else if (typeof v === "number") {
      if (!Number.isFinite(v))
        throw new QueryError({
          kind: "UnsupportedRelationQuery",
          detail: "encode relation query: non-finite number",
        });
      out.push(ch("#"));
      putStr(out, Number.isInteger(v) ? v.toFixed(0) : String(v));
    } else if (typeof v === "string") {
      out.push(ch("s"));
      putStr(out, v);
    } else if (Array.isArray(v)) {
      out.push(ch("["));
      putLen(out, v.length);
      v.forEach((x) => write(x, out));
    } else {
      const entries = Object.entries(v).sort(([a], [b]) => cmpKey([...utf8(a)], [...utf8(b)]));
      out.push(ch("{"));
      putLen(out, entries.length);
      for (const [k, x] of entries) {
        putStr(out, k);
        write(x, out);
      }
    }
  };
  const b: Out = [...utf8("jazz-relation-v1")];
  write({ rel: exprJson(q.rel) }, b);
  return b;
}

// ---- bindings and literal values ----

export function canonicalBindingBytes(values: Iterable<[string, Value]>): Uint8Array {
  const entries = [...values];
  const b: Out = [...utf8("jazz-binding-v0")];
  putLen(b, entries.length);
  for (const [name, value] of entries) {
    putStr(b, name);
    validateBindingValue(value);
    putValue(b, value);
  }
  return Uint8Array.from(b);
}

const mismatch = () => new QueryError({ kind: "OperandTypeMismatch" });

/** Binding identities only accept values a public caller can supply. */
function validateBindingValue(v: Value): void {
  switch (v.kind) {
    case "Tuple":
    case "Array":
      return v.value.forEach(validateBindingValue);
    case "Nullable":
      if (v.value !== undefined) validateBindingValue(v.value);
      return;
    case "Record":
      return validatePublicColumnType({ kind: "Record", descriptor: v.value.descriptor() });
    // U48 is an engine-owned stamp width, never a public query binding.
    case "Enum":
    case "Large":
    case "U48":
      throw mismatch();
  }
}

function validatePublicColumnType(t: ColumnType): void {
  switch (t.kind) {
    case "Tuple":
      return t.types.forEach(validatePublicColumnType);
    case "Array":
    case "Nullable":
      return validatePublicColumnType(t.inner);
    case "Record":
      for (const f of t.descriptor.fields()) validatePublicColumnType(f.value_type);
      return;
    case "Enum":
      for (const c of t.schema.cases)
        for (const f of c.payload.fields()) validatePublicColumnType(f.value_type);
      return;
    case "Internal":
    case "U48":
      throw mismatch();
  }
}

const SCALAR_VALUE_TYPES = new Set([
  "U8",
  "U16",
  "U32",
  "U64",
  "I32",
  "I64",
  "F64",
  "Bool",
  "String",
  "Bytes",
  "Uuid",
]);

export function valueType(v: Value): ColumnType {
  if (SCALAR_VALUE_TYPES.has(v.kind)) return { kind: v.kind };
  switch (v.kind) {
    case "EnumTag":
      return { kind: "U8" };
    case "Tuple":
      return { kind: "Tuple", types: v.value.map(valueType) };
    case "Array": {
      // Infer from the first element, but walk every element so unsupported values are rejected.
      const types = v.value.map(valueType);
      return { kind: "Array", inner: types[0] ?? { kind: "Bytes" } };
    }
    case "Nullable":
      return {
        kind: "Nullable",
        inner: v.value !== undefined ? valueType(v.value) : { kind: "Bytes" },
      };
    case "Record": {
      const t = { kind: "Record", descriptor: v.value.descriptor() };
      validatePublicColumnType(t);
      return t;
    }
    default:
      throw mismatch();
  }
}

export function valueMatchesType(v: Value, t: ColumnType): boolean {
  if (SCALAR_VALUE_TYPES.has(v.kind)) return v.kind === t.kind;
  switch (v.kind) {
    case "EnumTag":
      return t.kind === "EnumTag";
    case "Tuple":
      return (
        t.kind === "Tuple" &&
        v.value.length === t.types.length &&
        v.value.every((x: Value, i: number) => valueMatchesType(x, t.types[i]))
      );
    case "Array":
      return t.kind === "Array" && v.value.every((x: Value) => valueMatchesType(x, t.inner));
    case "Nullable":
      return t.kind === "Nullable" && (v.value === undefined || valueMatchesType(v.value, t.inner));
    case "Record":
      return t.kind === "Record" && recordDescriptorEq(v.value.descriptor(), t.descriptor);
    default:
      return false;
  }
}

const be = (width: 1 | 2 | 4 | 8, n: number | bigint, signed = false): number[] => {
  const view = new DataView(new ArrayBuffer(width));
  if (width === 1) view.setUint8(0, Number(n));
  else if (width === 2) view.setUint16(0, Number(n));
  else if (width === 4) signed ? view.setInt32(0, Number(n)) : view.setUint32(0, Number(n));
  else signed ? view.setBigInt64(0, BigInt(n)) : view.setBigUint64(0, BigInt(n));
  return [...new Uint8Array(view.buffer)];
};
const f64Bits = (x: number): number[] => {
  const view = new DataView(new ArrayBuffer(8));
  view.setFloat64(0, x);
  return [...new Uint8Array(view.buffer)];
};

export function putValue(b: Out, v: Value): void {
  switch (v.kind) {
    case "U8":
      return void b.push(1, v.value);
    case "U16":
      return void b.push(2, ...be(2, v.value));
    case "U32":
      return void b.push(3, ...be(4, v.value));
    case "U64":
      return void b.push(4, ...be(8, v.value));
    case "I32":
      return void b.push(15, ...be(4, v.value, true));
    case "I64":
      return void b.push(14, ...be(8, v.value, true));
    case "F64":
      return void b.push(5, ...f64Bits(v.value));
    case "Bool":
      return void b.push(6, v.value ? 1 : 0);
    case "String":
      b.push(7);
      return putStr(b, v.value);
    case "Bytes":
      b.push(8);
      return putBytes(b, v.value);
    case "Uuid":
      return void b.push(9, ...v.value);
    case "EnumTag":
      return void b.push(10, v.value);
    case "Tuple":
    case "Array":
      b.push(v.kind === "Tuple" ? 11 : 12);
      putLen(b, v.value.length);
      return v.value.forEach((x: Value) => putValue(b, x));
    case "Nullable":
      if (v.value === undefined) return void b.push(13, 0);
      b.push(13, 1);
      return putValue(b, v.value);
    case "Record":
      b.push(16);
      putColumnType(b, { kind: "Record", descriptor: v.value.descriptor() });
      return putBytes(b, v.value.raw());
    // Groove-native arms rejected by valueType; keep their fallback encodings total.
    case "Enum":
      b.push(17, ...be(4, v.value.tag()));
      putColumnType(b, { kind: "Record", descriptor: v.value.record().descriptor() });
      return putBytes(b, v.value.record().raw());
    case "U48":
      return void b.push(19, ...be(8, v.value));
    case "Large":
      b.push(
        18,
        v.value.kind === "Bytes" ? 0 : v.value.kind === "String" ? 1 : 2,
        v.value.format_version,
      );
      return void b.push(...v.value.logical_hash);
  }
}

const COLUMN_TYPE_TAGS: Record<string, number> = {
  U8: 0,
  U16: 1,
  U32: 2,
  U64: 3,
  I32: 4,
  I64: 5,
  F64: 6,
  Bool: 7,
  String: 8,
  Bytes: 9,
  Uuid: 10,
  Internal: 17,
  U48: 18,
};

export function putColumnType(b: Out, t: ColumnType): void {
  switch (t.kind) {
    case "EnumTag":
      b.push(11);
      putStr(b, t.schema.name);
      putLen(b, t.schema.variants.length);
      return t.schema.variants.forEach((v: string) => putStr(b, v));
    case "Tuple":
      b.push(12);
      putLen(b, t.types.length);
      return t.types.forEach((x: ColumnType) => putColumnType(b, x));
    case "Array":
    case "Nullable":
      b.push(t.kind === "Array" ? 13 : 14);
      return putColumnType(b, t.inner);
    case "Record": {
      const fields = t.descriptor.fields();
      b.push(15);
      putLen(b, fields.length);
      for (const f of fields) {
        putOptStr(b, f.name);
        // Physical slot identity is part of a record literal's type.
        const id = f.identity;
        if (!id) b.push(0);
        else if (id.kind === "Name") {
          b.push(1);
          putStr(b, id.name);
        } else if (id.kind === "Slot") b.push(2, ...be(8, id.slot));
        else {
          b.push(3);
          putStr(b, id.name);
          b.push(...be(8, id.slot));
        }
        putColumnType(b, f.value_type);
      }
      return;
    }
    case "Enum":
      b.push(16, ...be(8, t.schema.registry_id));
      putStr(b, t.schema.name);
      putLen(b, t.schema.cases.length);
      for (const c of t.schema.cases) {
        putStr(b, c.name);
        putColumnType(b, { kind: "Record", descriptor: c.payload });
      }
      return;
    default:
      b.push(COLUMN_TYPE_TAGS[t.kind]);
  }
}

// ---- primitive writers ----

const putStr = (b: Out, s: string) => putBytes(b, utf8(s));
function putBytes(b: Out, v: ArrayLike<number>): void {
  putLen(b, v.length);
  for (let i = 0; i < v.length; i++) b.push(v[i]);
}
function putKeys<T>(b: Out, items: T[], key: (t: T) => Key): void {
  putLen(b, items.length);
  for (const it of items) putBytes(b, key(it));
}
function putSortedKeys(b: Out, keys: Key[]): void {
  keys.sort(cmpKey);
  putLen(b, keys.length);
  keys.forEach((k) => putBytes(b, k));
}
function putOptStr(b: Out, s: string | undefined): void {
  if (s === undefined) b.push(0);
  else {
    b.push(1);
    putStr(b, s);
  }
}
const putLen = (b: Out, len: number) => void b.push(...be(8, len));
