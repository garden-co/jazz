// Typed Postcard representation for relation queries. JSON literals travel as a
// typed recursive tree; decoding charges depth/node/string budgets before the
// attacker-controlled tree exists.
import { utf8 } from "../../support/bytes.js";
import { PostcardError, Reader, Writer, decode } from "../../support/postcard.js";
import {
  RELATION_CMP_OPS,
  RELATION_ROW_ID_REFS,
  type JsonValue,
  type RelationColumnRef,
  type RelationExpr,
  type RelationKeyRef,
  type RelationPredicate,
  type RelationQuery,
  type RelationValueRef,
} from "./ast.js";
import type { OrderDirection } from "./policy_and_request_contracts.js";

const MAX_RELATION_BYTES = 1 << 20;
/** Deepest relation tree the typed Postcard decoder accepts. */
export const MAX_RELATION_DEPTH = 128;
const MAX_RELATION_ITEMS = 4096;
const MAX_RELATION_STRING_BYTES = 1 << 16;

export class RelationWireError extends Error {
  constructor(
    readonly kind: "TooLarge" | "Postcard" | "NonFiniteNumber" | "Invalid",
    detail = "",
  ) {
    super(
      kind === "TooLarge"
        ? "relation query is too large"
        : kind === "Postcard"
          ? `malformed Postcard relation query: ${detail}`
          : kind === "NonFiniteNumber"
            ? "relation query contains a non-finite JSON number"
            : `relation query ${detail}`,
    );
  }
}

const EXPR_KINDS = [
  "TableScan",
  "Filter",
  "Union",
  "Join",
  "Project",
  "Gather",
  "Distinct",
  "OrderBy",
  "Offset",
  "Limit",
] as const;
const PRED_KINDS = [
  "Cmp",
  "IsNull",
  "IsNotNull",
  "In",
  "Contains",
  "EnumMatch",
  "And",
  "Or",
  "Not",
  "True",
  "False",
] as const;
const VALUE_KINDS = [
  "Literal",
  "Param",
  "SessionRef",
  "OuterColumn",
  "FrontierColumn",
  "RowId",
] as const;
const DIRECTIONS: OrderDirection[] = ["Asc", "Desc"];

// ---- encode ----

const u32 = (v: number): number => {
  if (v > 0xffffffff) throw new RelationWireError("Invalid", "dimension exceeds u32");
  return v;
};

function wColumn(w: Writer, c: RelationColumnRef): void {
  w.option(c.scope, (w, s) => w.str(s)).str(c.column);
}
function wKey(w: Writer, k: RelationKeyRef): void {
  if (k.kind === "Column") wColumn(w.variant(0), k.column);
  else w.variant(1).variant(RELATION_ROW_ID_REFS.indexOf(k.row));
}

function wJson(w: Writer, v: JsonValue): void {
  if (v === null) w.variant(0);
  else if (typeof v === "boolean") w.variant(1).bool(v);
  else if (typeof v === "number") {
    if (!Number.isFinite(v)) throw new RelationWireError("NonFiniteNumber");
    if (Number.isInteger(v) && v < 0) w.variant(2).zigzag64(BigInt(v));
    else if (Number.isInteger(v)) w.variant(3).varint64(BigInt(v));
    else {
      const view = new DataView(new ArrayBuffer(8));
      view.setFloat64(0, v);
      w.variant(4).varint64(view.getBigUint64(0));
    }
  } else if (typeof v === "string") w.variant(5).str(v);
  else if (Array.isArray(v)) w.variant(6).seq(v, wJson);
  else {
    // serde_json maps iterate in key order.
    const entries = Object.entries(v).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
    w.variant(7).seq(entries, (w, [k, x]) => {
      w.str(k);
      wJson(w, x);
    });
  }
}

function wValue(w: Writer, v: RelationValueRef): void {
  w.variant(VALUE_KINDS.indexOf(v.kind));
  switch (v.kind) {
    case "Literal":
      return wJson(w, v.value);
    case "Param":
      w.str(v.name);
      return;
    case "SessionRef":
      w.seq(v.path, (w, s) => w.str(s));
      return;
    case "RowId":
      w.variant(RELATION_ROW_ID_REFS.indexOf(v.row));
      return;
    default:
      wColumn(w, v.column);
  }
}

function wPredicate(w: Writer, p: RelationPredicate): void {
  w.variant(PRED_KINDS.indexOf(p.kind));
  switch (p.kind) {
    case "Cmp":
      wColumn(w, p.left);
      w.variant(RELATION_CMP_OPS.indexOf(p.op));
      return wValue(w, p.right);
    case "IsNull":
    case "IsNotNull":
      return wColumn(w, p.column);
    case "In":
      wColumn(w, p.left);
      w.seq(p.values, wValue);
      return;
    case "Contains":
      wColumn(w, p.left);
      return wValue(w, p.right);
    case "EnumMatch":
      wColumn(w, p.column);
      w.str(p.case);
      return wPredicate(w, p.payload);
    case "And":
    case "Or":
      w.seq(p.predicates, wPredicate);
      return;
    case "Not":
      return wPredicate(w, p.predicate);
  }
}

function wExpr(w: Writer, e: RelationExpr): void {
  w.variant(EXPR_KINDS.indexOf(e.kind));
  switch (e.kind) {
    case "TableScan":
      w.str(e.table).option(e.alias, (w, a) => w.str(a));
      return;
    case "Filter":
      wExpr(w, e.input);
      return wPredicate(w, e.predicate);
    case "Union":
      w.seq(e.inputs, (w, arm) => {
        w.str(arm.label);
        wExpr(w, arm.input);
      });
      return;
    case "Join":
      wExpr(w, e.left);
      wExpr(w, e.right);
      w.seq(e.on, (w, c) => {
        wColumn(w, c.left);
        wColumn(w, c.right);
      });
      w.variant(e.join_kind === "Inner" ? 0 : 1);
      return;
    case "Project":
      wExpr(w, e.input);
      w.seq(e.columns, (w, c) => {
        w.str(c.alias);
        wKey(w, c.expr);
      });
      return;
    case "Gather":
      wExpr(w, e.seed);
      wExpr(w, e.step);
      wKey(w, e.frontier_key);
      if (e.bound.kind === "Fixpoint") w.variant(0);
      else w.variant(1).varint(u32(e.bound.depth));
      w.seq(e.dedupe_key, wKey);
      return;
    case "Distinct":
      wExpr(w, e.input);
      w.seq(e.key, wKey);
      return;
    case "OrderBy":
      wExpr(w, e.input);
      w.seq(e.terms, (w, t) => {
        wColumn(w, t.column);
        w.variant(DIRECTIONS.indexOf(t.direction));
      });
      return;
    case "Offset":
      wExpr(w, e.input);
      w.varint(u32(e.offset));
      return;
    case "Limit":
      wExpr(w, e.input);
      w.varint(u32(e.limit));
      return;
  }
}

// ---- budgeted decode ----

class BudgetedReader {
  depth = 0;
  nodes = 0;
  strings = 0;
  constructor(readonly r: Reader) {}

  nested<T>(f: () => T): T {
    this.depth++;
    this.nodes++;
    if (this.depth > MAX_RELATION_DEPTH + 1 || this.nodes > MAX_RELATION_ITEMS)
      throw new PostcardError("relation-query tree limit");
    try {
      return f();
    } finally {
      this.depth = Math.max(0, this.depth - 1);
    }
  }
  node<T>(f: () => T): T {
    if (++this.nodes > MAX_RELATION_ITEMS) throw new PostcardError("relation-query node limit");
    return f();
  }
  str(): string {
    const bytes = this.r.bytes();
    this.strings += bytes.length;
    if (bytes.length > MAX_RELATION_STRING_BYTES || this.strings > MAX_RELATION_BYTES)
      throw new PostcardError("relation-query string limit");
    return new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  }
  optStr(): string | undefined {
    return this.r.bool() ? this.str() : undefined;
  }
  vec<T>(f: () => T): T[] {
    const n = this.r.varint();
    const out: T[] = [];
    for (let i = 0; i < n; i++) {
      const v = f();
      if (out.length === MAX_RELATION_ITEMS)
        throw new PostcardError("relation-query collection limit");
      out.push(v);
    }
    return out;
  }
  variant<T>(all: readonly T[], what: string): T {
    return all[this.r.variant(all.length, what)];
  }

  column(): RelationColumnRef {
    return { scope: this.optStr(), column: this.str() };
  }
  key(): RelationKeyRef {
    return this.r.variant(2, "RelationKeyRef") === 0
      ? { kind: "Column", column: this.column() }
      : { kind: "RowId", row: this.variant(RELATION_ROW_ID_REFS, "RelationRowIdRef") };
  }
  json(): JsonValue {
    switch (this.r.variant(8, "WireJson")) {
      case 0:
        return null;
      case 1:
        return this.r.bool();
      case 2:
        return Number(this.r.zigzag64());
      case 3:
        return Number(this.r.varint64());
      case 4: {
        const view = new DataView(new ArrayBuffer(8));
        view.setBigUint64(0, this.r.varint64());
        return view.getFloat64(0);
      }
      case 5:
        return this.str();
      case 6:
        return this.vec(() => this.nested(() => this.json()));
      default: {
        const entries = this.vec((): [string, JsonValue] => [
          this.str(),
          this.nested(() => this.json()),
        ]);
        const out: { [key: string]: JsonValue } = {};
        for (const [k, v] of entries) out[k] = v;
        return out;
      }
    }
  }
  value(): RelationValueRef {
    const kind = this.variant(VALUE_KINDS, "RelationValueRef");
    switch (kind) {
      case "Literal":
        return { kind, value: this.nested(() => this.json()) };
      case "Param":
        return { kind, name: this.str() };
      case "SessionRef":
        return { kind, path: this.vec(() => this.str()) };
      case "RowId":
        return { kind, row: this.variant(RELATION_ROW_ID_REFS, "RelationRowIdRef") };
      default:
        return { kind, column: this.column() };
    }
  }
  predicate(): RelationPredicate {
    const kind = this.variant(PRED_KINDS, "RelationPredicate");
    switch (kind) {
      case "Cmp":
        return {
          kind,
          left: this.column(),
          op: this.variant(RELATION_CMP_OPS, "RelationCmpOp"),
          right: this.node(() => this.value()),
        };
      case "IsNull":
      case "IsNotNull":
        return { kind, column: this.column() };
      case "In":
        return { kind, left: this.column(), values: this.vec(() => this.node(() => this.value())) };
      case "Contains":
        return { kind, left: this.column(), right: this.node(() => this.value()) };
      case "EnumMatch":
        return {
          kind,
          column: this.column(),
          case: this.str(),
          payload: this.nested(() => this.predicate()),
        };
      case "And":
      case "Or":
        return { kind, predicates: this.vec(() => this.nested(() => this.predicate())) };
      case "Not":
        return { kind, predicate: this.nested(() => this.predicate()) };
      default:
        return { kind };
    }
  }
  input(): RelationExpr {
    return this.nested(() => this.expr());
  }
  expr(): RelationExpr {
    const kind = this.variant(EXPR_KINDS, "RelationExpr");
    switch (kind) {
      case "TableScan":
        return { kind, table: this.str(), alias: this.optStr() };
      case "Filter":
        return { kind, input: this.input(), predicate: this.nested(() => this.predicate()) };
      case "Union":
        return { kind, inputs: this.vec(() => ({ label: this.str(), input: this.input() })) };
      case "Join":
        return {
          kind,
          left: this.input(),
          right: this.input(),
          on: this.vec(() => this.node(() => ({ left: this.column(), right: this.column() }))),
          join_kind: this.r.variant(2, "RelationJoinKind") === 0 ? "Inner" : "Left",
        };
      case "Project":
        return {
          kind,
          input: this.input(),
          columns: this.vec(() => this.node(() => ({ alias: this.str(), expr: this.key() }))),
        };
      case "Gather":
        return {
          kind,
          seed: this.input(),
          step: this.input(),
          frontier_key: this.node(() => this.key()),
          bound:
            this.r.variant(2, "RecursionBound") === 0
              ? { kind: "Fixpoint" }
              : { kind: "MaxDepth", depth: this.r.varint() },
          dedupe_key: this.vec(() => this.node(() => this.key())),
        };
      case "Distinct":
        return { kind, input: this.input(), key: this.vec(() => this.node(() => this.key())) };
      case "OrderBy":
        return {
          kind,
          input: this.input(),
          terms: this.vec(() =>
            this.node(() => ({
              column: this.column(),
              direction: this.variant(DIRECTIONS, "OrderDirection"),
            })),
          ),
        };
      case "Offset":
        return { kind, input: this.input(), offset: this.r.varint() };
      case "Limit":
        return { kind, input: this.input(), limit: this.r.varint() };
    }
  }
}

// ---- semantic validation of the wire tree ----

class Validator {
  nodes = 0;
  strings = 0;
  node(): void {
    if (++this.nodes > MAX_RELATION_ITEMS)
      throw new RelationWireError("Invalid", "node count exceeds limit");
  }
  collection(len: number): void {
    if (len > MAX_RELATION_ITEMS)
      throw new RelationWireError("Invalid", "collection exceeds limit");
  }
  text(value: string): void {
    const len = utf8(value).length;
    if (len > MAX_RELATION_STRING_BYTES)
      throw new RelationWireError("Invalid", "string exceeds limit");
    this.strings += len;
    if (this.strings > MAX_RELATION_BYTES) throw new RelationWireError("TooLarge");
  }
  column(c: RelationColumnRef): void {
    if (c.scope !== undefined) this.text(c.scope);
    this.text(c.column);
  }
  key(k: RelationKeyRef): void {
    this.node();
    if (k.kind === "Column") this.column(k.column);
  }
  depth(depth: number): void {
    if (depth > MAX_RELATION_DEPTH) throw new RelationWireError("Invalid", "depth exceeds limit");
  }
  value(v: RelationValueRef, depth: number): void {
    this.node();
    switch (v.kind) {
      case "Literal":
        return this.json(v.value, depth + 1);
      case "Param":
        return this.text(v.name);
      case "SessionRef":
        this.collection(v.path.length);
        return v.path.forEach((s) => this.text(s));
      case "OuterColumn":
      case "FrontierColumn":
        return this.column(v.column);
    }
  }
  json(v: JsonValue, depth: number): void {
    this.depth(depth);
    this.node();
    if (typeof v === "number" && !Number.isFinite(v))
      throw new RelationWireError("NonFiniteNumber");
    if (typeof v === "string") this.text(v);
    else if (Array.isArray(v)) {
      this.collection(v.length);
      for (const x of v) this.json(x, depth + 1);
    } else if (v !== null && typeof v === "object") {
      const entries = Object.entries(v).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
      this.collection(entries.length);
      for (const [k, x] of entries) {
        this.text(k);
        this.json(x, depth + 1);
      }
    }
  }
  predicate(p: RelationPredicate, depth: number): void {
    this.depth(depth);
    this.node();
    switch (p.kind) {
      case "Cmp":
      case "Contains":
        this.column(p.left);
        return this.value(p.right, depth + 1);
      case "IsNull":
      case "IsNotNull":
        return this.column(p.column);
      case "In":
        this.column(p.left);
        this.collection(p.values.length);
        return p.values.forEach((v) => this.value(v, depth + 1));
      case "EnumMatch":
        this.column(p.column);
        this.text(p.case);
        return this.predicate(p.payload, depth + 1);
      case "And":
      case "Or":
        this.collection(p.predicates.length);
        return p.predicates.forEach((c) => this.predicate(c, depth + 1));
      case "Not":
        return this.predicate(p.predicate, depth + 1);
    }
  }
  expr(e: RelationExpr, depth: number): void {
    this.depth(depth);
    this.node();
    switch (e.kind) {
      case "TableScan":
        this.text(e.table);
        if (e.alias !== undefined) this.text(e.alias);
        return;
      case "Filter":
        this.expr(e.input, depth + 1);
        return this.predicate(e.predicate, depth + 1);
      case "Union": {
        this.collection(e.inputs.length);
        const labels = new Set<string>();
        for (const arm of e.inputs) {
          this.text(arm.label);
          if (
            !arm.label ||
            utf8(arm.label).length > 4096 ||
            arm.label.includes("\0") ||
            labels.has(arm.label)
          )
            throw new RelationWireError("Invalid", "invalid union label");
          labels.add(arm.label);
          this.expr(arm.input, depth + 1);
        }
        return;
      }
      case "Join":
        this.expr(e.left, depth + 1);
        this.expr(e.right, depth + 1);
        this.collection(e.on.length);
        for (const c of e.on) {
          this.node();
          this.column(c.left);
          this.column(c.right);
        }
        return;
      case "Project":
        this.expr(e.input, depth + 1);
        this.collection(e.columns.length);
        for (const c of e.columns) {
          this.node();
          this.text(c.alias);
          if (c.expr.kind === "Column") this.column(c.expr.column);
        }
        return;
      case "Gather":
        this.expr(e.seed, depth + 1);
        this.expr(e.step, depth + 1);
        this.key(e.frontier_key);
        this.collection(e.dedupe_key.length);
        return e.dedupe_key.forEach((k) => this.key(k));
      case "Distinct":
        this.expr(e.input, depth + 1);
        this.collection(e.key.length);
        return e.key.forEach((k) => this.key(k));
      case "OrderBy":
        this.expr(e.input, depth + 1);
        this.collection(e.terms.length);
        for (const t of e.terms) {
          this.node();
          this.column(t.column);
        }
        return;
      case "Offset":
      case "Limit":
        return this.expr(e.input, depth + 1);
    }
  }
}

const validateWire = (q: RelationQuery): void => new Validator().expr(q.rel, 0);

/** Writes the typed relation tree (used inside `Query`'s postcard form). */
export function encodeRelationQueryWire(w: Writer, q: RelationQuery): void {
  validateWire(q);
  const inner = new Writer();
  wExpr(inner, q.rel);
  const bytes = inner.finish();
  if (bytes.length > MAX_RELATION_BYTES) throw new RelationWireError("TooLarge");
  w.raw(bytes);
}

/** Reads the typed relation tree under a fresh decode budget. */
export function decodeRelationQueryWire(r: Reader): RelationQuery {
  const budget = new BudgetedReader(r);
  const query = { rel: budget.nested(() => budget.expr()) };
  validateWire(query);
  return query;
}

export function encodeRelationQueryPostcard(q: RelationQuery): Uint8Array {
  const w = new Writer();
  encodeRelationQueryWire(w, q);
  const bytes = w.finish();
  if (bytes.length > MAX_RELATION_BYTES) throw new RelationWireError("TooLarge");
  return bytes;
}

export function decodeRelationQueryPostcard(bytes: Uint8Array): RelationQuery {
  if (bytes.length > MAX_RELATION_BYTES) throw new RelationWireError("TooLarge");
  try {
    return decode(bytes, decodeRelationQueryWire);
  } catch (e) {
    if (e instanceof PostcardError) throw new RelationWireError("Postcard", e.message);
    throw e;
  }
}
