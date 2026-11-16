/**
 * The permissions DSL at run time: policies are small trees that render to
 * `jazz.permissions.json`'s readable form (SPEC §10.10), strings in its
 * expression grammar plus top-level `anyOf`/`allOf` objects. Rendering
 * resolves each condition object against the table it applies to, and each
 * handle against the scope it is used in (`this.col` inside an `exists`).
 * Rust's `artifact::load` is the only checker of the result; what is
 * refused here is what no file could say.
 */
import type { Relationship } from "../relationships.js";
import type { SqlType } from "../schema.js";

// ============================================================================
// Runtime brands
// ============================================================================

export const NODE: unique symbol = Symbol("jazz.permissions.node") as never;
export const HANDLE: unique symbol = Symbol("jazz.permissions.handle") as never;
export const OP: unique symbol = Symbol("jazz.permissions.op") as never;

export interface ColumnMeta {
  readonly sqlType: SqlType;
  readonly nullable: boolean;
}
export interface TableMeta {
  readonly columns: ReadonlyMap<string, ColumnMeta>;
  readonly relations: Readonly<Record<string, Relationship>>;
}
export type SchemaMeta = ReadonlyMap<string, TableMeta>;

/** A row's scope: the checked row of a rule, or the row of an `exists.t((t) => …)`. */
export interface Scope {
  readonly table: string;
  readonly id: number;
}

export type HandleData =
  | { readonly k: "column"; readonly scope: Scope; readonly column: string }
  | { readonly k: "tableColumn"; readonly table: string; readonly column: string }
  | { readonly k: "old"; readonly table: string; readonly column: string }
  | { readonly k: "session"; readonly path: readonly string[] }
  | { readonly k: "text"; readonly text: string }
  | { readonly k: "rel"; readonly scope: Scope; readonly relation: string };

export type CmpOp = "=" | "!=" | "<" | "<=" | ">" | ">=";
export type OpData =
  | { readonly op: "ne" | "gt" | "gte" | "lt" | "lte"; readonly operand: unknown }
  | { readonly op: "contains"; readonly operand: unknown }
  | { readonly op: "in"; readonly operand: unknown }
  | { readonly op: "is"; readonly name: string; readonly operand: unknown };

export type NodeData =
  | { readonly k: "and" | "or"; readonly items: readonly unknown[] }
  | { readonly k: "not"; readonly item: unknown }
  | {
      readonly k: "exists";
      readonly table: string;
      readonly conditions: readonly unknown[];
      readonly hops: readonly {
        readonly relation: string;
        readonly conditions: readonly unknown[];
      }[];
    }
  | {
      readonly k: "relWhere";
      readonly rel: HandleData & { k: "rel" };
      readonly conditions: unknown;
    }
  | {
      readonly k: "allowed";
      readonly op: "read" | "insert" | "update" | "delete";
      readonly target: HandleData;
      readonly maxDepth?: number;
    }
  | {
      readonly k: "compare";
      readonly left: unknown;
      readonly op: OpData["op"] | "eq";
      readonly right: unknown;
    }
  | { readonly k: "existsQuery"; readonly query: { render(ctx: Ctx): string } };

export interface PolicyObject {
  readonly [NODE]: NodeData;
}

export function node(data: NodeData): PolicyObject {
  return Object.freeze({ [NODE]: data });
}
export function handle<T extends HandleData>(data: T): { readonly [HANDLE]: T } {
  return Object.freeze({ [HANDLE]: data });
}
export function op(data: OpData): { readonly [OP]: OpData } {
  return Object.freeze({ [OP]: data });
}

export function isNode(value: unknown): value is PolicyObject {
  return typeof value === "object" && value !== null && NODE in value;
}
export function handleOf(value: unknown): HandleData | undefined {
  if ((typeof value === "object" || typeof value === "function") && value !== null) {
    return (value as { [HANDLE]?: HandleData })[HANDLE];
  }
  return undefined;
}
export function opOf(value: unknown): OpData | undefined {
  if (typeof value === "object" && value !== null && OP in value) {
    return (value as { [OP]: OpData })[OP];
  }
  return undefined;
}

/** A refusal of the permissions DSL (the caller adds the JSON path). */
export class PermissionsDslError extends Error {}

function fail(message: string): never {
  throw new PermissionsDslError(message);
}

// ============================================================================
// Rendering context
// ============================================================================

export interface Ctx {
  readonly meta: SchemaMeta;
  /** The table whose columns condition objects name. */
  readonly table: string;
  /** The scope whose row bare columns name. */
  readonly scope: Scope;
  /** The rule's checked row. */
  readonly root: Scope;
  /** 0 at a rule's top, 1 inside an `exists`, 2 inside an `exists` inside that, … */
  readonly depth: number;
  /** In `updateTo`: `old` columns may be compared. */
  readonly old: boolean;
  /** Inside a relation (`exists(from.t…)`): object keys are column refs. */
  readonly relation?: boolean;
  /** Builds the row handle of a scope, for `exists.t((t) => …)` callbacks. */
  readonly row: (meta: SchemaMeta, scope: Scope) => unknown;
}

let nextScope = 0;
export function newScope(table: string): Scope {
  return { table, id: ++nextScope };
}

// ============================================================================
// Lexical pieces
// ============================================================================

const KEYWORDS = new Set([
  "and",
  "or",
  "not",
  "true",
  "false",
  "null",
  "exists",
  "in",
  "contains",
  "this",
  "outer",
  "frontier",
  "session",
  "as",
  "from",
  "join",
  "left",
  "on",
  "where",
  "select",
  "distinct",
  "order",
  "by",
  "asc",
  "desc",
  "limit",
  "offset",
  "union",
  "recursive",
  "is",
  "via",
  "maxDepth",
  "fixpoint",
  "anyOf",
  "allOf",
  "allowedTo",
  "param",
]);

/** A name in the grammar: an identifier, or backticks around anything else. */
export function name(text: string): string {
  if (/^[A-Za-z_][A-Za-z0-9_]*$/.test(text) && !KEYWORDS.has(text)) return text;
  return "`" + text.replaceAll("`", "``") + "`";
}

/** A column ref `a.b` or `b`, each part a name. */
export function colref(text: string): string {
  const dot = text.indexOf(".");
  if (dot < 0) return name(text);
  return `${name(text.slice(0, dot))}.${name(text.slice(dot + 1))}`;
}

function stringLiteral(text: string): string {
  return `'${text.replaceAll("'", "''")}'`;
}

function numberLiteral(value: number): string {
  if (Number.isNaN(value)) return "double('NaN')";
  if (value === Infinity) return "double('inf')";
  if (value === -Infinity) return "double('-inf')";
  if (Number.isInteger(value)) {
    if (Object.is(value, -0)) return "double(-0.0)";
    return BigInt(value).toString();
  }
  const text = String(value);
  if (!/e/i.test(text)) return text;
  // The grammar has no exponents: write the decimal expansion.
  const fixed = value.toFixed(20).replace(/0+$/, "").replace(/\.$/, ".0");
  return fixed;
}

function hex(bytes: Uint8Array): string {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

/** A JS literal in the grammar. Column types decide the rest (`'…'` for a uuid). */
export function literal(value: unknown): string {
  if (value === null) return "null";
  switch (typeof value) {
    case "string":
      return stringLiteral(value);
    case "number":
      return numberLiteral(value);
    case "boolean":
      return value ? "true" : "false";
    case "bigint":
      return value >= BigInt(Number.MIN_SAFE_INTEGER) && value <= BigInt(Number.MAX_SAFE_INTEGER)
        ? value.toString()
        : `bigint(${value.toString()})`;
  }
  if (value instanceof Date) {
    const ms = value.getTime();
    if (Number.isNaN(ms)) fail("an invalid Date can't be compared");
    return String(ms);
  }
  if (value instanceof Uint8Array) return `bytes('${hex(value)}')`;
  if (Array.isArray(value)) {
    fail("an array here means `in`: use it as a condition's value, `{ column: [a, b] }`");
  }
  fail(`${describe(value)} is not a value a policy can compare with`);
}

function describe(value: unknown): string {
  if (value === undefined) return "undefined";
  if (typeof value === "function") return "a function";
  if (isNode(value)) return "a policy";
  try {
    return JSON.stringify(value) ?? String(value);
  } catch {
    return String(value);
  }
}

// ============================================================================
// Columns and values
// ============================================================================

const MAGIC_COLUMNS = new Set([
  "$createdBy",
  "$createdBy.account",
  "$createdBy.identity",
  "$updatedBy",
  "$updatedBy.account",
  "$updatedBy.identity",
  "$deletedBy",
  "$deletedBy.account",
  "$deletedBy.identity",
  "$createdAt",
  "$updatedAt",
  "$deletedAt",
]);

export function isMagicColumn(column: string): boolean {
  return MAGIC_COLUMNS.has(column);
}

export function tableMeta(meta: SchemaMeta, table: string): TableMeta {
  const found = meta.get(table);
  if (!found) fail(`unknown table "${table}"`);
  return found;
}

/** Whether `column` is one a condition on `table` can name. */
export function isConditionColumn(meta: SchemaMeta, table: string, column: string): boolean {
  return column === "id" || isMagicColumn(column) || tableMeta(meta, table).columns.has(column);
}

function suggestion(candidates: Iterable<string>, wanted: string): string {
  let best: string | undefined;
  let bestDistance = Infinity;
  for (const candidate of candidates) {
    const distance = editDistance(candidate.toLowerCase(), wanted.toLowerCase());
    if (distance < bestDistance) {
      bestDistance = distance;
      best = candidate;
    }
  }
  return best !== undefined && bestDistance <= Math.max(2, wanted.length / 3)
    ? `; did you mean "${best}"?`
    : "";
}

function editDistance(a: string, b: string): number {
  const row = Array.from({ length: b.length + 1 }, (_, i) => i);
  for (let i = 1; i <= a.length; i++) {
    let previous = row[0]!;
    row[0] = i;
    for (let j = 1; j <= b.length; j++) {
      const current = row[j]!;
      row[j] = Math.min(row[j]! + 1, row[j - 1]! + 1, previous + (a[i - 1] === b[j - 1] ? 0 : 1));
      previous = current;
    }
  }
  return row[b.length]!;
}

export function unknownColumn(meta: SchemaMeta, table: string, column: string): never {
  const known = [...tableMeta(meta, table).columns.keys(), "id"];
  fail(`unknown column "${column}" in ${table}${suggestion(known, column)}`);
}

export function unknownMember(meta: SchemaMeta, table: string, member: string): never {
  const t = tableMeta(meta, table);
  const known = [...t.columns.keys(), "id", ...Object.keys(t.relations)];
  fail(`unknown column or relation "${member}" in ${table}${suggestion(known, member)}`);
}

/** The column a condition key names, as written in an expression: magic keys one to one. */
function conditionColumn(key: string): string {
  if (isMagicColumn(key)) return key;
  return name(key);
}

/** A handle or literal as a value in `ctx`. */
export function value(raw: unknown, ctx: Ctx): string {
  const h = handleOf(raw);
  if (!h) {
    if (opOf(raw))
      fail("a comparison helper (ne, gt, …) is a condition's value: `{ column: gt(1) }`");
    if (isNode(raw)) fail("a policy is not a value: compare a column with a value");
    return literal(raw);
  }
  switch (h.k) {
    case "session":
      return ["session", ...h.path.map(name)].join(".");
    case "text":
      return h.text;
    case "old":
      fail(
        "`old` columns can only be compared directly in `updateTo`'s conditions: `{ isPublic: old.isPublic }`",
      );
    case "tableColumn":
      fail(
        `schema.tables.${h.table}.${h.column} is for \`allowedTo.<op>(…)\`; use a row's column (\`row.${h.column}\`) as a value`,
      );
    case "rel":
      fail(
        `relation "${h.relation}" is not a value: use \`.where({…})\` or \`allowedTo.<op>(…)\` on it`,
      );
    case "column":
      return columnValue(h, ctx);
  }
}

function columnValue(h: HandleData & { k: "column" }, ctx: Ctx): string {
  const column = isMagicColumn(h.column) ? h.column : name(h.column);
  if (ctx.relation) {
    if (h.scope.id === ctx.root.id) return `outer.${column}`;
    fail(`${h.scope.table}.${h.column} belongs to another row than this relation's`);
  }
  if (h.scope.id === ctx.root.id) {
    if (ctx.depth === 0)
      fail(
        `${h.scope.table}.${h.column} compared with another column of the same row: the engine has no such policy`,
      );
    if (ctx.depth === 1) return `this.${column}`;
    fail(
      `not supported in policies yet: an \`exists\` inside another \`exists\` can't refer to the checked row's columns (${h.scope.table}.${h.column})`,
    );
  }
  if (h.scope.id === ctx.scope.id)
    fail(
      `${h.scope.table}.${h.column} compared with another column of the same row: the engine has no such policy`,
    );
  fail(
    `${h.scope.table}.${h.column} belongs to an enclosing \`exists\`: a condition names only its own row's columns and the checked row's`,
  );
}

const OP_SYMBOL: Record<string, CmpOp> = {
  eq: "=",
  ne: "!=",
  gt: ">",
  gte: ">=",
  lt: "<",
  lte: "<=",
};
const FLIPPED: Record<CmpOp, CmpOp> = {
  "=": "=",
  "!=": "!=",
  "<": ">",
  "<=": ">=",
  ">": "<",
  ">=": "<=",
};

/** `{ key: raw }` as one expression in `ctx`. */
function condition(key: string, raw: unknown, ctx: Ctx): string {
  if (ctx.relation) return relationCondition(key, raw, ctx);
  if (!isConditionColumn(ctx.meta, ctx.table, key)) unknownColumn(ctx.meta, ctx.table, key);
  if (raw === undefined)
    fail(`${ctx.table}.${key} is undefined: leave the key out, or compare with null`);
  const lhs = conditionColumn(key);
  if (raw === null) return `${lhs} = null`;
  if (Array.isArray(raw)) return `${lhs} in [${raw.map((item) => value(item, ctx)).join(", ")}]`;
  const o = opOf(raw);
  if (o) {
    if (o.op === "contains") return `${lhs} contains ${value(o.operand, ctx)}`;
    if (o.op === "in") {
      const h = handleOf(o.operand);
      if (h?.k !== "session")
        fail(
          "isIn(…) takes a session list, e.g. isIn(session.claims.teams); use an array for literals",
        );
      return `${lhs} in ${value(o.operand, ctx)}`;
    }
    if (o.op === "is") fail("is(…) is for relation predicates: exists(from.t.where({…}))");
    const symbol = OP_SYMBOL[o.op]!;
    if (o.operand === null) {
      if (o.op !== "ne") fail(`${o.op}(null) compares nothing: use \`${key}: null\` or ne(null)`);
      return `${lhs} != null`;
    }
    if (handleOf(o.operand)?.k === "old") return oldComparison(key, symbol, o.operand, ctx);
    return `${lhs} ${symbol} ${value(o.operand, ctx)}`;
  }
  if (handleOf(raw)?.k === "old") return oldComparison(key, "=", raw, ctx);
  return `${lhs} = ${value(raw, ctx)}`;
}

/**
 * `col op old.c` in `updateTo`: the stored row is the old one, so it is
 * `exists t(id = this.id, c <flipped op> this.col)`.
 */
function oldComparison(key: string, symbol: CmpOp, raw: unknown, ctx: Ctx): string {
  return `exists ${name(ctx.table)}(id = this.id, ${oldCondition(key, symbol, raw, ctx)})`;
}

/** `col op old.c` as a condition of the self-lookup: `c <flipped op> this.col`. */
function oldCondition(key: string, symbol: CmpOp, raw: unknown, ctx: Ctx): string {
  const old = handleOf(raw) as HandleData & { k: "old" };
  if (!ctx.old) fail("`old` is only available in `updateTo: (row, old) => …`");
  if (ctx.depth !== 0 || ctx.scope.id !== ctx.root.id || old.table !== ctx.table)
    fail(
      "`old` columns can only be compared directly with the updated row's columns, at the top of `updateTo`",
    );
  const column = isMagicColumn(old.column) ? old.column : name(old.column);
  const self = isMagicColumn(key) ? key : name(key);
  return `${column} ${FLIPPED[symbol]} this.${self}`;
}

/** The `old` comparison `{ key: raw }` makes, if any: its operator and old handle. */
function oldOperand(raw: unknown): { symbol: CmpOp; operand: unknown } | undefined {
  if (handleOf(raw)?.k === "old") return { symbol: "=", operand: raw };
  const o = opOf(raw);
  if (
    o &&
    o.op !== "contains" &&
    o.op !== "in" &&
    o.op !== "is" &&
    handleOf(o.operand)?.k === "old"
  )
    return { symbol: OP_SYMBOL[o.op]!, operand: o.operand };
  return undefined;
}

/**
 * A condition object's expressions in `ctx`, one per key, except that its
 * `old` comparisons share one self-lookup, `exists t(id = this.id, …)`,
 * where the first of them is.
 */
function conditionList(object: Record<string, unknown>, ctx: Ctx): string[] {
  const out: string[] = [];
  const olds: string[] = [];
  let at = -1;
  for (const [key, raw] of Object.entries(object)) {
    const old = ctx.relation ? undefined : oldOperand(raw);
    if (old) {
      if (!isConditionColumn(ctx.meta, ctx.table, key)) unknownColumn(ctx.meta, ctx.table, key);
      if (at < 0) at = out.length;
      olds.push(oldCondition(key, old.symbol, old.operand, ctx));
      continue;
    }
    out.push(condition(key, raw, ctx));
  }
  if (olds.length > 0)
    out.splice(at, 0, `exists ${name(ctx.table)}(id = this.id, ${olds.join(", ")})`);
  return out;
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  if (isNode(value) || handleOf(value) || opOf(value)) return false;
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
}

// ============================================================================
// Policies
// ============================================================================

export type PolicyJson = string | { anyOf: PolicyJson[] } | { allOf: PolicyJson[] };

/** A policy input as an expression string. */
export function text(input: unknown, ctx: Ctx): string {
  if (input === true) return "true";
  if (input === false) return "false";
  if (isPlainObject(input)) {
    const list = conditionList(input, ctx);
    if (list.length === 0) return "true";
    return list.length === 1 ? list[0]! : `allOf(${list.join(", ")})`;
  }
  if (!isNode(input)) fail(`${describe(input)} is not a policy`);
  const data = input[NODE];
  switch (data.k) {
    case "and":
    case "or": {
      // One item stays a one-item node, no items an empty one: the engine
      // keeps both as written (SPEC §10.10).
      const items = data.items.map((item) => text(item, ctx));
      return `${data.k === "and" ? "allOf" : "anyOf"}(${items.join(", ")})`;
    }
    case "not":
      return `not ${text(data.item, ctx)}`;
    case "exists":
      return existsText(data, ctx);
    case "relWhere":
      return relWhereText(data, ctx);
    case "allowed":
      return allowedText(data, ctx);
    case "compare":
      return compareText(data, ctx);
    case "existsQuery":
      return `exists (${data.query.render({ ...ctx, relation: true })})`;
  }
}

/** A policy input as a `jazz.permissions.json` Policy: top-level `anyOf`/`allOf` as objects. */
export function json(input: unknown, ctx: Ctx): PolicyJson {
  if (isPlainObject(input) && !ctx.relation) {
    const list = conditionList(input, ctx);
    if (list.length === 0) return "true";
    return list.length === 1 ? list[0]! : { allOf: list };
  }
  if (isNode(input)) {
    const data = input[NODE];
    if (data.k === "and" || data.k === "or") {
      if (data.items.length === 0) return data.k === "and" ? "allOf()" : "anyOf()";
      const items = data.items.map((item) => json(item, ctx));
      return data.k === "and" ? { allOf: items } : { anyOf: items };
    }
  }
  return text(input, ctx);
}

/** The conditions an `exists` or hop takes, in its inner scope. */
function innerConditions(
  conditions: readonly unknown[],
  inner: Ctx,
  makeRow: (scope: Scope) => unknown,
): string[] {
  const out: string[] = [];
  for (const raw of conditions) {
    const input =
      typeof raw === "function" ? (raw as (row: unknown) => unknown)(makeRow(inner.scope)) : raw;
    if (isPlainObject(input)) out.push(...conditionList(input, inner));
    else if (input === true) continue;
    else out.push(text(input, inner));
  }
  return out;
}

function existsText(data: NodeData & { k: "exists" }, ctx: Ctx): string {
  if (ctx.relation) fail("exists.<table>(…) can't be used inside a relation's predicate");
  tableMeta(ctx.meta, data.table);
  const scope = newScope(data.table);
  const inner: Ctx = { ...ctx, table: data.table, scope, depth: ctx.depth + 1 };
  const parts = [
    `exists ${name(data.table)}(${innerConditions(data.conditions, inner, (s) => ctx.row(ctx.meta, s)).join(", ")})`,
  ];
  let table = data.table;
  for (const hop of data.hops) {
    const relation = tableMeta(ctx.meta, table).relations[hop.relation];
    if (!relation) unknownRelation(ctx.meta, table, hop.relation);
    table = relation.table;
    const hopScope = newScope(table);
    const hopCtx: Ctx = { ...ctx, table, scope: hopScope, depth: ctx.depth + 1 };
    parts.push(
      `.${name(hop.relation)}(${innerConditions(hop.conditions, hopCtx, (s) => ctx.row(ctx.meta, s)).join(", ")})`,
    );
  }
  return parts.join("");
}

export function unknownRelation(meta: SchemaMeta, table: string, relation: string): never {
  fail(
    `unknown relation "${relation}" in ${table}${suggestion(Object.keys(tableMeta(meta, table).relations), relation)}`,
  );
}

/** The column of `source` that its forward relation `relation` follows. */
function forwardColumn(meta: SchemaMeta, source: string, relation: string): string {
  const forward = tableMeta(meta, source).relations[relation];
  if (!forward || forward.kind !== "forward")
    fail(`${source} has no forward relation "${relation}" for a reverse relation to follow`);
  return forward.column;
}

function isArrayColumn(meta: SchemaMeta, table: string, column: string): boolean {
  const sqlType = tableMeta(meta, table).columns.get(column)?.sqlType;
  return typeof sqlType === "object" && sqlType.kind === "ARRAY";
}

/**
 * `row.<rel>.where({…})`: a forward relation is `exists target(id =
 * this.col, …)`, a reverse one `exists source(relCol = this.id, …)`.
 */
function relWhereText(data: NodeData & { k: "relWhere" }, ctx: Ctx): string {
  const rel = data.rel;
  if (ctx.relation)
    fail(`row.${rel.relation}.where(…) can't be used inside a relation's predicate`);
  if (rel.scope.id !== ctx.root.id || ctx.depth !== 0)
    fail(
      `row.${rel.relation}.where(…) inside an \`exists\` would refer to an enclosing row: use exists.<table>(…).hop("${rel.relation}", {…})`,
    );
  const relation = tableMeta(ctx.meta, rel.scope.table).relations[rel.relation];
  if (!relation) unknownRelation(ctx.meta, rel.scope.table, rel.relation);
  const target = relation.table;
  tableMeta(ctx.meta, target);
  let link: string;
  if (relation.kind === "forward") {
    if (isArrayColumn(ctx.meta, rel.scope.table, relation.column))
      fail(
        `row.${rel.relation}.where(…) follows a uuid-array column (${relation.column}), which a policy can't join yet; use allowedTo.<op>(row.${rel.relation})`,
      );
    link = `id = this.${name(relation.column)}`;
  } else {
    const column = forwardColumn(ctx.meta, target, relation.relation);
    if (isArrayColumn(ctx.meta, target, column)) link = `${name(column)} contains this.id`;
    else link = `${name(column)} = this.id`;
  }
  const scope = newScope(target);
  const inner: Ctx = { ...ctx, table: target, scope, depth: ctx.depth + 1 };
  const rest =
    data.conditions === undefined
      ? []
      : innerConditions([data.conditions], inner, (s) => ctx.row(ctx.meta, s));
  return `exists ${name(target)}(${[link, ...rest].join(", ")})`;
}

function allowedText(data: NodeData & { k: "allowed" }, ctx: Ctx): string {
  if (ctx.relation) fail("allowedTo.<op>(…) can't be used inside a relation's predicate");
  const target = data.target;
  let argument: string;
  if (target.k === "rel") {
    if (target.scope.id !== ctx.scope.id)
      fail(
        `allowedTo.${data.op}(row.${target.relation}) names a relation of another row than the one checked here`,
      );
    const relation = tableMeta(ctx.meta, target.scope.table).relations[target.relation];
    if (!relation) unknownRelation(ctx.meta, target.scope.table, target.relation);
    argument = name(target.relation);
    if (relation.kind === "reverse" && data.maxDepth !== undefined)
      fail(
        `allowedTo.${data.op}(${target.relation}): a reverse relation takes no maxDepth (the engine checks every row pointing here)`,
      );
  } else if (target.k === "column") {
    if (target.scope.id !== ctx.scope.id)
      fail(
        `allowedTo.${data.op}(row.${target.column}) names a column of another row than the one checked here`,
      );
    argument = `via ${name(target.column)}`;
  } else if (target.k === "tableColumn") {
    argument = `via ${name(target.table)}.${name(target.column)}`;
    if (data.maxDepth !== undefined)
      fail(`allowedTo.${data.op}(via ${target.table}.${target.column}) takes no maxDepth`);
  } else {
    fail(
      `allowedTo.${data.op}(…) takes a relation (row.chat), a reference column (row.chatId) or schema.tables.<t>.<column>`,
    );
  }
  if (data.maxDepth !== undefined) {
    if (!Number.isInteger(data.maxDepth) || data.maxDepth < 0 || data.maxDepth > 64)
      fail(`maxDepth must be an integer from 0 to 64, not ${data.maxDepth}`);
    return `allowedTo.${data.op}(${argument}, maxDepth: ${data.maxDepth})`;
  }
  return `allowedTo.${data.op}(${argument})`;
}

/** The left side of `eq(a, b)`: a column of the current row, or a session value. */
function leftOperand(raw: unknown, ctx: Ctx): string {
  const h = handleOf(raw);
  if (h?.k === "session") return value(raw, ctx);
  if (h?.k === "column" && h.scope.id === ctx.scope.id)
    return isMagicColumn(h.column) ? h.column : name(h.column);
  if (h?.k === "column") return value(raw, ctx);
  if (typeof raw === "string" && ctx.relation) return colref(raw);
  fail(
    "a comparison's left side is a column of the checked row or a session value: eq(session.claims.role, 'admin')",
  );
}

function compareText(data: NodeData & { k: "compare" }, ctx: Ctx): string {
  const left = leftOperand(data.left, ctx);
  const right = data.right;
  switch (data.op) {
    case "contains":
      return `${left} contains ${operandValue(right, ctx)}`;
    case "in": {
      if (Array.isArray(right))
        return `${left} in [${right.map((v) => operandValue(v, ctx)).join(", ")}]`;
      if (handleOf(right)?.k !== "session") fail("isIn(a, b) takes an array or a session list");
      return `${left} in ${value(right, ctx)}`;
    }
    case "is":
      fail("is(…) is a condition's value: { column: is('case', {…}) }");
  }
  if (right === null) {
    if (data.op === "eq") return `${left} = null`;
    if (data.op === "ne") return `${left} != null`;
    fail(`${data.op}(a, null) compares nothing`);
  }
  return `${left} ${OP_SYMBOL[data.op]} ${operandValue(right, ctx)}`;
}

function operandValue(raw: unknown, ctx: Ctx): string {
  return ctx.relation ? relationValue(raw, ctx) : value(raw, ctx);
}

// ============================================================================
// Relation predicates (inside `exists(from.t …)`)
// ============================================================================

/** A value in a relation predicate: literal, session, `param.x`, `outer.c`, `frontier.c`, `id(…)`, `json('…')`. */
export function relationValue(raw: unknown, ctx: Ctx): string {
  const h = handleOf(raw);
  if (h) return value(raw, ctx);
  return literal(raw);
}

function relationCondition(key: string, raw: unknown, ctx: Ctx): string {
  if (raw === undefined) fail(`${key} is undefined: leave the key out, or compare with null`);
  const lhs = colref(key);
  if (raw === null) return `${lhs} = null`;
  if (Array.isArray(raw))
    return `${lhs} in [${raw.map((item) => relationValue(item, ctx)).join(", ")}]`;
  const o = opOf(raw);
  if (o) {
    if (o.op === "contains") return `${lhs} contains ${relationValue(o.operand, ctx)}`;
    if (o.op === "in") fail("isIn(…) is not available in a relation predicate: use an array");
    if (o.op === "is") return `${lhs} is ${name(o.name)}(${text(o.operand, ctx)})`;
    if (o.operand === null) {
      if (o.op !== "ne") fail(`${o.op}(null) compares nothing`);
      return `${lhs} != null`;
    }
    return `${lhs} ${OP_SYMBOL[o.op]} ${relationValue(o.operand, ctx)}`;
  }
  return `${lhs} = ${relationValue(raw, ctx)}`;
}
