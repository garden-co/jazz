/**
 * The permissions DSL's tools, which `s.defineSchema` hands out bound to
 * its tables: `definePermissions`, `exists`, `allowedTo`, `session`,
 * `anyOf`/`allOf`/`not`, the comparison helpers and the relation IR.
 */
import {
  handle,
  handleOf,
  HANDLE,
  isMagicColumn,
  json,
  literal,
  newScope,
  node,
  op,
  PermissionsDslError,
  tableMeta,
  unknownMember,
  type Ctx,
  type HandleData,
  type PolicyJson,
  type SchemaMeta,
  type Scope,
  type TableMeta,
} from "./compile.js";
import {
  makeFrom,
  union,
  recursive,
  outer,
  frontier,
  param,
  rowId,
  jsonValue,
  type From,
} from "./relation.js";
import {
  READABLE_PERMISSIONS,
  PERMISSIONS_SCHEMA_URL,
  type CheckPermissions,
  type CompareOp,
  type Conditions,
  type ContainsOp,
  type InOp,
  type Permissions,
  type PermissionsInput,
  type AllTables,
  type Policy,
  type PolicyInput,
  type ConditionsIn,
  type ReadablePermissionsFile,
  type RelationTarget,
  type Row,
  type Session,
  type SessionAuthMode,
  type AnyClaims,
  type ValueHandle as Value,
  type Rel,
  type ColumnHandle,
  type Inherits,
  type InheritsIn,
  type NestedPolicy,
  type PermissionError,
  type IsMember,
} from "./types.js";

// ============================================================================
// Rows
// ============================================================================

function relHandle(meta: SchemaMeta, scope: Scope, relation: string): unknown {
  const data: HandleData & { k: "rel" } = { k: "rel", scope, relation };
  return Object.freeze({
    [HANDLE]: data,
    where(conditions?: unknown) {
      return node({ k: "relWhere", rel: data, conditions });
    },
  });
}

/** A row's handles: its columns, `id`, magic columns and relations. Anything else throws. */
function rowHandle(meta: SchemaMeta, scope: Scope): unknown {
  const table = tableMeta(meta, scope.table);
  const cache = new Map<string, unknown>();
  return new Proxy(Object.create(null) as object, {
    get(_target, prop) {
      if (typeof prop === "symbol") return undefined;
      const cached = cache.get(prop);
      if (cached) return cached;
      let made: unknown;
      if (prop === "id" || table.columns.has(prop) || isMagicColumn(prop)) {
        made = handle({ k: "column", scope, column: prop });
      } else if (Object.hasOwn(table.relations, prop)) {
        made = relHandle(meta, scope, prop);
      } else if (prop === "then" || prop === "toJSON") {
        return undefined;
      } else {
        unknownMember(meta, scope.table, prop);
      }
      cache.set(prop, made);
      return made;
    },
    has(_target, prop) {
      return (
        typeof prop === "string" &&
        (table.columns.has(prop) || Object.hasOwn(table.relations, prop))
      );
    },
    ownKeys() {
      return ["id", ...table.columns.keys(), ...Object.keys(table.relations)];
    },
    getOwnPropertyDescriptor(target, prop) {
      if (typeof prop !== "string") return undefined;
      return {
        enumerable: true,
        configurable: true,
        value: (this as ProxyHandler<object>).get!(target, prop, undefined),
      };
    },
  });
}

function oldRow(meta: SchemaMeta, table: string): unknown {
  const t = tableMeta(meta, table);
  return new Proxy(Object.create(null) as object, {
    get(_target, prop) {
      if (typeof prop === "symbol") return undefined;
      if (prop === "id" || t.columns.has(prop) || isMagicColumn(prop))
        return handle({ k: "old", table, column: prop });
      if (prop === "then" || prop === "toJSON") return undefined;
      if (Object.hasOwn(t.relations, prop))
        throw new PermissionsDslError(
          `old.${prop} is a relation: \`old\` only has columns, for direct comparisons`,
        );
      unknownMember(meta, table, prop);
    },
  });
}

// ============================================================================
// Session
// ============================================================================

function sessionPath(path: readonly string[]): unknown {
  const target = handle({ k: "session", path });
  return new Proxy(target, {
    get(t, prop) {
      if (prop === HANDLE) return (t as { [HANDLE]: unknown })[HANDLE];
      if (typeof prop === "symbol" || prop === "then" || prop === "toJSON") return undefined;
      return sessionPath([...path, prop]);
    },
  });
}

function makeSession(): Session<AnyClaims> {
  return Object.freeze({
    user: handle({ k: "session", path: ["user"] }),
    identity: Object.freeze({
      ...handle({ k: "session", path: ["identity"] }),
      subject: handle({ k: "session", path: ["identity", "subject"] }),
      issuer: handle({ k: "session", path: ["identity", "issuer"] }),
    }),
    authMode: handle({ k: "session", path: ["authMode"] }),
    claims: sessionPath(["claims"]),
  }) as unknown as Session<AnyClaims>;
}

// ============================================================================
// Policy helpers
// ============================================================================

type PolicyOf<A extends readonly unknown[]> = Policy<ConditionsIn<A[number]>> &
  InheritsIn<A[number]>;

/** Any of the policies holds. */
export function anyOf<const A extends readonly PolicyInput[]>(...items: A): PolicyOf<A> {
  return node({ k: "or", items }) as unknown as PolicyOf<A>;
}
/** All of the policies hold. */
export function allOf<const A extends readonly PolicyInput[]>(...items: A): PolicyOf<A> {
  return node({ k: "and", items }) as unknown as PolicyOf<A>;
}
/** The policy doesn't hold. Not of `allowedTo`, which is not supported yet. */
export function not<const A extends PolicyInput>(
  item: A & NotOfInherits<A>,
): Policy<ConditionsIn<A>> {
  return node({ k: "not", item }) as unknown as Policy<ConditionsIn<A>>;
}

/** A refusal for `not` of a policy that inherits with `allowedTo`, else `unknown`. */
type NotOfInherits<A> =
  InheritsIn<A> extends Inherits
    ? PermissionError<"`not allowedTo…` is not supported yet: write the rule without negating inheritance">
    : unknown;

type Comparable = Value<any, any> | string | number | boolean | bigint | Date | Uint8Array | null;

/**
 * What a value compared with `left` may be: `session.authMode` only compares
 * with its own values, so the pre-rename `"local-first"` is a type error.
 */
type ComparableWith<L> =
  L extends Value<infer V, any>
    ? string extends V
      ? Comparable
      : [V] extends [SessionAuthMode]
        ? SessionAuthMode | Value<any, any> | null
        : Comparable
    : Comparable;

function comparison(name: "ne" | "gt" | "gte" | "lt" | "lte" | "eq") {
  return (...args: unknown[]): unknown => {
    if (args.length === 1) {
      if (name === "eq") return args[0];
      return op({ op: name, operand: args[0] });
    }
    if (args.length === 2) return node({ k: "compare", left: args[0], op: name, right: args[1] });
    throw new TypeError(`${name}(value) or ${name}(left, right)`);
  };
}

/** One-argument comparison helpers are a condition's value (`{ n: gt(3) }`); two-argument ones a policy (`gt(session.claims.level, 3)`). */
export interface CompareHelper<Op extends string> {
  <const X>(operand: X): CompareOp<X>;
  <L extends Value<any, any>>(left: L, right: ComparableWith<L>): Policy;
  readonly __op?: Op;
}
/** `eq(a, b)`: equal (`=`); `eq(v)` is just `v`. */
export interface EqHelper {
  <const X>(operand: X): X;
  <L extends Value<any, any>>(left: L, right: ComparableWith<L>): Policy;
}

export const eq = /*#__PURE__*/ comparison("eq") as EqHelper;
export const ne = /*#__PURE__*/ comparison("ne") as CompareHelper<"ne">;
export const gt = /*#__PURE__*/ comparison("gt") as CompareHelper<"gt">;
export const gte = /*#__PURE__*/ comparison("gte") as CompareHelper<"gte">;
export const lt = /*#__PURE__*/ comparison("lt") as CompareHelper<"lt">;
export const lte = /*#__PURE__*/ comparison("lte") as CompareHelper<"lte">;

/** `{ tags: contains("x") }`: the column contains the value; `contains(a, b)` as a policy. */
export function contains<const X>(operand: X): ContainsOp<X>;
export function contains(left: Value<any, any>, right: Comparable): Policy;
export function contains(...args: unknown[]): unknown {
  if (args.length === 1) return op({ op: "contains", operand: args[0] });
  return node({ k: "compare", left: args[0], op: "contains", right: args[1] });
}

/** `{ teamId: isIn(session.claims.teams) }`: the column is one of a session list; `isIn(a, list)` as a policy. */
export function isIn(list: Value<any, any>): InOp;
export function isIn<L extends Value<any, any>>(
  left: L,
  list: Value<any, any> | readonly ComparableWith<L>[],
): Policy;
export function isIn(...args: unknown[]): unknown {
  if (args.length === 1) return op({ op: "in", operand: args[0] });
  return node({ k: "compare", left: args[0], op: "in", right: args[1] });
}

/** `{ shape: is("circle", { "c.radius": gt(1) }) }`: in a relation predicate, an enum case test. */
export function is(caseName: string, predicate: PolicyInput) {
  return op({ op: "is", name: caseName, operand: predicate }) as unknown as CompareOp<never>;
}

const TYPED =
  (kind: string) =>
  (...args: unknown[]) => {
    const parts = args.map((arg) => {
      if (arg === null) return "null";
      const h = handleOf(arg);
      if (h?.k === "text") return h.text;
      if (typeof arg === "string") return `'${arg.replaceAll("'", "''")}'`;
      if (typeof arg === "number" || typeof arg === "bigint" || typeof arg === "boolean")
        return String(arg);
      if (arg instanceof Uint8Array)
        return `'${Array.from(arg, (b) => b.toString(16).padStart(2, "0")).join("")}'`;
      throw new PermissionsDslError(`typed.${kind}(…) can't hold ${String(arg)}`);
    });
    if (kind === "double" && typeof args[0] === "number" && Number.isInteger(args[0]))
      parts[0] = `${args[0]}.0`;
    // `row(id, …)`: the id is a uuid string (written `uuid('…')`) or null.
    if (kind === "row" && typeof args[0] === "string") parts[0] = `uuid(${parts[0]})`;
    return handle({ k: "text", text: `${kind}(${parts.join(", ")})` }) as unknown as Value<
      any,
      any
    >;
  };

/** Typed literals, exactly that value whatever they are compared with: `typed.int(5)`, `typed.text("5")`, … */
export const typed = Object.freeze({
  literal: /*#__PURE__*/ TYPED("literal") as (
    value: string | number | boolean | null,
  ) => Value<any, any>,
  int: /*#__PURE__*/ TYPED("int") as (value: number | bigint) => Value<number, any>,
  bigint: /*#__PURE__*/ TYPED("bigint") as (value: number | bigint) => Value<bigint, any>,
  timestamp: /*#__PURE__*/ TYPED("timestamp") as (value: number) => Value<Date, any>,
  double: /*#__PURE__*/ TYPED("double") as (
    value: number | "NaN" | "inf" | "-inf",
  ) => Value<number, any>,
  text: /*#__PURE__*/ TYPED("text") as (value: string) => Value<string, any>,
  uuid: /*#__PURE__*/ TYPED("uuid") as (value: string) => Value<string, any>,
  bytes: /*#__PURE__*/ TYPED("bytes") as (hex: string | Uint8Array) => Value<Uint8Array, any>,
  txid: /*#__PURE__*/ TYPED("txid") as (value: string) => Value<string, any>,
  bool: /*#__PURE__*/ TYPED("bool") as (value: boolean) => Value<boolean, any>,
  array: /*#__PURE__*/ TYPED("array") as (
    ...items: (Value<any, any> | null)[]
  ) => Value<unknown[], any>,
  row: /*#__PURE__*/ TYPED("row") as (
    id: Value<any, any> | null,
    ...fields: (Value<any, any> | null)[]
  ) => Value<unknown, any>,
});

// ============================================================================
// exists and allowedTo
// ============================================================================

/** `exists.<t>(…)`, extended along relations with `.hop(rel, {…})`. */
export interface ExistsPolicy<S, T extends keyof S> extends Policy {
  /** Hop along `T`'s relation `rel` to the rows it leads to, filtered by `conditions`. */
  hop<const R extends keyof Row<S, T> & string>(
    rel: R,
    conditions?: Conditions<S, RelationTarget<S, T, R> & keyof S>,
  ): ExistsPolicy<S, RelationTarget<S, T, R> & keyof S>;
}

/**
 * What `exists.<t>(…)` takes: condition objects and policies, but no
 * `allowedTo` (not supported inside an `exists` yet).
 */
type ExistsCondition<S, T extends keyof S> =
  | Conditions<S, T>
  | NestedPolicy<Conditions<S, T>>
  | boolean
  | ((row: Row<S, T>) => Conditions<S, T> | NestedPolicy | boolean);

/** `exists.<table>(conditions…)`: some row of the table meets them; `exists(query)` for any relation. */
export type Exists<S> = {
  /** Some row the relation yields exists. */
  (query: { render(ctx: Ctx): string }): Policy;
} & {
  readonly [T in keyof S & string]: (...conditions: ExistsCondition<S, T>[]) => ExistsPolicy<S, T>;
};

function existsPolicy(
  table: string,
  conditions: readonly unknown[],
  hops: readonly { relation: string; conditions: readonly unknown[] }[],
): unknown {
  return Object.freeze({
    ...node({ k: "exists", table, conditions, hops }),
    hop(relation: string, hopConditions?: unknown) {
      return existsPolicy(table, conditions, [
        ...hops,
        { relation, conditions: hopConditions === undefined ? [] : [hopConditions] },
      ]);
    },
  });
}

function makeExists<S>(tables: readonly string[]): Exists<S> {
  const exists = (query: { render(ctx: Ctx): string }) => {
    if (!query || typeof query.render !== "function")
      throw new PermissionsDslError(
        "exists(…) takes a query (from.<table>…); use exists.<table>({…}) for a table",
      );
    return node({ k: "existsQuery", query });
  };
  for (const table of tables) {
    Object.defineProperty(exists, table, {
      enumerable: true,
      value: (...conditions: unknown[]) => existsPolicy(table, conditions, []),
    });
  }
  return exists as unknown as Exists<S>;
}

/** What `allowedTo.<op>` inherits through: a relation, a reference column of the row, or `schema.tables.t.col`. */
export type AllowedTarget = Rel<any> | ColumnHandle<string, "uuid" | "array", string>;
/**
 * `allowedTo.<op>(target)`: the rows `target` leads to allow `op`. A rule
 * takes it (alone or in `anyOf` / `allOf`); an `exists` and `not` don't yet:
 * repeat the other table's rule there instead.
 */
export interface AllowedTo {
  /** The rows `target` leads to may be read. */
  read(target: AllowedTarget, options?: { maxDepth?: number }): AllowedPolicy;
  insert(target: AllowedTarget, options?: { maxDepth?: number }): AllowedPolicy;
  update(target: AllowedTarget, options?: { maxDepth?: number }): AllowedPolicy;
  delete(target: AllowedTarget, options?: { maxDepth?: number }): AllowedPolicy;
}

/** What `allowedTo.<op>(…)` gives: a policy for a rule, not for inside an `exists`. */
export type AllowedPolicy = Policy & Inherits;

function makeAllowedTo(): AllowedTo {
  const make =
    (operation: "read" | "insert" | "update" | "delete") =>
    (target: unknown, options?: { maxDepth?: number }) => {
      const data = handleOf(target);
      if (!data || (data.k !== "rel" && data.k !== "column" && data.k !== "tableColumn"))
        throw new PermissionsDslError(
          `allowedTo.${operation}(…) takes a relation (row.chat), a reference column (row.chatId) or schema.tables.<t>.<column>`,
        );
      const at = userFrame(new Error());
      return node({
        k: "allowed",
        op: operation,
        target: data,
        ...(options?.maxDepth === undefined ? {} : { maxDepth: options.maxDepth }),
        ...(at === undefined ? {} : { at }),
      });
    };
  return Object.freeze({
    read: make("read"),
    insert: make("insert"),
    update: make("update"),
    delete: make("delete"),
  }) as unknown as AllowedTo;
}

// ============================================================================
// isMember
// ============================================================================

/** Jazz's members table: one row per account and space it may read (protocol §1.3). */
const MEMBERS_TABLE = "__e2ee_members";
const MEMBERS_META: TableMeta = {
  columns: new Map(
    ["scopeId", "identifier", "accountId"].map((column) => [
      column,
      { sqlType: "UUID", nullable: false },
    ]),
  ),
  relations: {},
};

/**
 * The schema's tables plus the members table, which `isMember` renders an
 * `exists` over. Rules see the members table only through `isMember`.
 */
function withMembersTable(meta: SchemaMeta, spaces: ReadonlySet<string>): SchemaMeta {
  if (spaces.size === 0) return meta;
  return new Map([...meta, [MEMBERS_TABLE, MEMBERS_META]]);
}

/**
 * `isMember(table, id)`: the session's account holds a member row for the
 * space `id` of `table`. It renders exactly the members rule deploy looks
 * for, `exists __e2ee_members(scopeId = table_id('<table>'), identifier =
 * this.<column>, accountId = session.user)`, so a space table's read rule
 * can't be a near miss of it.
 */
function makeIsMember(meta: SchemaMeta, spaces: ReadonlySet<string>): IsMember<any> {
  return ((table: string, id: unknown) => {
    const at = userFrame(new Error());
    const fail = (reason: string): never => {
      throw new PermissionsDslError(`isMember("${table}", …): ${reason}`, at);
    };
    if (!spaces.has(table))
      fail(
        `${table} is not a space table of this schema. A table is one when it declares ` +
          `{ space: "id" } or another table's space relation leads to it.`,
      );
    const column = handleOf(id);
    const usage =
      `the id is the checked row's \`row.id\` (on ${table} itself) or a column of the row ` +
      `whose forward relation leads to ${table}`;
    if (!column || column.k !== "column") return fail(usage);
    const leads =
      column.column === "id"
        ? column.scope.table === table
        : Object.values(meta.get(column.scope.table)?.relations ?? {}).some(
            (relation) =>
              relation.kind === "forward" &&
              relation.column === column.column &&
              relation.table === table,
          );
    if (!leads) fail(`${column.scope.table}.${column.column} doesn't lead to ${table}; ${usage}`);
    return node({
      k: "exists",
      table: MEMBERS_TABLE,
      conditions: [
        {
          scopeId: handle({ k: "text", text: `table_id(${literal(table)})` }),
          identifier: id,
          accountId: handle({ k: "session", path: ["user"] }),
        },
      ],
      hops: [],
    });
  }) as unknown as IsMember<any>;
}

// ============================================================================
// definePermissions
// ============================================================================

const RULE_KEYS = ["read", "insert", "update", "updateIf", "updateTo", "delete"] as const;

/** Where in the user's code an error was thrown: the first stack frame outside jazz-tools. */
function userFrame(error: unknown): string | undefined {
  const stack = error instanceof Error ? error.stack : undefined;
  if (!stack) return undefined;
  for (const line of stack.split("\n").slice(1)) {
    const match = /\(?((?:file:\/\/)?[^\s()]+:\d+:\d+)\)?\s*$/.exec(line.trim());
    if (!match) continue;
    const location = match[1]!;
    if (/[\\/]jazz-tools[\\/](src|dist)[\\/]/.test(location)) continue;
    if (/node:internal|node_modules[\\/]/.test(location)) continue;
    return location.replace(/^file:\/\//, "");
  }
  return undefined;
}

/** A permissions refusal: names the JSON path (and, when known, the TS line). */
export class PermissionsError extends Error {
  readonly code = "JAZZ_PERMISSIONS_DSL";
  constructor(
    readonly path: string,
    readonly reason: string,
    readonly location?: string,
  ) {
    super(`jazz.permissions.json: ${path}: ${reason}${location ? `\n  at ${location}` : ""}`);
  }
}

function compileRule(
  /** The tables a rule can render: the schema's, and the members table when it has spaces. */
  meta: SchemaMeta,
  table: string,
  rule: unknown,
  updateTo: boolean,
): PolicyJson {
  const root = newScope(table);
  const ctx: Ctx = { meta, table, scope: root, root, depth: 0, old: updateTo, row: rowHandle };
  const input =
    typeof rule === "function"
      ? updateTo
        ? (rule as (row: unknown, old: unknown) => unknown)(
            rowHandle(meta, root),
            oldRow(meta, table),
          )
        : (rule as (row: unknown) => unknown)(rowHandle(meta, root))
      : rule;
  if (input === undefined) throw new PermissionsDslError("the rule returned undefined");
  return json(input, ctx);
}

/** Compile the rules of every table to the readable tree. */
export function compilePermissions(
  meta: SchemaMeta,
  rules: Record<string, unknown>,
  callSite?: string,
  spaces: ReadonlySet<string> = new Set(),
): ReadablePermissionsFile {
  const rendered = withMembersTable(meta, spaces);
  const tables: ReadablePermissionsFile["tables"] = {};
  const fault = (path: string, error: unknown): never => {
    if (error instanceof PermissionsError) throw error;
    const reason = error instanceof Error ? error.message : String(error);
    const at = error instanceof PermissionsDslError ? error.location : undefined;
    throw new PermissionsError(path, reason, at ?? userFrame(error) ?? callSite);
  };
  if (!rules || typeof rules !== "object" || Array.isArray(rules))
    fault(
      "tables",
      new Error("definePermissions({ <table>: { read, insert, … } }) takes an object"),
    );
  for (const table of Object.keys(rules)) {
    if (!meta.has(table)) fault(`tables.${table}`, new Error(`unknown table "${table}"`));
  }
  for (const table of [...meta.keys()].sort()) {
    const path = `tables.${table}`;
    if (!Object.hasOwn(rules, table))
      fault(path, new Error(`every table must be listed; use \`${table}: {}\` to deny everything`));
    const tableRules = rules[table] as Record<string, unknown>;
    if (!tableRules || typeof tableRules !== "object" || Array.isArray(tableRules))
      fault(path, new Error("a table's rules are an object: { read, insert, update, delete }"));
    for (const key of Object.keys(tableRules)) {
      if (!(RULE_KEYS as readonly string[]).includes(key))
        fault(
          `${path}.${key}`,
          new Error(
            `unknown rule key "${key}": use read, insert, update, updateIf, updateTo or delete`,
          ),
        );
    }
    const has = (key: string) => tableRules[key] !== undefined;
    if (has("update") && (has("updateIf") || has("updateTo")))
      fault(`${path}.update`, new Error("`update` can't be combined with `updateIf`/`updateTo`"));
    if (has("updateIf") !== has("updateTo"))
      fault(
        `${path}.${has("updateIf") ? "updateIf" : "updateTo"}`,
        new Error(
          "`updateIf` and `updateTo` come together: which rows may be updated, and to what",
        ),
      );
    const out: Record<string, unknown> = {};
    for (const key of ["read", "insert", "update", "delete"] as const) {
      if (key === "update" && has("updateIf")) {
        let old: PolicyJson | undefined;
        let next: PolicyJson | undefined;
        try {
          old = compileRule(rendered, table, tableRules.updateIf, false);
        } catch (error) {
          fault(`${path}.updateIf`, error);
        }
        try {
          next = compileRule(rendered, table, tableRules.updateTo, true);
        } catch (error) {
          fault(`${path}.updateTo`, error);
        }
        out.update = { old, new: next };
        continue;
      }
      if (!has(key)) continue;
      try {
        out[key] = compileRule(rendered, table, tableRules[key], false);
      } catch (error) {
        fault(`${path}.${key}`, error);
      }
    }
    tables[table] = out;
  }
  return { $schema: PERMISSIONS_SCHEMA_URL, tables };
}

/** The readable tree of `definePermissions`' result. */
export function readablePermissions(value: unknown): ReadablePermissionsFile | undefined {
  if (typeof value !== "object" || value === null) return undefined;
  return (value as { [READABLE_PERMISSIONS]?: ReadablePermissionsFile })[READABLE_PERMISSIONS];
}

export interface DefinePermissions<S> {
  /**
   * The app's permissions: every table of the schema, each with its rules
   * (`read`, `insert`, `update` or `updateIf` + `updateTo`, `delete`). A
   * missing operation is denied; `{}` denies everything.
   */
  <const R extends AllTables<S>>(
    rules: PermissionsInput<S, R> & NoInfer<CheckPermissions<S, R>>,
  ): Permissions<S>;
}

function makeDefinePermissions<S>(
  meta: SchemaMeta,
  spaces: ReadonlySet<string>,
): DefinePermissions<S> {
  return ((rules: Record<string, unknown>) => {
    const callSite = userFrame(new Error());
    const readable = compilePermissions(meta, rules, callSite, spaces);
    return Object.freeze({ [READABLE_PERMISSIONS]: readable });
  }) as DefinePermissions<S>;
}

// ============================================================================
// The tools
// ============================================================================

/** The permission tools of a schema (properties of `s.defineSchema`'s result). */
export interface PermissionTools<S, Claims = AnyClaims> {
  definePermissions: DefinePermissions<S>;
  exists: Exists<S>;
  allowedTo: AllowedTo;
  /**
   * `isMember(table, id)`: the session's account is a member of the space
   * `id` of `table`, a space table of the schema. Write a space table's
   * read rule as `isMember("rooms", room.id)`, and a table in its spaces
   * as `isMember("rooms", message.roomId)`.
   */
  isMember: IsMember<S>;
  session: Session<Claims>;
  anyOf: typeof anyOf;
  allOf: typeof allOf;
  not: typeof not;
  eq: typeof eq;
  ne: typeof ne;
  gt: typeof gt;
  gte: typeof gte;
  lt: typeof lt;
  lte: typeof lte;
  contains: typeof contains;
  isIn: typeof isIn;
  typed: typeof typed;
  /** The relation IR: `exists(from.t("a").join(…).where(…).select(…))`. */
  from: From<keyof S & string>;
  union: typeof union;
  recursive: typeof recursive;
  outer: typeof outer;
  frontier: typeof frontier;
  param: typeof param;
  rowId: typeof rowId;
  jsonValue: typeof jsonValue;
  is: typeof is;
}

/**
 * The permission tools of a schema whose tables are `meta`; `spaces` are its
 * space tables (each encryption entry's scope), which `isMember` accepts.
 */
export function permissionTools<S, Claims>(
  meta: SchemaMeta,
  spaces: ReadonlySet<string> = new Set(),
): PermissionTools<S, Claims> {
  const tables = [...meta.keys()];
  return {
    definePermissions: makeDefinePermissions<S>(meta, spaces),
    exists: makeExists<S>(tables),
    allowedTo: makeAllowedTo(),
    isMember: makeIsMember(meta, spaces) as IsMember<S>,
    session: makeSession() as unknown as Session<Claims>,
    anyOf,
    allOf,
    not,
    eq,
    ne,
    gt,
    gte,
    lt,
    lte,
    contains,
    isIn,
    typed,
    from: makeFrom(tables) as From<keyof S & string>,
    union,
    recursive,
    outer,
    frontier,
    param,
    rowId,
    jsonValue,
    is,
  };
}
