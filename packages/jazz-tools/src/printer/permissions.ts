/**
 * `jazz.permissions.json` → the permissions DSL (`definePermissions`), printed
 * idiomatically:
 *
 * - `update {old, new}` → `updateIf` / `updateTo`;
 * - `allowedTo.op(rel)` → `allowedTo.op(row.rel)`;
 * - `exists t(id = this.col, …)` along a forward relation → `row.rel.where({…})`;
 * - a self-`exists(id = this.id, col = this.col)` in `updateTo` → `{ col: old.col }`;
 * - `in` lists → arrays;
 * - a repeated `exists` (or condition object) → a `const` helper, with a
 *   parameter for each value it takes from the checked row.
 *
 * What the DSL can't spell is reported in `unsupported` (with its JSON path)
 * rather than printed, so callers can refuse.
 */
import {
  columnKind,
  type AppJson,
  type PermissionsJson,
  type PolicyJson,
  type TableJson,
  type TablePoliciesJson,
} from "./app-json.js";
import {
  parseExpr,
  type Ast,
  type CmpOp,
  type ColRef,
  type Key,
  type Operand,
  type Pred,
  type Rel,
  type Typed,
  type ValueRef,
} from "./expr.js";
import { camel, fresh, isBindable, jsLiteral, member, pascal, propKey, singular } from "./names.js";

export interface Unsupported {
  /** JSON path, e.g. `tables.todos.read.anyOf[1]`. */
  path: string;
  /** What couldn't be printed. */
  text: string;
  reason: string;
}

export interface PrintedPermissions {
  /** The destructuring of the tools, the helpers and `export const permissions = …`. */
  code: string;
  unsupported: Unsupported[];
}

export interface PrintPermissionsOptions {
  /** The name `schema.ts` binds `s.defineSchema(…)` to. Default `schema`. */
  schemaName?: string;
  /** Spread before the tables, e.g. `betterAuth.permissions`. */
  spreads?: string[];
  /** Tables the spreads cover, which are not printed. */
  skipTables?: Iterable<string>;
}

const FLIP: Record<CmpOp, CmpOp> = {
  "=": "=",
  "!=": "!=",
  "<": ">",
  "<=": ">=",
  ">": "<",
  ">=": "<=",
};
const OP_FN: Record<Exclude<CmpOp, "=">, string> = {
  "!=": "ne",
  "<": "lt",
  "<=": "lte",
  ">": "gt",
  ">=": "gte",
};
const PLACEHOLDER = (i: number) => `\uE000${i}\uE000`;
const PLACEHOLDER_RE = /\uE000(\d+)\uE000/g;

class Unprintable extends Error {
  constructor(
    readonly text: string,
    readonly reason: string,
  ) {
    super(reason);
  }
}

/** An exists (or constant condition object) seen while printing: a helper candidate. */
interface Shape {
  key: string;
  /** Body with placeholders for the row values. */
  body: string;
  /** Per placeholder: the key it is the value of (to name the parameter), and the column type. */
  params: { name?: string; kind?: string }[];
  table?: string;
  /** For condition constants: the single key and value, to name it. */
  hint: string;
}

interface Helper {
  name: string;
  shape: Shape;
}

interface RuleCtx {
  table: string;
  row: string;
  /** Inside `updateTo`: `old` may be used. */
  allowOld: boolean;
  usesRow: boolean;
  usesOld: boolean;
}

/** Where bare names point while printing conditions. */
interface Scope {
  table: string;
  alias?: string;
  /** The checked row, when `this.col` is a value here (inside `exists`). */
  outer?: string;
  /** Inside a nested `exists`: the checked row may not be referenced. */
  nested?: boolean;
}

/** A value with the row values it takes, as placeholders. */
interface Valued {
  text: string;
  args: string[];
  /** The text through the helper table, for a constant condition object. */
  constant?: string;
  /** Further policies scoped to the same table, after the object. */
  extras?: string[];
}

export function printPermissions(
  app: AppJson,
  permissions: PermissionsJson,
  options: PrintPermissionsOptions = {},
): PrintedPermissions {
  const schemaName = options.schemaName ?? "schema";
  const skip = new Set(options.skipTables ?? []);
  // Pass 1 counts shapes, pass 2 prints with helpers for the repeated ones.
  const counts = new Map<string, { shape: Shape; count: number; order: number }>();
  const run = (helpers: Map<string, Helper> | undefined) =>
    new Printer(app, permissions, schemaName, helpers, counts).print(skip);
  run(undefined);

  const taken = new Set<string>([
    schemaName,
    "s",
    "permissions",
    "definePermissions",
    "old",
    ...TOOL_ORDER,
    ...Object.keys(app.tables).map((t) => rowName(t)),
  ]);
  const helpers = new Map<string, Helper>();
  const repeated = [...counts.values()]
    .filter((c) => c.count >= 2)
    .sort((a, b) => a.order - b.order);
  const byTable = new Map<string, number>();
  for (const { shape } of repeated) {
    if (shape.table) byTable.set(shape.table, (byTable.get(shape.table) ?? 0) + 1);
  }
  for (const { shape } of repeated) {
    helpers.set(shape.key, { name: fresh(helperName(shape, byTable), taken), shape });
  }
  const printer = new Printer(app, permissions, schemaName, helpers, counts);
  const body = printer.print(skip);

  const lines: string[] = [];
  const tools = ["definePermissions", ...TOOL_ORDER.filter((t) => printer.tools.has(t))];
  lines.push(`const { ${tools.join(", ")} } = ${schemaName};`, "");
  for (const helper of helpers.values()) lines.push(printHelper(helper), "");
  const spreads = (options.spreads ?? []).map((s) => `...${s},`);
  lines.push(`export const permissions = definePermissions({`, ...spreads, body, `});`);
  return { code: lines.join("\n") + "\n", unsupported: printer.unsupported };
}

const TOOL_ORDER = [
  "exists",
  "allowedTo",
  "session",
  "anyOf",
  "allOf",
  "not",
  "eq",
  "ne",
  "gt",
  "gte",
  "lt",
  "lte",
  "contains",
  "isIn",
  "typed",
  "from",
  "union",
  "recursive",
  "outer",
  "frontier",
  "rowId",
  "param",
  "jsonValue",
];

function rowName(table: string): string {
  const name = singular(table);
  return isBindable(name) && !TOOL_ORDER.includes(name) && name !== "old" ? name : "row";
}

function helperName(shape: Shape, byTable: Map<string, number>): string {
  if (!shape.table) return shape.hint;
  const base = `is${pascal(singular(shape.table))}`;
  if ((byTable.get(shape.table) ?? 0) <= 1 || !shape.hint) return base;
  return base + pascal(shape.hint);
}

function printHelper({ name, shape }: Helper): string {
  if (shape.params.length === 0) return `const ${name} = ${shape.body};`;
  const taken = new Set<string>();
  const names = shape.params.map((p) =>
    fresh(p.name && isBindable(p.name) ? p.name : "value", taken),
  );
  const types = shape.params.map((p) =>
    p.kind === "uuid" ? "s.Uuid" : `s.Value<${JSON.stringify(p.kind ?? "string")}>`,
  );
  let body = shape.body;
  // `{ chatId: chatId }` → `{ chatId }`
  body = body.replace(
    /([A-Za-z_$][A-Za-z0-9_$]*): \uE000(\d+)\uE000/g,
    (all, key: string, i: string) => (names[Number(i)] === key ? key : all),
  );
  body = body.replace(PLACEHOLDER_RE, (_, i: string) => names[Number(i)]!);
  const params = names.map((n, i) => `${n}: ${types[i]}`).join(", ");
  return `const ${name} = (${params}) => ${body};`;
}

class Printer {
  readonly tools = new Set<string>();
  readonly unsupported: Unsupported[] = [];
  private order = 0;

  constructor(
    readonly app: AppJson,
    readonly permissions: PermissionsJson,
    readonly schemaName: string,
    readonly helpers: Map<string, Helper> | undefined,
    readonly counts: Map<string, { shape: Shape; count: number; order: number }>,
  ) {}

  print(skip: Set<string>): string {
    const out: string[] = [];
    for (const table of Object.keys(this.app.tables).sort()) {
      if (skip.has(table)) continue;
      const rules = this.permissions.tables[table] ?? {};
      out.push(this.table(table, rules));
    }
    return out.join("\n");
  }

  private table(table: string, rules: TablePoliciesJson): string {
    const entries: string[] = [];
    const path = `tables.${table}`;
    const rule = (op: string, policy: PolicyJson | undefined, allowOld = false) => {
      if (policy === undefined) return;
      entries.push(`${op}: ${this.rule(table, policy, `${path}.${op}`, allowOld)},`);
    };
    rule("read", rules.read);
    rule("insert", rules.insert);
    const update = rules.update;
    if (update !== undefined && typeof update === "object" && "old" in update && "new" in update) {
      rule("updateIf", update.old);
      rule("updateTo", update.new, true);
    } else {
      rule("update", update as PolicyJson | undefined);
    }
    rule("delete", rules.delete);
    const comment = rules.$comment ? lineComment(rules.$comment) : "";
    if (entries.length === 0) return `${comment}${propKey(table)}: {},`;
    return `${comment}${propKey(table)}: {\n${entries.join("\n")}\n},`;
  }

  private rule(table: string, policy: PolicyJson, path: string, allowOld: boolean): string {
    const ctx: RuleCtx = { table, row: rowName(table), allowOld, usesRow: false, usesOld: false };
    let text: string;
    try {
      text = this.policy(policy, ctx, path);
    } catch (error) {
      if (!(error instanceof Unprintable)) throw error;
      this.unsupported.push({ path, text: error.text, reason: error.reason });
      return "false";
    }
    const body = text.startsWith("{") ? `(${text})` : text;
    if (ctx.usesOld) return `(${ctx.row}, old) => ${body}`;
    if (ctx.usesRow) return `(${ctx.row}) => ${body}`;
    return text;
  }

  private policy(policy: PolicyJson, ctx: RuleCtx, path: string): string {
    if (typeof policy === "string") {
      let ast: Ast;
      try {
        ast = parseExpr(policy);
      } catch (error) {
        throw new Unprintable(policy, (error as Error).message);
      }
      try {
        return this.expr(this.inlineDefs(ast, path), ctx, { table: ctx.table });
      } catch (error) {
        if (error instanceof Unprintable && error.text === "")
          throw new Unprintable(policy, error.reason);
        throw error;
      }
    }
    if ("anyOf" in policy) {
      return this.call(
        "anyOf",
        policy.anyOf.map((p, i) => this.policy(p, ctx, `${path}.anyOf[${i}]`)),
      );
    }
    if ("allOf" in policy) {
      const items = policy.allOf.map((p) => (typeof p === "string" ? safeParse(p) : undefined));
      const merged = this.mergeConditions(items, ctx, { table: ctx.table });
      if (merged) return merged;
      return this.call(
        "allOf",
        policy.allOf.map((p, i) => this.policy(p, ctx, `${path}.allOf[${i}]`)),
      );
    }
    if ("where" in policy) {
      return `${lineComment(policy.$comment)}${this.policy(policy.where, ctx, `${path}.where`)}`;
    }
    throw new Unprintable(
      JSON.stringify(policy),
      "a policy with both `using` and `withCheck` has no DSL form",
    );
  }

  private call(fn: string, args: string[]): string {
    this.tools.add(fn);
    return `${fn}(${args.join(", ")})`;
  }

  private inlineDefs(ast: Ast, path: string): Ast {
    const defs = this.permissions.defs;
    if (!defs || !JSON.stringify(ast).includes('"call"')) return ast;
    void path;
    throw new Unprintable("", "`defs` are not part of the alpha.66 format; inline them first");
  }

  /**
   * `a = 1 and b = 2` → `{ a: 1, b: 2 }`, when every item is a plain
   * condition on a distinct column of the scope.
   */
  private mergeConditions(
    items: (Ast | undefined)[],
    ctx: RuleCtx,
    scope: Scope,
  ): string | undefined {
    if (items.length < 2 || items.some((i) => i === undefined)) return undefined;
    const entries: [string, Valued][] = [];
    for (const item of items) {
      const cond = this.condition(item!, ctx, scope);
      if (!cond || entries.some(([k]) => k === cond[0])) return undefined;
      entries.push(cond);
    }
    return this.conditionObject(entries, ctx, scope);
  }

  /** Print a condition object, as a helper constant when it repeats at the top level. */
  private conditionObject(entries: [string, Valued][], ctx: RuleCtx, scope: Scope): string {
    const args: string[] = [];
    const text = objectText(entries.map(([k, v]) => [k, shift(v, args)]));
    if (scope.outer || args.length > 0) return fill(text, args);
    // Only conditions on the session repeat meaningfully: `const mine = { userId: session.user }`.
    if (entries.length !== 1 || !entries[0]![1].text.startsWith("session.")) return text;
    const [key, value] = entries[0]!;
    const hint =
      value.text === "session.user"
        ? key.startsWith("$createdBy")
          ? "createdByMe"
          : key === "userId"
            ? "mine"
            : `${camel(key.replace(/Id$/, ""))}IsMe`
        : `${camel(key.replace(/^\$/, ""))}FromSession`;
    return this.shaped({ key: `const:${text}`, body: text, params: [], hint }, []);
  }

  /** Count (pass 1) or use (pass 2) a helper for `shape`. */
  private shaped(shape: Shape, args: string[]): string {
    if (!this.helpers) {
      const seen = this.counts.get(shape.key);
      if (seen) seen.count++;
      else this.counts.set(shape.key, { shape, count: 1, order: this.order++ });
      return fill(shape.body, args);
    }
    const helper = this.helpers.get(shape.key);
    if (!helper) return fill(shape.body, args);
    return shape.params.length === 0 ? helper.name : `${helper.name}(${args.join(", ")})`;
  }

  private expr(ast: Ast, ctx: RuleCtx, scope: Scope): string {
    switch (ast.kind) {
      case "bool":
        return String(ast.value);
      case "or":
        return this.call(
          "anyOf",
          ast.items.map((i) => this.expr(i, ctx, scope)),
        );
      case "and": {
        const merged = this.mergeConditions(ast.items, ctx, scope);
        if (merged) return merged;
        return this.call(
          "allOf",
          ast.items.map((i) => this.expr(i, ctx, scope)),
        );
      }
      case "not":
        return this.call("not", [this.expr(ast.inner, ctx, scope)]);
      case "exists":
        return this.exists(ast, ctx, scope);
      case "existsRel":
        this.tools.add("exists");
        return `exists(${this.relation(ast.relation)})`;
      case "allowed": {
        if (scope.outer) throw new Unprintable("", "`allowedTo` inside `exists`");
        this.tools.add("allowedTo");
        ctx.usesRow = true;
        let target: string;
        if (ast.target.kind === "relation") target = member(ctx.row, ast.target.name);
        else if (ast.target.table === undefined) target = member(ctx.row, ast.target.column);
        else
          target = member(member(`${this.schemaName}.tables`, ast.target.table), ast.target.column);
        const depth = ast.maxDepth !== undefined ? `, { maxDepth: ${ast.maxDepth} }` : "";
        return `allowedTo.${ast.op}(${target}${depth})`;
      }
      case "call":
        throw new Unprintable("", "`defs` calls are not part of the alpha.66 format");
      default: {
        const cond = this.condition(ast, ctx, scope);
        if (!cond) {
          const session = this.sessionComparison(ast, ctx, scope);
          if (session) return session;
        }
        if (!cond) throw new Unprintable("", "a comparison the DSL has no condition for");
        return this.conditionObject([cond], ctx, scope);
      }
    }
  }

  /** `exists t(…)`, idiomatically. */
  private exists(ast: Extract<Ast, { kind: "exists" }>, ctx: RuleCtx, scope: Scope): string {
    if (scope.outer) {
      // An `exists` inside another: its conditions name only its own rows.
      return this.existsGeneric(ast, ctx, {
        table: ast.table,
        alias: ast.alias,
        outer: scope.outer,
        nested: true,
      });
    }
    const inner: Scope = { table: ast.table, alias: ast.alias, outer: ctx.row };
    const thisCol = (c: Ast | undefined, col: string, op: CmpOp = "="): string | undefined => {
      if (c?.kind !== "cmp" || c.op !== op) return undefined;
      if (c.left.kind !== "column" || !this.isInnerColumn(c.left, inner) || c.left.name !== col)
        return undefined;
      return this.outerColumn(c.right, inner);
    };
    // `{ col: old.col }`: the old row is the same table's row with this id.
    if (
      ctx.allowOld &&
      ast.table === ctx.table &&
      ast.hops.length === 0 &&
      ast.conditions.length === 2 &&
      thisCol(ast.conditions[0], "id") === "id"
    ) {
      const c = ast.conditions[1]!;
      if (
        c.kind === "cmp" &&
        c.left.kind === "column" &&
        this.isInnerColumn(c.left, inner) &&
        this.outerColumn(c.right, inner) === c.left.name
      ) {
        ctx.usesOld = true;
        const value = member("old", c.left.name);
        const op = FLIP[c.op];
        return objectText([
          [conditionKey(c.left.name), op === "=" ? value : this.fn(OP_FN[op], value)],
        ]);
      }
    }
    // `row.rel.where({…})` along a forward relation.
    if (ast.hops.length === 0) {
      const idIndex = ast.conditions.findIndex((c) => thisCol(c, "id") !== undefined);
      if (idIndex >= 0) {
        const column = thisCol(ast.conditions[idIndex], "id")!;
        const relations = this.app.tables[ctx.table]?.relations ?? {};
        const rel = Object.entries(relations).find(
          ([, r]) => "rel" in r && r.rel === ast.table && r.column === column,
        );
        if (rel) {
          const rest = ast.conditions.filter((_, i) => i !== idIndex);
          const conds = this.conditions(rest, ctx, inner);
          if (conds !== undefined && !conds.extras?.length) {
            ctx.usesRow = true;
            const arg = conds.text === "" ? "{}" : (conds.constant ?? conds.text);
            return `${member(ctx.row, rel[0])}.where(${arg})`;
          }
        }
      }
    }
    return this.existsGeneric(ast, ctx, inner);
  }

  private existsGeneric(ast: Extract<Ast, { kind: "exists" }>, ctx: RuleCtx, inner: Scope): string {
    this.tools.add("exists");
    const head = this.conditions(ast.conditions, ctx, inner, true);
    if (!head)
      throw new Unprintable("", "an `exists` whose conditions aren't plain column conditions");
    let body = `${member("exists", ast.table)}(${[head.text || "{}", ...(head.extras ?? [])].join(", ")})`;
    const args = [...head.args];
    let table = ast.table;
    for (const hop of ast.hops) {
      const target = this.relationTarget(table, hop.relation);
      if (!target) throw new Unprintable("", `unknown relation \`${hop.relation}\` of ${table}`);
      const conds = this.conditions(
        hop.conditions,
        ctx,
        { table: target, outer: inner.outer, nested: inner.nested },
        true,
        args.length,
      );
      if (!conds)
        throw new Unprintable("", "a hop whose conditions aren't plain column conditions");
      args.push(...conds.args);
      const hopArgs = [
        conds.text || (conds.extras?.length ? "{}" : ""),
        ...(conds.extras ?? []),
      ].filter(Boolean);
      body += `.hop(${[JSON.stringify(hop.relation), ...hopArgs].join(", ")})`;
      table = target;
    }
    if (inner.nested) return fill(body, args);
    const kinds = this.paramKinds(body, args.length, ast.table);
    return this.shaped(
      { key: `exists:${body}`, body, params: kinds, table: ast.table, hint: hintOf(body) },
      args,
    );
  }

  private paramKinds(body: string, n: number, table: string): { name?: string; kind?: string }[] {
    const params: { name?: string; kind?: string }[] = Array.from({ length: n }, () => ({}));
    for (const m of body.matchAll(
      /([A-Za-z_$][A-Za-z0-9_$]*|"[^"]*"): (?:[a-z]+\()?\uE000(\d+)\uE000/g,
    )) {
      const key = m[1]!.replace(/^"|"$/g, "");
      params[Number(m[2])] = { name: key, kind: this.kindOfName(key, table) };
    }
    return params;
  }

  private kindOfName(column: string, table: string): string | undefined {
    const own = this.kindOf(table, column);
    if (own) return own;
    for (const table of Object.values(this.app.tables)) {
      const kind = columnKind(table.columns[column]);
      if (kind) return kind;
    }
    return column === "id" ? "uuid" : undefined;
  }

  private relationTarget(table: string, relation: string): string | undefined {
    const r = this.app.tables[table]?.relations?.[relation];
    if (!r) return undefined;
    return "rel" in r ? r.rel : r.reverse;
  }

  /**
   * Conditions inside `exists` (or a hop): one object of plain conditions.
   * With `placeholders`, values from the checked row become placeholders.
   */
  private conditions(
    items: Ast[],
    ctx: RuleCtx,
    scope: Scope,
    placeholders = false,
    argBase = 0,
  ): Valued | undefined {
    const flat: Ast[] = [];
    for (const item of items) {
      if (item.kind === "and") flat.push(...item.items);
      else flat.push(item);
    }
    const entries: [string, Valued][] = [];
    const extraItems: Ast[] = [];
    for (const item of flat) {
      const cond = this.condition(item, ctx, scope);
      if (!cond || entries.some(([k]) => k === cond[0])) extraItems.push(item);
      else entries.push(cond);
    }
    // Conditions that aren't `{ col: value }`, scoped to the table: `exists.t({ a: 1 }, anyOf(…))`.
    const extras = extraItems.map((item) => this.expr(item, ctx, scope));
    if (entries.length === 0) return { text: "", args: [], extras };
    if (!placeholders && extras.length === 0 && entries.every(([, v]) => v.args.length === 0)) {
      // A constant object, which may repeat elsewhere: `msg.sender.where(mine)`.
      return {
        text: objectText(entries.map(([k, v]) => [k, v.text])),
        args: [],
        constant: this.conditionObject(entries, ctx, { table: scope.table }),
      };
    }
    const args: string[] = [];
    const text = objectText(
      entries.map(([k, v]) => {
        let t = v.text;
        if (placeholders) {
          t = t.replace(PLACEHOLDER_RE, (_, i: string) => {
            args.push(v.args[Number(i)]!);
            return PLACEHOLDER(argBase + args.length - 1);
          });
        } else {
          t = fill(t, v.args);
        }
        return [k, t];
      }),
    );
    return { text, args, extras };
  }

  private isInnerColumn(op: Operand, scope: Scope): boolean {
    if (op.kind !== "column") return false;
    if (op.qual.kind === "none") return true;
    if (op.qual.kind === "named")
      return op.qual.name === (scope.alias ?? scope.table) || op.qual.name === scope.table;
    if (!scope.outer) return op.qual.kind === "this";
    return false;
  }

  /** The checked row's column a value names, inside `exists`. */
  private outerColumn(op: Operand, scope: Scope): string | undefined {
    if (!scope.outer || op.kind !== "column" || this.isInnerColumn(op, scope)) return undefined;
    return op.name;
  }

  /** A plain condition `[key, value]` on a column of the scope. */
  private condition(ast: Ast, ctx: RuleCtx, scope: Scope): [string, Valued] | undefined {
    const column = (op: Operand) =>
      op.kind === "column" && this.isInnerColumn(op, scope) ? op.name : undefined;
    switch (ast.kind) {
      case "cmp": {
        let left = column(ast.left);
        let right = ast.right;
        let op = ast.op;
        if (left === undefined) {
          const flipped = column(ast.right);
          if (flipped === undefined) return undefined;
          left = flipped;
          right = ast.left;
          op = FLIP[op];
        }
        const value = this.value(right, ctx, scope, this.kindOf(scope.table, left));
        if (!value) return undefined;
        if (right.kind === "null") {
          if (op === "=") return [conditionKey(left), value];
          if (op === "!=")
            return [conditionKey(left), { text: this.fn("ne", value.text), args: value.args }];
          return undefined;
        }
        if (op === "=") return [conditionKey(left), value];
        return [conditionKey(left), { text: this.fn(OP_FN[op], value.text), args: value.args }];
      }
      case "inList": {
        const left = column(ast.left);
        if (left === undefined) return undefined;
        const kind = this.kindOf(scope.table, left);
        const values = ast.values.map((v) => this.value(v, ctx, scope, kind));
        if (values.some((v) => !v)) return undefined;
        const args: string[] = [];
        const text = values.map((v) => shift(v!, args)).join(", ");
        return [conditionKey(left), { text: `[${text}]`, args }];
      }
      case "inSession": {
        const left = column(ast.left);
        if (left === undefined) return undefined;
        return [
          conditionKey(left),
          { text: this.fn("isIn", this.sessionText(ast.path)), args: [] },
        ];
      }
      case "contains": {
        const left = column(ast.left);
        if (left === undefined) return undefined;
        const value = this.value(ast.right, ctx, scope, undefined);
        if (!value) return undefined;
        return [conditionKey(left), { text: this.fn("contains", value.text), args: value.args }];
      }
      default:
        return undefined;
    }
  }

  /** The relation IR: `from.files("f").join(…).where(…)`. */
  relation(rel: Rel): string {
    switch (rel.kind) {
      case "scan":
        this.tools.add("from");
        return `${member("from", rel.table)}(${rel.alias !== undefined ? JSON.stringify(rel.alias) : ""})`;
      case "join": {
        const opts: string[] = [];
        if (rel.on.length) {
          opts.push(
            `on: [${rel.on.map((c) => `[${colRefText(c.left)}, ${colRefText(c.right)}]`).join(", ")}]`,
          );
        }
        if (rel.joinKind === "left") opts.push("left: true");
        const right = this.relation(rel.right);
        return `${this.relation(rel.left)}.join(${right}${opts.length ? `, { ${opts.join(", ")} }` : ""})`;
      }
      case "filter":
        return `${this.relation(rel.input)}.where(${this.predicate(rel.predicate)})`;
      case "project": {
        const cols = rel.columns.map((c) => `${propKey(c.alias)}: ${this.keyText(c.expr)}`);
        return `${this.relation(rel.input)}.select({${cols.length ? ` ${cols.join(", ")} ` : ""}})`;
      }
      case "distinct":
        return `${this.relation(rel.input)}.distinctOn(${rel.key.map((k) => this.keyText(k)).join(", ")})`;
      case "orderBy": {
        const terms = rel.terms.map((t) =>
          t.direction === "desc" ? `[${colRefText(t.column)}, "desc"]` : colRefText(t.column),
        );
        return `${this.relation(rel.input)}.orderBy(${terms.join(", ")})`;
      }
      case "limit":
        return `${this.relation(rel.input)}.limit(${rel.limit})`;
      case "offset":
        return `${this.relation(rel.input)}.offset(${rel.offset})`;
      // `union` and `recursive` are sources: a query starts from them with `from(…)`.
      case "union": {
        this.tools.add("union");
        this.tools.add("from");
        const arms = rel.inputs.map((a) => `${propKey(a.label)}: ${this.relation(a.input)}`);
        return `from(union({ ${arms.join(", ")} }))`;
      }
      case "gather": {
        this.tools.add("recursive");
        const depth = rel.maxDepth === "fixpoint" ? '"fixpoint"' : String(rel.maxDepth);
        this.tools.add("from");
        return `from(recursive({ seed: ${this.relation(rel.seed)}, step: ${this.relation(rel.step)}, frontier: ${this.keyText(rel.frontierKey)}, dedupe: [${rel.dedupeKey.map((k) => this.keyText(k)).join(", ")}], maxDepth: ${depth} }))`;
      }
    }
  }

  private keyText(key: Key): string {
    if (key.kind === "rowId") return this.fn("rowId", JSON.stringify(key.row));
    return colRefText(key.column);
  }

  /** A relation predicate: condition objects keyed by `"scope.col"`, and the combinators. */
  private predicate(p: Pred): string {
    switch (p.kind) {
      case "true":
      case "false":
        return p.kind;
      case "or":
        return this.call(
          "anyOf",
          p.items.map((i) => this.predicate(i)),
        );
      case "and": {
        const entries = p.items.map((i) => this.predEntry(i));
        if (
          entries.length > 1 &&
          entries.every((e) => e) &&
          new Set(entries.map((e) => e![0])).size === entries.length
        ) {
          return objectText(entries as [string, string][]);
        }
        return this.call(
          "allOf",
          p.items.map((i) => this.predicate(i)),
        );
      }
      case "not":
        return this.call("not", [this.predicate(p.inner)]);
      default:
        return objectText([this.predEntry(p)!]);
    }
  }

  private predEntry(p: Pred): [string, string] | undefined {
    switch (p.kind) {
      case "cmp": {
        const v = this.valueRef(p.right);
        return [colRefKey(p.left), p.op === "=" ? v : this.fn(OP_FN[p.op], v)];
      }
      case "isNull":
        return [colRefKey(p.column), "null"];
      case "isNotNull":
        return [colRefKey(p.column), this.fn("ne", "null")];
      case "in":
        return [colRefKey(p.left), `[${p.values.map((v) => this.valueRef(v)).join(", ")}]`];
      case "contains":
        return [colRefKey(p.left), this.fn("contains", this.valueRef(p.right))];
      default:
        return undefined;
    }
  }

  private valueRef(v: ValueRef): string {
    switch (v.kind) {
      case "literal":
        if (
          v.value !== null &&
          typeof v.value === "object" &&
          "kind" in (v.value as object) &&
          !v.text.startsWith("json(")
        ) {
          return this.typed(v.value as Typed);
        }
        if (v.text.startsWith("json(")) return this.fn("jsonValue", jsLiteral(v.value));
        return JSON.stringify(v.value);
      case "session":
        return this.sessionText(v.path);
      case "param":
        return this.fn("param", JSON.stringify(v.name));
      case "outer":
      case "frontier":
        return this.fn(v.kind, colRefText(v.column));
      case "rowId":
        return this.fn("rowId", JSON.stringify(v.row));
    }
  }

  sessionText(path: string[]): string {
    this.tools.add("session");
    return path.reduce((acc, part) => member(acc, part), "session");
  }

  /** `typed.int(5)`: exactly that value, as the file's typed literal. */
  typed(t: Typed): string {
    this.tools.add("typed");
    const num = (v: string) => (/^-?\d+(\.\d+)?$/.test(v) ? v : JSON.stringify(v));
    switch (t.kind) {
      case "literal":
        return `typed.literal(${t.raw ?? JSON.stringify(t.value)})`;
      case "int":
      case "timestamp":
      case "double":
        return `typed.${t.kind}(${num(t.value)})`;
      case "bigint":
        return `typed.bigint(${/^-?\d+$/.test(t.value) ? `${t.value}n` : JSON.stringify(t.value)})`;
      case "text":
      case "uuid":
      case "bytes":
      case "txid":
        return `typed.${t.kind}(${JSON.stringify(t.value)})`;
      case "bool":
        return `typed.bool(${t.value})`;
      case "null":
        return "null";
      case "array":
        return `typed.array(${t.items.map((i) => this.typed(i)).join(", ")})`;
      case "row":
        return `typed.row(${[t.id === null ? "null" : JSON.stringify(t.id), ...t.values.map((v) => this.typed(v))].join(", ")})`;
    }
  }

  /** `session.claims.role = 'admin'` → `eq(session.claims.role, "admin")`. */
  private sessionComparison(ast: Ast, ctx: RuleCtx, scope: Scope): string | undefined {
    const plain = (op: Operand) =>
      op.kind === "session" ? undefined : this.value(op, ctx, scope, undefined);
    if (ast.kind === "cmp") {
      let [left, right, op] = [ast.left, ast.right, ast.op];
      if (left.kind !== "session") [left, right, op] = [right, left, FLIP[op]];
      if (left.kind !== "session") return undefined;
      const value = plain(right);
      if (!value || value.args.length) return undefined;
      const text = this.fn(
        op === "=" ? "eq" : OP_FN[op],
        `${this.sessionText(left.path)}, ${value.text}`,
      );
      const name = camel(left.path[left.path.length - 1]!);
      const hint =
        op === "=" ? `${name}Is${pascal(value.text.replace(/"/g, "")) || "Set"}` : `${name}Check`;
      return this.shaped({ key: `const:${text}`, body: text, params: [], hint }, []);
    }
    if (ast.kind === "inList" && ast.left.kind === "session") {
      const values = ast.values.map(plain);
      if (values.some((v) => !v || v.args.length)) return undefined;
      const text = this.fn(
        "isIn",
        `${this.sessionText(ast.left.path)}, [${values.map((v) => v!.text).join(", ")}]`,
      );
      const name = camel(ast.left.path[ast.left.path.length - 1]!);
      const hint = `${name}In${values.map((v) => pascal(v!.text.replace(/"/g, ""))).join("Or")}`;
      return this.shaped({ key: `const:${text}`, body: text, params: [], hint }, []);
    }
    if (ast.kind === "contains" && ast.left.kind === "session") {
      const value = plain(ast.right);
      if (!value || value.args.length) return undefined;
      return this.fn("contains", `${this.sessionText(ast.left.path)}, ${value.text}`);
    }
    return undefined;
  }

  private fn(name: string, arg: string): string {
    this.tools.add(name);
    return `${name}(${arg})`;
  }

  private kindOf(table: string, column: string): string | undefined {
    if (column === "id") return "uuid";
    return columnKind(this.app.tables[table]?.columns[column]);
  }

  /** A value: literals, `null`, session values, and the checked row's columns (as placeholders). */
  private value(
    op: Operand,
    ctx: RuleCtx,
    scope: Scope,
    kind: string | undefined,
  ): Valued | undefined {
    switch (op.kind) {
      case "string":
        return { text: JSON.stringify(op.value), args: [] };
      case "number":
        if (kind === "bigint") return { text: `${op.raw}n`, args: [] };
        return { text: op.raw, args: [] };
      case "bool":
        return { text: String(op.value), args: [] };
      case "null":
        return { text: "null", args: [] };
      case "session":
        return { text: this.sessionText(op.path), args: [] };
      case "typed":
        return { text: this.typed(op.value), args: [] };
      case "column": {
        if (!scope.outer) return undefined;
        if (scope.nested) return undefined;
        if (this.isInnerColumn(op, scope)) return undefined;
        if (op.qual.kind === "named" && op.qual.name !== ctx.table) return undefined;
        ctx.usesRow = true;
        return { text: PLACEHOLDER(0), args: [member(ctx.row, op.name)] };
      }
      case "param":
        return undefined;
    }
  }
}

function colRefKey(c: ColRef): string {
  return c.scope !== undefined ? `${c.scope}.${c.column}` : c.column;
}

function colRefText(c: ColRef): string {
  return JSON.stringify(colRefKey(c));
}

/** Append `v`'s args to `args`, renumbering its placeholders. */
function shift(v: Valued, args: string[]): string {
  return v.text.replace(PLACEHOLDER_RE, (_, i: string) => {
    args.push(v.args[Number(i)]!);
    return PLACEHOLDER(args.length - 1);
  });
}

function fill(text: string, args: string[]): string {
  return text.replace(PLACEHOLDER_RE, (_, i: string) => args[Number(i)]!);
}

/** The DSL's key for a column: the file's name, one to one (`$createdBy`, `"$createdBy.account"`, …). */
function conditionKey(column: string): string {
  return column;
}

function objectText(entries: [string, string][]): string {
  return `{ ${entries.map(([k, v]) => `${propKey(k)}: ${v}`).join(", ")} }`;
}

function lineComment(text: string): string {
  return text
    .split("\n")
    .map((line) => `// ${line}`.trimEnd() + "\n")
    .join("");
}

function safeParse(text: string): Ast | undefined {
  try {
    return parseExpr(text);
  } catch {
    return undefined;
  }
}

/** A name hint from an exists body's literal values: `{ role: "admin" }` → `admin`. */
function hintOf(body: string): string {
  const literals = [...body.matchAll(/: "([A-Za-z][A-Za-z0-9_ -]*)"/g)].map((m) => m[1]!);
  return literals.map((l) => pascal(l)).join("");
}

export type { TableJson };
