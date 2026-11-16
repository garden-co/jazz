/**
 * `jazz.permissions.json` → the permissions DSL (`definePermissions`), printed
 * idiomatically:
 *
 * - `update {old, new}` → `updateIf` / `updateTo`;
 * - `allowedTo.op(rel)` → `allowedTo.op(row.rel)`;
 * - `exists t(id = this.col, …)` along a forward relation → `row.rel.where({…})`;
 * - a self-`exists(id = this.id, a = this.a, …)` in `updateTo` → `{ a: old.a, … }`;
 * - `in` lists → arrays; session-claim checks → `eq` / `isIn`;
 * - any sub-expression that repeats → one `const` helper. What it takes from
 *   the checked row becomes a parameter (a column's value, or the row itself
 *   for its relations), and so does a literal that differs between the
 *   places it repeats (`isMembershipWithRole(organizationId, role)`).
 *
 * What the DSL can't spell is reported in `unsupported` (with its JSON path)
 * rather than printed, so callers can refuse.
 *
 * How helpers are found: every sub-expression is printed as a fragment whose
 * row values and literals are numbered placeholders. Pass 1 records each
 * candidate occurrence (its shape, with every placeholder abstract, and the
 * occurrences inside it); the selection takes the largest shapes that repeat
 * first. Pass 2 walks the policies in the same order, so occurrence numbers
 * match, and replaces each occurrence of a selected shape by a call.
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
  /** Identifiers of `constants` the code uses. */
  constantsUsed: string[];
}

export interface PrintPermissionsOptions {
  /** The name `schema.ts` binds `s.defineSchema(…)` to. Default `schema`. */
  schemaName?: string;
  /** Spread before the tables, e.g. `betterAuth.permissions`. */
  spreads?: string[];
  /** Tables the spreads cover, which are not printed. */
  skipTables?: Iterable<string>;
  /**
   * Named values to print by name: a value's TS literal (`"admin"`,
   * `["editor", "viewer"]`) → its identifier. `upgrade` passes the constants
   * the old permissions.ts used.
   */
  constants?: ReadonlyMap<string, string>;
  /** Comment lines to print above `export const permissions`. */
  notes?: string[];
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
const PLACEHOLDER = (i: number) => `${i}`;
const PLACEHOLDER_RE = /(\d+)/g;

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

class Unprintable extends Error {
  constructor(
    readonly text: string,
    readonly reason: string,
  ) {
    super(reason);
  }
}

/** What a placeholder stands for. */
interface Arg {
  /** Its text at the place it is used. */
  text: string;
  /** A parameter name for it. */
  name: string;
  /** Its parameter type. */
  type: string;
  /** A literal: a parameter only when it differs between a helper's uses. */
  literal?: boolean;
}

/** A printed sub-expression, its row values and literals as placeholders. */
interface Frag {
  text: string;
  args: Arg[];
  /** Uses `old`: never a helper. */
  old?: boolean;
  /** Occurrences (candidate helper shapes) inside it, itself included. */
  occ: number[];
  /** A name for it as a helper (its structure). */
  hint?: string;
  /** A name for it inside another helper's name (its literals too: `isMembershipAdmin`). */
  label?: string;
}

/** A candidate helper occurrence, recorded in pass 1. */
interface Occurrence {
  /** The shape: the text with every placeholder abstract. */
  key: string;
  text: string;
  args: Arg[];
  hint: string;
  children: number[];
}

interface Helper {
  name: string;
  /** Per canonical placeholder: the parameter it becomes, or its fixed literal. */
  params: ({ name: string; type: string } | { fixed: string })[];
}

interface RuleCtx {
  table: string;
  row: string;
  /** Inside `updateTo`: `old` may be used. */
  allowOld: boolean;
}

/** Where bare names point while printing conditions. */
interface Scope {
  table: string;
  alias?: string;
  /** Inside `exists`: the checked row's columns are values. */
  inExists?: boolean;
  /** Inside a nested `exists`: the checked row may not be referenced. */
  nested?: boolean;
}

export function printPermissions(
  app: AppJson,
  permissions: PermissionsJson,
  options: PrintPermissionsOptions = {},
): PrintedPermissions {
  const schemaName = options.schemaName ?? "schema";
  const skip = new Set(options.skipTables ?? []);
  const constants = options.constants ?? new Map<string, string>();

  const first = new Printer(app, permissions, schemaName, constants, undefined);
  first.print(skip);
  const taken = new Set<string>([
    schemaName,
    "s",
    "permissions",
    "definePermissions",
    "old",
    ...TOOL_ORDER,
    ...constants.values(),
    ...Object.keys(app.tables).map((t) => rowName(t)),
  ]);
  const selected = selectHelpers(first.occurrences, taken);
  // A helper's parameters follow what its uses pass once inner helpers are
  // calls; inner helpers' parameters change what outer ones see, so repeat
  // until they settle.
  let printer!: Printer;
  let body = "";
  for (let round = 0; round < 8; round++) {
    printer = new Printer(app, permissions, schemaName, constants, selected);
    body = printer.print(skip);
    let changed = false;
    for (const [key, helper] of selected.byKey) {
      const params = paramsOf(printer.seen.get(key) ?? []);
      if (JSON.stringify(params) !== JSON.stringify(helper.params)) {
        helper.params = params;
        changed = true;
      }
    }
    if (!changed) break;
  }

  const lines: string[] = [];
  const tools = ["definePermissions", ...TOOL_ORDER.filter((t) => printer.tools.has(t))];
  lines.push(`const { ${tools.join(", ")} } = ${schemaName};`, "");
  for (const helper of printer.helperOrder)
    lines.push(printHelper(helper, printer.bodies.get(helper)!), "");
  for (const note of options.notes ?? []) lines.push(`// ${note}`);
  const spreads = (options.spreads ?? []).map((s) => `...${s},`);
  lines.push(`export const permissions = definePermissions({`, ...spreads, body, `});`);
  return {
    code: lines.join("\n") + "\n",
    unsupported: printer.unsupported,
    constantsUsed: [...printer.constantsUsed],
  };
}

function rowName(table: string): string {
  const name = singular(table);
  return isBindable(name) && !TOOL_ORDER.includes(name) && name !== "old" ? name : "row";
}

// ---------------------------------------------------------------------------
// Selecting helpers
// ---------------------------------------------------------------------------

interface Selection {
  byKey: Map<string, Helper>;
  /** Pass 1's occurrences' shapes, by occurrence number. */
  keyOf: string[];
}

/** Parameters from a helper's uses: row values always, literals where they differ. */
function paramsOf(uses: Arg[][]): Helper["params"] {
  const first = uses[0] ?? [];
  return first.map((arg, i) => {
    if (arg.literal && uses.every((u) => u[i]?.text === arg.text)) return { fixed: arg.text };
    return { name: arg.name, type: arg.type };
  });
}

function selectHelpers(occurrences: Occurrence[], taken: Set<string>): Selection {
  const byKey = new Map<string, number[]>();
  occurrences.forEach((o, i) => byKey.set(o.key, [...(byKey.get(o.key) ?? []), i]));
  const parent = new Map<number, number>();
  occurrences.forEach((o, i) =>
    o.children.forEach((c) => c !== i && !parent.has(c) && parent.set(c, i)),
  );
  const ancestors = (i: number): number[] => {
    const out: number[] = [];
    for (let p = parent.get(i); p !== undefined; p = parent.get(p)) out.push(p);
    return out;
  };
  // Largest shapes first: a repeated `anyOf(isCrew(…), isChief(…))` before its parts.
  const keys = [...byKey.keys()].sort((a, b) => b.length - a.length || (a < b ? -1 : 1));
  const chosen = new Set<string>();
  for (const key of keys) {
    // Uses outside chosen helpers, plus one per chosen helper whose body holds it.
    let uses = 0;
    const bodies = new Set<string>();
    for (const i of byKey.get(key)!) {
      const holder = ancestors(i).find((a) => chosen.has(occurrences[a]!.key));
      if (holder === undefined) uses++;
      else bodies.add(occurrences[holder]!.key);
    }
    if (uses + bodies.size >= 2) chosen.add(key);
  }

  // Parameters: row values always, literals where they differ between uses.
  const helpers = new Map<string, Helper>();
  const drafts: { base: string; qualifiers: string[]; helper: Helper }[] = [];
  for (const key of keys.filter((k) => chosen.has(k)).reverse()) {
    const occ = byKey.get(key)!.map((i) => occurrences[i]!);
    const firstOcc = occ[0]!;
    const params = paramsOf(occ.map((o) => o.args));
    const varying = params.flatMap((p, i) =>
      "name" in p && firstOcc.args[i]!.literal ? [p.name] : [],
    );
    const fixed = params.flatMap((p, i) =>
      "fixed" in p ? [`${pascal(firstOcc.args[i]!.name)}${pascal(p.fixed.replace(/"/g, ""))}`] : [],
    );
    // A combination too long to name by its parts is named for the row it checks.
    const row = /^s\.Row<typeof \w+, "(\w+)">$/.exec(
      firstOcc.args.find((a) => a.type.startsWith("s.Row<"))?.type ?? "",
    )?.[1];
    const hint =
      (firstOcc.hint === "anyRule" || firstOcc.hint === "allRules") && row
        ? `${camel(singular(row))}Rule`
        : firstOcc.hint;
    const base = hint + varying.map((n) => `With${pascal(n)}`).join("");
    const helper: Helper = { name: "", params };
    helpers.set(key, helper);
    drafts.push({ base, qualifiers: fixed, helper });
  }
  // Names: the base; helpers sharing one are told apart by their fixed literals.
  const counts = new Map<string, number>();
  for (const d of drafts) counts.set(d.base, (counts.get(d.base) ?? 0) + 1);
  for (const d of drafts) {
    let name = d.base;
    if ((counts.get(d.base) ?? 0) > 1 && d.qualifiers.length)
      name += `With${d.qualifiers.join("And")}`;
    if (name.length > 40) name = d.base;
    d.helper.name = fresh(name, taken);
  }
  return { byKey: helpers, keyOf: occurrences.map((o) => o.key) };
}

function printHelper(helper: Helper, source: string): string {
  const taken = new Set<string>();
  const names = helper.params.map((p) =>
    "name" in p ? fresh(isBindable(p.name) ? p.name : "value", taken) : "",
  );
  let body = source;
  // `{ chatId: chatId }` → `{ chatId }`
  body = body.replace(/([A-Za-z_$][A-Za-z0-9_$]*): (\d+)/g, (all, key: string, i: string) =>
    names[Number(i)] === key ? key : all,
  );
  body = body.replace(PLACEHOLDER_RE, (_, i: string) => {
    const p = helper.params[Number(i)]!;
    return "fixed" in p ? p.fixed : names[Number(i)]!;
  });
  const params = helper.params.flatMap((p, i) => ("name" in p ? [`${names[i]}: ${p.type}`] : []));
  if (params.length === 0) return `const ${helper.name} = ${body};`;
  return `const ${helper.name} = (${params.join(", ")}) => ${body};`;
}

// ---------------------------------------------------------------------------
// Fragments
// ---------------------------------------------------------------------------

const lit = (text: string, hint?: string): Frag => ({
  text,
  args: [],
  occ: [],
  ...(hint ? { hint } : {}),
});

/**
 * Join fragments with `render`, renumbering their placeholders. A row value
 * used twice is one placeholder; literals stay apart (they may differ).
 */
function join(parts: Frag[], render: (texts: string[]) => string): Frag {
  const args: Arg[] = [];
  const texts = parts.map((part) =>
    part.text.replace(PLACEHOLDER_RE, (_, i: string) => {
      const arg = part.args[Number(i)]!;
      let at = arg.literal ? -1 : args.findIndex((a) => !a.literal && a.text === arg.text);
      if (at < 0) {
        args.push(arg);
        at = args.length - 1;
      }
      return PLACEHOLDER(at);
    }),
  );
  return {
    text: render(texts),
    args,
    old: parts.some((p) => p.old),
    occ: parts.flatMap((p) => p.occ),
  };
}

/** Number placeholders by first appearance; fold `row.col` into a `row` argument. */
function canonical(frag: Frag): Frag {
  const rows = new Map(frag.args.map((a, i) => [a.text, i] as const));
  let text = frag.text.replace(PLACEHOLDER_RE, (all, i: string) => {
    const arg = frag.args[Number(i)]!;
    if (arg.literal) return all;
    const dot = arg.text.lastIndexOf(".");
    const row = dot > 0 ? rows.get(arg.text.slice(0, dot)) : undefined;
    return row !== undefined && row !== Number(i)
      ? `${PLACEHOLDER(row)}${arg.text.slice(dot)}`
      : all;
  });
  const order: number[] = [];
  text = text.replace(PLACEHOLDER_RE, (_, i: string) => {
    let at = order.indexOf(Number(i));
    if (at < 0) {
      order.push(Number(i));
      at = order.length - 1;
    }
    return PLACEHOLDER(at);
  });
  return { ...frag, text, args: order.map((i) => frag.args[i]!) };
}

function fill(frag: Frag): string {
  return frag.text.replace(PLACEHOLDER_RE, (_, i: string) => frag.args[Number(i)]!.text);
}

// ---------------------------------------------------------------------------
// The printer
// ---------------------------------------------------------------------------

class Printer {
  readonly tools = new Set<string>();
  readonly unsupported: Unsupported[] = [];
  readonly occurrences: Occurrence[] = [];
  readonly constantsUsed = new Set<string>();
  /** Helpers in the order their bodies were found: inner ones first. */
  readonly helperOrder: Helper[] = [];
  /** Per selected shape, the arguments of each use (inner helpers as calls). */
  readonly seen = new Map<string, Arg[][]>();
  /** Each helper's body: its first use, inner helpers as calls. */
  readonly bodies = new Map<Helper, string>();

  constructor(
    readonly app: AppJson,
    readonly permissions: PermissionsJson,
    readonly schemaName: string,
    readonly constants: ReadonlyMap<string, string>,
    readonly selection: Selection | undefined,
  ) {}

  print(skip: Set<string>): string {
    const out: string[] = [];
    for (const table of Object.keys(this.app.tables).sort()) {
      if (skip.has(table)) continue;
      out.push(this.table(table, this.permissions.tables[table] ?? {}));
    }
    return out.join("\n");
  }

  /**
   * A candidate helper: recorded in pass 1, replaced by a call in pass 2 when
   * its shape was selected (and the first one kept as the helper's body).
   */
  private shaped(frag: Frag, hint: string, table?: string, literalsInHint = false): Frag {
    if (frag.old) return { ...frag, hint };
    const c = canonical(frag);
    const id = this.occurrences.length;
    const key = (table ? `${table}|` : "") + c.text;
    this.occurrences.push({ key, text: c.text, args: c.args, hint, children: frag.occ });
    // Pass 2 matches pass 1 by occurrence number: inner helpers are calls now.
    const shape = this.selection?.keyOf[id] ?? key;
    const helper = this.selection?.byKey.get(shape);
    const label = literalsInHint
      ? hint
      : hint +
        c.args
          .filter((a) => a.literal)
          .map((a) => pascal(a.text.replace(/"/g, "")))
          .join("");
    if (!helper) return { ...c, occ: [...frag.occ, id], hint, label };
    this.seen.set(shape, [...(this.seen.get(shape) ?? []), c.args]);
    if (!this.bodies.has(helper)) {
      this.bodies.set(helper, c.text);
      this.helperOrder.push(helper);
    }
    const args: Arg[] = [];
    const callArgs: string[] = [];
    helper.params.forEach((p, i) => {
      if ("fixed" in p) return;
      args.push(c.args[i]!);
      callArgs.push(PLACEHOLDER(args.length - 1));
    });
    const takes = helper.params.some((p) => "name" in p);
    const text = takes ? `${helper.name}(${callArgs.join(", ")})` : helper.name;
    const passed = args
      .filter((a) => a.literal)
      .map((a) => pascal(a.text.replace(/"/g, "")))
      .join("");
    return {
      text,
      args,
      occ: [id],
      hint: helper.name,
      label: helper.name.replace(/With[A-Z]\w*$/, "") + passed,
    };
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
    const ctx: RuleCtx = { table, row: rowName(table), allowOld };
    let frag: Frag;
    try {
      frag = this.policy(policy, ctx, path);
    } catch (error) {
      if (!(error instanceof Unprintable)) throw error;
      this.unsupported.push({ path, text: error.text, reason: error.reason });
      return "false";
    }
    const text = fill(frag);
    const body = text.startsWith("{") ? `(${text})` : text;
    const usesRow = frag.args.some((a) => !a.literal);
    if (frag.old) return `(${ctx.row}, old) => ${body}`;
    if (usesRow) return `(${ctx.row}) => ${body}`;
    return text;
  }

  private policy(policy: PolicyJson, ctx: RuleCtx, path: string): Frag {
    if (typeof policy === "string") {
      let ast: Ast;
      try {
        ast = parseExpr(policy);
      } catch (error) {
        throw new Unprintable(policy, (error as Error).message);
      }
      if (JSON.stringify(ast).includes('"kind":"call"')) {
        throw new Unprintable(
          policy,
          "`defs` are not part of the alpha.66 format; inline them first",
        );
      }
      try {
        return this.expr(ast, ctx, { table: ctx.table });
      } catch (error) {
        if (error instanceof Unprintable && error.text === "")
          throw new Unprintable(policy, error.reason);
        throw error;
      }
    }
    if ("anyOf" in policy) {
      return this.combinator(
        "anyOf",
        policy.anyOf.map((p, i) => this.policy(p, ctx, `${path}.anyOf[${i}]`)),
      );
    }
    if ("allOf" in policy) {
      const items = policy.allOf.map((p) => (typeof p === "string" ? safeParse(p) : undefined));
      const merged = this.mergeConditions(items, ctx, { table: ctx.table });
      if (merged) return merged;
      return this.combinator(
        "allOf",
        policy.allOf.map((p, i) => this.policy(p, ctx, `${path}.allOf[${i}]`)),
      );
    }
    if ("where" in policy) {
      const inner = this.policy(policy.where, ctx, `${path}.where`);
      return { ...inner, text: `${lineComment(policy.$comment)}${inner.text}` };
    }
    throw new Unprintable(
      JSON.stringify(policy),
      "a policy with both `using` and `withCheck` has no DSL form",
    );
  }

  /** `anyOf(…)` / `allOf(…)` / `not(…)`: a helper when it repeats. */
  private combinator(fn: "anyOf" | "allOf" | "not", items: Frag[]): Frag {
    this.tools.add(fn);
    const frag = join(items, (texts) => `${fn}(${texts.join(", ")})`);
    if (items.length === 0 || (fn !== "not" && items.length < 2)) return frag;
    const hints = items.map((i) => i.label ?? i.hint ?? "rule");
    let hint: string;
    if (fn === "not") hint = `not${pascal(hints[0]!)}`;
    else {
      const sep = fn === "anyOf" ? "Or" : "And";
      // `isMembershipAdmin`, `isMembershipEditor` → `isMembershipAdminOrEditor`.
      const named = (parts: string[]) => {
        const words = parts.map((p) => p.split(/(?=[A-Z])/));
        return words
          .map((w, i) => {
            if (i === 0) return w.join("");
            let shared = 0;
            while (shared < w.length - 1 && w[shared] === words[0]![shared]) shared++;
            return pascal(
              w
                .slice(shared)
                .join("")
                .replace(/^is(?=[A-Z])/, ""),
            );
          })
          .join(sep);
      };
      // Short enough to read: all the parts, else the first and "…OrMore".
      hint = named(hints);
      if (hint.length > 40) hint = named([hints[0]!, "more"]);
      if (hint.length > 40) hint = fn === "anyOf" ? "anyRule" : "allRules";
    }
    return this.shaped(frag, hint, undefined, true);
  }

  /**
   * `a = 1 and b = 2` → `{ a: 1, b: 2 }`, when every item is a plain
   * condition on a distinct column of the scope.
   */
  private mergeConditions(
    items: (Ast | undefined)[],
    ctx: RuleCtx,
    scope: Scope,
  ): Frag | undefined {
    if (items.length < 2 || items.some((i) => i === undefined)) return undefined;
    const entries: [string, Frag][] = [];
    for (const item of items) {
      const cond = this.condition(item!, ctx, scope);
      if (!cond || entries.some(([k]) => k === cond[0])) return undefined;
      entries.push(cond);
    }
    return this.conditionObject(entries, scope);
  }

  /** A condition object: a helper when it is a check on the session that repeats (`mine`). */
  private conditionObject(entries: [string, Frag][], scope: Scope): Frag {
    const frag = objectFrag(entries);
    frag.hint = frag.label = conditionHint(entries);
    if (scope.inExists || entries.length !== 1) return frag;
    const [key, value] = entries[0]!;
    if (!value.text.startsWith("session.")) return frag;
    const hint =
      value.text === "session.user"
        ? key.startsWith("$createdBy")
          ? "createdByMe"
          : key === "userId"
            ? "mine"
            : `${camel(key.replace(/Id$/, ""))}IsMe`
        : `${camel(key.replace(/^\$/, ""))}FromSession`;
    return this.shaped(frag, hint, undefined, true);
  }

  private expr(ast: Ast, ctx: RuleCtx, scope: Scope): Frag {
    switch (ast.kind) {
      case "bool":
        return lit(String(ast.value));
      case "or":
        return this.combinator(
          "anyOf",
          ast.items.map((i) => this.expr(i, ctx, scope)),
        );
      case "and": {
        const merged = this.mergeConditions(ast.items, ctx, scope);
        if (merged) return merged;
        return this.combinator(
          "allOf",
          ast.items.map((i) => this.expr(i, ctx, scope)),
        );
      }
      case "not":
        return this.combinator("not", [this.expr(ast.inner, ctx, scope)]);
      case "exists":
        return this.exists(ast, ctx, scope);
      case "existsRel":
        this.tools.add("exists");
        return lit(`exists(${this.relation(ast.relation)})`, "relationExists");
      case "allowed": {
        if (scope.inExists) throw new Unprintable("", "`allowedTo` inside `exists`");
        this.tools.add("allowedTo");
        const depth = ast.maxDepth !== undefined ? `, { maxDepth: ${ast.maxDepth} }` : "";
        if (ast.target.kind === "via" && ast.target.table !== undefined) {
          const target = member(
            member(`${this.schemaName}.tables`, ast.target.table),
            ast.target.column,
          );
          return lit(
            `allowedTo.${ast.op}(${target}${depth})`,
            `can${pascal(ast.op)}Via${pascal(ast.target.table)}`,
          );
        }
        const name = ast.target.kind === "relation" ? ast.target.name : ast.target.column;
        return {
          text: `allowedTo.${ast.op}(${member(PLACEHOLDER(0), name)}${depth})`,
          args: [this.rowArg(ctx)],
          occ: [],
          hint: `can${pascal(ast.op)}${pascal(name)}`,
        };
      }
      case "call":
        throw new Unprintable("", "`defs` calls are not part of the alpha.66 format");
      default: {
        const cond = this.condition(ast, ctx, scope);
        if (cond) return this.conditionObject([cond], scope);
        const session = this.sessionComparison(ast, ctx, scope);
        if (session) return session;
        throw new Unprintable("", "a comparison the DSL has no condition for");
      }
    }
  }

  /** The checked row itself, as an argument (for its relations). */
  private rowArg(ctx: RuleCtx): Arg {
    return {
      text: ctx.row,
      name: ctx.row,
      type: `s.Row<typeof ${this.schemaName}, ${JSON.stringify(ctx.table)}>`,
    };
  }

  /** A column of the checked row, as an argument. */
  private columnArg(ctx: RuleCtx, column: string): Arg {
    return {
      text: member(ctx.row, column),
      name: column,
      type: valueType(this.kindOf(ctx.table, column)),
    };
  }

  /** `exists t(…)`, idiomatically. */
  private exists(ast: Extract<Ast, { kind: "exists" }>, ctx: RuleCtx, scope: Scope): Frag {
    if (scope.inExists) {
      // An `exists` inside another: its conditions name only its own rows.
      return this.existsGeneric(ast, ctx, {
        table: ast.table,
        alias: ast.alias,
        inExists: true,
        nested: true,
      });
    }
    const inner: Scope = { table: ast.table, alias: ast.alias, inExists: true };
    /** The checked row's column that `c` compares `col` of `t` with. */
    const outerOf = (c: Ast | undefined, col: string): string | undefined => {
      if (c?.kind !== "cmp" || c.op !== "=") return undefined;
      if (c.left.kind !== "column" || !this.isInnerColumn(c.left, inner) || c.left.name !== col)
        return undefined;
      return this.outerColumn(c.right, inner);
    };
    // `{ a: old.a, b: old.b }`: the old row is the same table's row with this id,
    // each condition comparing one of its columns with the new row's.
    if (ctx.allowOld && ast.table === ctx.table && ast.hops.length === 0) {
      const idIndex = ast.conditions.findIndex((c) => outerOf(c, "id") === "id");
      const rest = ast.conditions.filter((_, i) => i !== idIndex);
      const olds = rest.map((c) =>
        c.kind === "cmp" &&
        c.left.kind === "column" &&
        this.isInnerColumn(c.left, inner) &&
        this.outerColumn(c.right, inner) === c.left.name
          ? { column: c.left.name, op: FLIP[c.op] }
          : undefined,
      );
      const columns = new Set(olds.map((o) => o?.column));
      if (idIndex >= 0 && rest.length > 0 && olds.every((o) => o) && columns.size === olds.length) {
        const text = objectText(
          olds.map((o) => {
            const value = member("old", o!.column);
            const op = o!.op;
            return [o!.column, op === "=" ? value : this.fn(OP_FN[op], value)];
          }),
        );
        return { text, args: [], occ: [], old: true, hint: "unchanged" };
      }
    }
    // `row.rel.where({…})` along a forward relation: the one form for that check.
    if (ast.hops.length === 0) {
      const idIndex = ast.conditions.findIndex((c) => outerOf(c, "id") !== undefined);
      if (idIndex >= 0) {
        const column = outerOf(ast.conditions[idIndex], "id")!;
        const relations = this.app.tables[ctx.table]?.relations ?? {};
        const rel = Object.entries(relations).find(
          ([, r]) => "rel" in r && r.rel === ast.table && r.column === column,
        );
        if (rel) {
          const rest = ast.conditions.filter((_, i) => i !== idIndex);
          const conds = this.conditions(rest, ctx, inner);
          if (conds && conds.extras.length === 0) {
            const where = conds.object ?? lit("{}");
            const row: Frag = { text: PLACEHOLDER(0), args: [this.rowArg(ctx)], occ: [] };
            const frag = join([row, where], ([r, w]) => `${member(r!, rel[0])}.where(${w})`);
            const hint = `${camel(rel[0])}${where.hint ? `With${pascal(where.hint)}` : ""}`;
            return this.shaped(frag, hint, ctx.table);
          }
        }
      }
    }
    return this.existsGeneric(ast, ctx, inner);
  }

  private existsGeneric(ast: Extract<Ast, { kind: "exists" }>, ctx: RuleCtx, inner: Scope): Frag {
    this.tools.add("exists");
    const head = this.conditions(ast.conditions, ctx, inner);
    if (!head)
      throw new Unprintable("", "an `exists` whose conditions aren't plain column conditions");
    const parts: Frag[] = [head.object ?? lit("{}"), ...head.extras];
    let frag = join(parts, (texts) => `${member("exists", ast.table)}(${texts.join(", ")})`);
    let table = ast.table;
    for (const hop of ast.hops) {
      const target = this.relationTarget(table, hop.relation);
      if (!target) throw new Unprintable("", `unknown relation \`${hop.relation}\` of ${table}`);
      const conds = this.conditions(hop.conditions, ctx, {
        table: target,
        inExists: true,
        nested: inner.nested,
      });
      if (!conds)
        throw new Unprintable("", "a hop whose conditions aren't plain column conditions");
      const hopParts =
        conds.object || conds.extras.length ? [conds.object ?? lit("{}"), ...conds.extras] : [];
      frag = join(
        [frag, ...hopParts],
        ([h, ...rest]) => `${h}.hop(${[JSON.stringify(hop.relation), ...rest].join(", ")})`,
      );
      table = target;
    }
    const hint = `is${pascal(singular(ast.table))}`;
    if (inner.nested) return { ...frag, hint };
    return this.shaped(frag, hint);
  }

  private relationTarget(table: string, relation: string): string | undefined {
    const r = this.app.tables[table]?.relations?.[relation];
    if (!r) return undefined;
    return "rel" in r ? r.rel : r.reverse;
  }

  /**
   * Conditions inside `exists` (or a hop): one object of plain conditions,
   * then the policies that aren't (`exists.t({ a: 1 }, anyOf(…))`).
   */
  private conditions(
    items: Ast[],
    ctx: RuleCtx,
    scope: Scope,
  ): { object: Frag | undefined; extras: Frag[] } | undefined {
    const flat: Ast[] = [];
    for (const item of items) {
      if (item.kind === "and") flat.push(...item.items);
      else flat.push(item);
    }
    const entries: [string, Frag][] = [];
    const extraItems: Ast[] = [];
    for (const item of flat) {
      const cond = this.condition(item, ctx, scope);
      if (!cond || entries.some(([k]) => k === cond[0])) extraItems.push(item);
      else entries.push(cond);
    }
    const extras = extraItems.map((item) => this.expr(item, ctx, scope));
    if (entries.length === 0) return { object: undefined, extras };
    // A constant object may be a helper used elsewhere (`msg.sender.where(mine)`).
    const constant = entries.every(([, v]) => v.args.every((a) => a.literal));
    const object = constant
      ? this.conditionObject(entries, { table: scope.table })
      : objectFrag(entries);
    object.hint ??= conditionHint(entries);
    return { object, extras };
  }

  private isInnerColumn(op: Operand, scope: Scope): boolean {
    if (op.kind !== "column") return false;
    if (op.qual.kind === "none") return true;
    if (op.qual.kind === "named")
      return op.qual.name === (scope.alias ?? scope.table) || op.qual.name === scope.table;
    if (!scope.inExists) return op.qual.kind === "this";
    return false;
  }

  /** The checked row's column a value names, inside `exists`. */
  private outerColumn(op: Operand, scope: Scope): string | undefined {
    if (!scope.inExists || op.kind !== "column" || this.isInnerColumn(op, scope)) return undefined;
    return op.name;
  }

  /** A plain condition `[key, value]` on a column of the scope. */
  private condition(ast: Ast, ctx: RuleCtx, scope: Scope): [string, Frag] | undefined {
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
        const value = this.value(right, ctx, scope, this.kindOf(scope.table, left), left);
        if (!value) return undefined;
        if (right.kind === "null") {
          if (op === "=") return [left, value];
          if (op === "!=") return [left, wrap(value, (t) => this.fn("ne", t))];
          return undefined;
        }
        if (op === "=") return [left, value];
        const fn = OP_FN[op];
        return [left, wrap(value, (t) => this.fn(fn, t))];
      }
      case "inList": {
        const left = column(ast.left);
        if (left === undefined) return undefined;
        const kind = this.kindOf(scope.table, left);
        // A named list (`invitableRoles`) by name.
        if (
          ast.values.every((v) => v.kind === "string" || v.kind === "number" || v.kind === "bool")
        ) {
          const text = `[${ast.values.map((v) => this.plainLiteral(v, kind)!).join(", ")}]`;
          const named = this.constants.get(text);
          if (named) {
            this.constantsUsed.add(named);
            return [left, lit(named)];
          }
        }
        const values = ast.values.map((v) => this.value(v, ctx, scope, kind, left));
        if (values.some((v) => !v)) return undefined;
        return [left, join(values as Frag[], (texts) => `[${texts.join(", ")}]`)];
      }
      case "inSession": {
        const left = column(ast.left);
        if (left === undefined) return undefined;
        return [left, lit(this.fn("isIn", this.sessionText(ast.path)))];
      }
      case "contains": {
        const left = column(ast.left);
        if (left === undefined) return undefined;
        const value = this.value(ast.right, ctx, scope, undefined, left);
        if (!value) return undefined;
        return [left, wrap(value, (t) => this.fn("contains", t))];
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
        this.tools.add("from");
        const depth = rel.maxDepth === "fixpoint" ? '"fixpoint"' : String(rel.maxDepth);
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

  private call(fn: string, args: string[]): string {
    this.tools.add(fn);
    return `${fn}(${args.join(", ")})`;
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

  /** `session.claims.role = 'admin'` → `eq(session.claims.role, "admin")`: a helper when it repeats. */
  private sessionComparison(ast: Ast, ctx: RuleCtx, scope: Scope): Frag | undefined {
    const plain = (op: Operand) =>
      op.kind === "session" ? undefined : this.value(op, ctx, scope, undefined);
    const claim = (path: string[]) => camel(path[path.length - 1]!);
    if (ast.kind === "cmp") {
      let [left, right, op] = [ast.left, ast.right, ast.op];
      if (left.kind !== "session") [left, right, op] = [right, left, FLIP[op]];
      if (left.kind !== "session") return undefined;
      const value = plain(right);
      if (!value || value.args.some((a) => !a.literal)) return undefined;
      const fn = op === "=" ? "eq" : OP_FN[op];
      const session = this.sessionText(left.path);
      const frag = wrap(value, (t) => this.fn(fn, `${session}, ${t}`));
      const shown = fill(value).replace(/"/g, "");
      const hint =
        op === "=" ? `${claim(left.path)}Is${pascal(shown) || "Set"}` : `${claim(left.path)}Check`;
      return this.shaped(frag, hint, undefined, true);
    }
    if (ast.kind === "inList" && ast.left.kind === "session") {
      const values = ast.values.map((v) => plain(v));
      if (values.some((v) => !v || v.args.some((a) => !a.literal))) return undefined;
      const session = this.sessionText(ast.left.path);
      const list = join(values as Frag[], (texts) => `[${texts.join(", ")}]`);
      const named = this.constants.get(fill(list));
      if (named) this.constantsUsed.add(named);
      const frag = named
        ? lit(this.fn("isIn", `${session}, ${named}`))
        : wrap(list, (t) => this.fn("isIn", `${session}, ${t}`));
      const shown = named
        ? pascal(named)
        : (values as Frag[]).map((v) => pascal(fill(v).replace(/"/g, ""))).join("Or");
      return this.shaped(frag, `${claim(ast.left.path)}In${shown}`, undefined, true);
    }
    if (ast.kind === "contains" && ast.left.kind === "session") {
      const value = plain(ast.right);
      if (!value || value.args.some((a) => !a.literal)) return undefined;
      const session = this.sessionText(ast.left.path);
      return wrap(value, (t) => this.fn("contains", `${session}, ${t}`));
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

  private plainLiteral(op: Operand, kind: string | undefined): string | undefined {
    switch (op.kind) {
      case "string":
        return JSON.stringify(op.value);
      case "number":
        return kind === "bigint" ? `${op.raw}n` : op.raw;
      case "bool":
        return String(op.value);
      default:
        return undefined;
    }
  }

  /**
   * A value: a literal (a placeholder, so a helper may take it), `null`, a
   * session value, or a column of the checked row (a placeholder too).
   */
  private value(
    op: Operand,
    ctx: RuleCtx,
    scope: Scope,
    kind: string | undefined,
    key = "value",
  ): Frag | undefined {
    switch (op.kind) {
      case "string":
      case "number":
      case "bool": {
        const text = this.plainLiteral(op, kind)!;
        const named = this.constants.get(text);
        if (named) {
          this.constantsUsed.add(named);
          return lit(named);
        }
        const valueKind =
          kind && kind !== "enum" && kind !== "json" && kind !== "array"
            ? kind
            : op.kind === "string"
              ? "string"
              : op.kind === "bool"
                ? "boolean"
                : "float";
        const name = camel(key.replace(/^\$/, "").replace(/\./g, " ")) || "value";
        return {
          text: PLACEHOLDER(0),
          args: [{ text, name, type: valueType(valueKind), literal: true }],
          occ: [],
        };
      }
      case "null":
        return lit("null");
      case "session":
        return lit(this.sessionText(op.path));
      case "typed":
        return lit(this.typed(op.value));
      case "column": {
        if (!scope.inExists || scope.nested) return undefined;
        if (this.isInnerColumn(op, scope)) return undefined;
        if (op.qual.kind === "named" && op.qual.name !== ctx.table) return undefined;
        return { text: PLACEHOLDER(0), args: [this.columnArg(ctx, op.name)], occ: [] };
      }
      case "param":
        return undefined;
    }
  }
}

function valueType(kind: string | undefined): string {
  if (kind === "uuid") return "s.Uuid";
  return `s.Value<${JSON.stringify(kind ?? "string")}>`;
}

/** `value` inside `render(…)`, keeping its placeholders. */
function wrap(value: Frag, render: (text: string) => string): Frag {
  return { ...value, text: render(value.text) };
}

function objectFrag(entries: [string, Frag][]): Frag {
  return join(
    entries.map(([, v]) => v),
    (texts) => objectText(entries.map(([k], i) => [k, texts[i]!])),
  );
}

/** A name for a condition object: its first key and literal (`roleAdmin`), or its keys. */
function conditionHint(entries: [string, Frag][]): string {
  const [key, value] = entries[0]!;
  const name = camel(key.replace(/^\$/, "").replace(/\./g, " "));
  const shown = fill(value);
  if (/^"[A-Za-z][\w -]*"$/.test(shown)) return `${name}${pascal(shown.slice(1, -1))}`;
  if (shown === "true") return name;
  if (shown === "null") return `no${pascal(name)}`;
  return entries.length > 1 ? `${name}And${pascal(camel(entries[1]![0]))}` : name;
}

function colRefKey(c: ColRef): string {
  return c.scope !== undefined ? `${c.scope}.${c.column}` : c.column;
}

function colRefText(c: ColRef): string {
  return JSON.stringify(colRefKey(c));
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

export type { TableJson };
