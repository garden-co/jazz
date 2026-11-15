/**
 * Step 2 of `jazz-tools upgrade`: turn a .64 app's TS-only columns into the
 * `computed` fields and `transforms` jazz.app.json declares, and move their
 * callbacks, unchanged, into `jazz.computed.ts`.
 *
 * schema.ts is read with the TypeScript checker against the DSL types of the
 * project's jazz-tools (≤ alpha.64), never run: `s.computed` return types
 * give the declared types, the callbacks' parameter use gives `from`, and
 * the callbacks' source text moves as it is, with the top-level helpers and
 * imports it uses.
 */
import { basename, dirname, relative } from "node:path";
import type * as TS from "typescript";
import {
  columnType,
  type AppEdits,
  type ComputedDecl,
  type Json,
  type JsonObject,
  type TransformDecl,
  type TsOnlyMarkers,
  type TypeSpec,
} from "./app-json.js";
import { UpgradeRefusal, notSupportedYet } from "./errors.js";

type Ts = typeof TS;

export interface ComputedConversion extends AppEdits {
  /** The text of `jazz.computed.ts`, or `null` when the app has nothing TS-only. */
  computedFile: string | null;
  /** Names schema.ts exported that now live in jazz.computed.ts. */
  movedExports: string[];
  /** Decisions left to the author and guesses worth a look. */
  notes: string[];
  /** Number of `"TODO: …"` strings written into jazz.app.json. */
  todos: number;
}

export interface ComputedInput {
  ts: Ts;
  program: TS.Program;
  schemaFile: string;
  /** The current tables of jazz.app.json. */
  tables: Record<string, JsonObject>;
  markers: TsOnlyMarkers;
}

interface Found {
  table: string;
  field: string;
  node: TS.CallExpression;
}

/** Convert every TS-only column of schema.ts, or refuse naming what can't be. */
export function convertComputed(input: ComputedInput): ComputedConversion {
  const { ts, program, schemaFile, tables, markers } = input;
  const source = program.getSourceFile(schemaFile);
  if (!source) throw new UpgradeRefusal(`TypeScript could not read ${schemaFile}`);
  const checker = program.getTypeChecker();
  const ctx = new Context(ts, checker, source);

  const computedCalls: Found[] = [];
  const transformCalls: Found[] = [];
  const visit = (node: TS.Node): void => {
    if (ts.isCallExpression(node)) {
      if (ctx.isDslCall(node, "computed")) {
        computedCalls.push({ ...ctx.placeInTable(node, "computed"), node });
      } else if (ctx.isTransformCall(node)) {
        transformCalls.push({ ...ctx.placeInTable(node, "column"), node });
      }
    }
    ts.forEachChild(node, visit);
  };
  visit(source);

  // The AST and the file's tsOnly markers must agree: both come from the same schema.ts.
  const seen = new Set<string>();
  for (const found of [...computedCalls, ...transformCalls]) {
    const kind = computedCalls.includes(found) ? "computed" : "transform";
    const key = `${found.table}.${found.field}`;
    if (!tables[found.table]) {
      throw new UpgradeRefusal(
        `found ${kind === "computed" ? "s.computed" : ".transform"} on table \`${found.table}\`, which jazz.app.json doesn't have`,
        ctx.location(found.node),
      );
    }
    if (markers[found.table]?.[found.field] !== kind) {
      throw new UpgradeRefusal(
        `jazz.app.json doesn't mark ${key} as tsOnly "${kind}": it is out of date with schema.ts; run the old \`jazz-tools validate\` first`,
        ctx.location(found.node),
      );
    }
    seen.add(key);
  }
  for (const [table, fields] of Object.entries(markers)) {
    for (const [field, kind] of Object.entries(fields)) {
      if (!seen.has(`${table}.${field}`)) {
        throw notSupportedYet(
          `a ${kind} column defined outside schema.ts's s.table(...) calls (jazz.app.json marks ${table}.${field} as tsOnly "${kind}", but upgrade couldn't find where schema.ts defines it)`,
          relative(process.cwd(), schemaFile) || schemaFile,
          "define it inline in s.table(...) in schema.ts, run the old `jazz-tools validate`, then upgrade again",
        );
      }
    }
  }

  const edits: AppEdits = { computed: {}, transforms: {}, columnTransforms: {} };
  const notes: string[] = [];
  let todos = 0;
  const computedEntries: Record<string, Record<string, string>> = {};
  const transformEntries: Record<string, string> = {};

  for (const found of computedCalls) {
    const callback = found.node.arguments[0];
    if (!callback || found.node.arguments.length !== 1) {
      throw new UpgradeRefusal(
        "s.computed(...) takes exactly one callback",
        ctx.location(found.node),
      );
    }
    const fn = ctx.functionOf(callback);
    const table = tables[found.table]!;
    const from = ctx.readColumns(fn, table, markers[found.table] ?? {}, found, notes);
    const signature = checker.getSignatureFromDeclaration(fn);
    const returnType = signature ? checker.getReturnTypeOfSignature(signature) : undefined;
    if (!returnType) {
      throw new UpgradeRefusal(
        `can't read the return type of ${found.table}.${found.field}`,
        ctx.location(fn),
      );
    }
    const typed = ctx.typeSpec(returnType, fn, `${found.table}.${found.field}`, table);
    if (typed.todo) todos += 1;
    if (typed.note) notes.push(typed.note);
    (edits.computed[found.table] ??= {})[found.field] = {
      type: typed.spec,
      from,
    } satisfies ComputedDecl;
    ctx.move(callback);
    (computedEntries[found.table] ??= {})[found.field] = callback.getText(source);
  }

  for (const found of transformCalls) {
    const arg = found.node.arguments[0];
    if (!arg || found.node.arguments.length !== 1) {
      throw new UpgradeRefusal(
        ".transform(...) takes exactly one { from, to } object",
        ctx.location(found.node),
      );
    }
    const literal = ctx.objectLiteralOf(arg);
    const names = literal.properties.map((p) =>
      p.name && ts.isIdentifier(p.name) ? p.name.text : "?",
    );
    if (names.includes("dependsOn")) {
      throw notSupportedYet(
        `.transform({ dependsOn }) on ${found.table}.${found.field}`,
        ctx.location(literal),
        "a transform in jazz.app.json maps one stored value to one view value; remove dependsOn (compute the dependent value with s.computed instead) and run upgrade again",
      );
    }
    const unexpected = names.filter((n) => n !== "from" && n !== "to");
    if (unexpected.length > 0 || !names.includes("from") || !names.includes("to")) {
      throw notSupportedYet(
        `a transform that isn't exactly { from, to } (${found.table}.${found.field} has ${names.join(", ") || "nothing"})`,
        ctx.location(literal),
      );
    }
    const column = tables[found.table]!.columns as JsonObject | undefined;
    const spec = column?.[found.field];
    if (spec === undefined) {
      throw new UpgradeRefusal(
        `jazz.app.json has no column ${found.table}.${found.field} for its transform`,
        ctx.location(found.node),
      );
    }
    const stored = columnType(spec).type;
    const fromProp = literal.properties.find(
      (p) => p.name && ts.isIdentifier(p.name) && p.name.text === "from",
    )!;
    const fromFn = ctx.methodOrFunction(fromProp);
    const viewType = ctx.firstParamOfTo(literal) ?? ctx.returnTypeOf(fromFn);
    const typed = ctx.typeSpec(
      viewType,
      fromFn,
      `the view type of ${found.table}.${found.field}'s transform`,
      undefined,
    );
    if (typed.todo) todos += 1;
    if (typed.note) notes.push(typed.note);
    const name = ctx.transformName(arg, found);
    if (transformEntries[name] !== undefined) {
      throw new UpgradeRefusal(
        `two transforms would be named \`${name}\`; rename one of them in schema.ts`,
        ctx.location(arg),
      );
    }
    // A transform's types carry no `?`: the column it is named on says whether it is optional.
    edits.transforms[name] = { stored, view: required(typed.spec) } satisfies TransformDecl;
    (edits.columnTransforms[found.table] ??= {})[found.field] = name;
    ctx.move(arg);
    transformEntries[name] = arg.getText(source);
  }

  const hasComputed = Object.keys(computedEntries).length > 0;
  const hasTransforms = Object.keys(transformEntries).length > 0;
  if (!hasComputed && !hasTransforms) {
    return { ...edits, computedFile: null, movedExports: [], notes, todos };
  }
  const { text: support, exports: movedExports } = ctx.supportText();
  const lines: string[] = [
    "// The computed fields and transforms jazz.app.json declares, implemented",
    "// in TypeScript. Written by `jazz-tools upgrade` from schema.ts; edit freely.",
  ];
  const generatedImports = [
    hasComputed ? "AppComputed" : null,
    hasTransforms ? "AppTransforms" : null,
  ].filter(Boolean);
  lines.push(`import type { ${generatedImports.join(", ")} } from "./jazz.generated.js";`);
  if (support) lines.push(support.startsWith("import ") ? support : `\n${support}`);
  lines.push("");
  if (hasComputed) {
    lines.push("export const computed = {");
    for (const [table, fields] of Object.entries(computedEntries)) {
      lines.push(`  ${propertyKey(table)}: {`);
      for (const [field, text] of Object.entries(fields)) {
        lines.push(`    ${propertyKey(field)}: ${reindent(text, 4)},`);
      }
      lines.push("  },");
    }
    lines.push("} satisfies AppComputed;");
  }
  if (hasTransforms) {
    if (hasComputed) lines.push("");
    lines.push("export const transforms = {");
    for (const [name, text] of Object.entries(transformEntries)) {
      lines.push(text === name ? `  ${name},` : `  ${propertyKey(name)}: ${reindent(text, 2)},`);
    }
    lines.push("} satisfies AppTransforms;");
  }
  return { ...edits, computedFile: `${lines.join("\n")}\n`, movedExports, notes, todos };
}

/** A type without its `?` / `optional`. */
function required(spec: TypeSpec): TypeSpec {
  if (typeof spec === "string") return spec.replace(/\?$/, "");
  const { optional: _optional, ...rest } = spec;
  return rest;
}

function propertyKey(name: string): string {
  return /^[A-Za-z_$][\w$]*$/.test(name) ? name : JSON.stringify(name);
}

/** Re-indent a moved expression's continuation lines to `indent`. */
function reindent(text: string, indent: number): string {
  const lines = text.split("\n");
  if (lines.length === 1) return text;
  const rest = lines.slice(1).filter((l) => l.trim() !== "");
  // The least indented continuation line (the closing one) sits at the property's indentation.
  const min = Math.min(...rest.map((l) => l.match(/^ */)![0].length));
  return [
    lines[0],
    ...lines.slice(1).map((l) => (l.trim() === "" ? "" : " ".repeat(indent) + l.slice(min))),
  ].join("\n");
}

/** A number type's kind, from the expressions a callback returns. */
type NumberKind = "int" | "float" | "unknown";

const INT_MATH = new Set(["round", "floor", "ceil", "trunc", "sign", "clz32", "imul"]);
const FLOAT_MATH = new Set([
  "random",
  "sqrt",
  "cbrt",
  "log",
  "log2",
  "log10",
  "log1p",
  "exp",
  "expm1",
  "sin",
  "cos",
  "tan",
  "asin",
  "acos",
  "atan",
  "atan2",
  "sinh",
  "cosh",
  "tanh",
  "hypot",
  "pow",
  "fround",
]);

class Context {
  /** Top-level statements of schema.ts the moved callbacks need, in file order. */
  private readonly support = new Set<TS.Statement>();
  /** Per import declaration, the names of it the moved code uses. */
  private readonly importNames = new Map<TS.ImportDeclaration, Set<string>>();
  private readonly dslBindings: Set<TS.Symbol>;

  constructor(
    private readonly ts: Ts,
    private readonly checker: TS.TypeChecker,
    private readonly source: TS.SourceFile,
  ) {
    this.dslBindings = this.findDslBindings();
  }

  location(node: TS.Node): string {
    const { line } = this.source.getLineAndCharacterOfPosition(node.getStart(this.source));
    return `${relative(process.cwd(), this.source.fileName) || this.source.fileName}:${line + 1}`;
  }

  /** The local names `schema` from jazz-tools is bound to (`import { schema as s }`). */
  private findDslBindings(): Set<TS.Symbol> {
    const { ts } = this;
    const out = new Set<TS.Symbol>();
    for (const statement of this.source.statements) {
      if (!ts.isImportDeclaration(statement)) continue;
      if (!isJazzTools(statement.moduleSpecifier)) continue;
      const named = statement.importClause?.namedBindings;
      if (!named || !ts.isNamedImports(named)) continue;
      for (const element of named.elements) {
        if ((element.propertyName ?? element.name).text !== "schema") continue;
        const symbol = this.checker.getSymbolAtLocation(element.name);
        if (symbol) out.add(symbol);
      }
    }
    return out;
  }

  private isDsl(node: TS.Node): boolean {
    if (!this.ts.isIdentifier(node)) return false;
    const symbol = this.checker.getSymbolAtLocation(node);
    return symbol !== undefined && this.dslBindings.has(symbol);
  }

  /** `s.<member>(...)`, with `s` the DSL namespace (a .63 object or a .64 ES namespace). */
  isDslCall(node: TS.CallExpression, member?: string): boolean {
    const callee = node.expression;
    return (
      this.ts.isPropertyAccessExpression(callee) &&
      this.isDsl(callee.expression) &&
      (member === undefined || callee.name.text === member)
    );
  }

  /** `s.<builder>(...)….transform(...)`. */
  isTransformCall(node: TS.CallExpression): boolean {
    const { ts } = this;
    const callee = node.expression;
    if (!ts.isPropertyAccessExpression(callee) || callee.name.text !== "transform") return false;
    let inner: TS.Expression = callee.expression;
    while (true) {
      if (ts.isCallExpression(inner)) {
        if (this.isDslCall(inner)) return true;
        inner = inner.expression;
      } else if (ts.isPropertyAccessExpression(inner)) {
        inner = inner.expression;
      } else {
        return false;
      }
    }
  }

  /** The outermost expression of a builder chain `node` is part of. */
  private chainTop(node: TS.Expression): TS.Expression {
    const { ts } = this;
    let top: TS.Expression = node;
    while (
      (ts.isPropertyAccessExpression(top.parent) && top.parent.expression === top) ||
      (ts.isCallExpression(top.parent) && top.parent.expression === top)
    ) {
      top = top.parent as TS.Expression;
    }
    return top;
  }

  /**
   * The property an expression is the value of: directly (`name: expr`), or
   * through a `const name = expr` used as exactly one property value.
   */
  private propertyOf(
    expr: TS.Expression,
    what: string,
  ): { name: string; object: TS.ObjectLiteralExpression } {
    const { ts } = this;
    let node: TS.Node = expr;
    while (
      ts.isParenthesizedExpression(node.parent) ||
      ts.isAsExpression(node.parent) ||
      ts.isSatisfiesExpression(node.parent)
    ) {
      node = node.parent;
    }
    const parent = node.parent;
    if (ts.isPropertyAssignment(parent) && parent.initializer === node) {
      return {
        name: propertyName(ts, parent.name, () => this.location(parent)),
        object: parent.parent,
      };
    }
    if (
      ts.isVariableDeclaration(parent) &&
      parent.initializer === node &&
      ts.isIdentifier(parent.name)
    ) {
      const symbol = this.checker.getSymbolAtLocation(parent.name);
      const uses: Array<TS.PropertyAssignment | TS.ShorthandPropertyAssignment> = [];
      const visit = (n: TS.Node): void => {
        if (ts.isShorthandPropertyAssignment(n)) {
          if (this.checker.getShorthandAssignmentValueSymbol(n) === symbol) uses.push(n);
        } else if (
          ts.isPropertyAssignment(n) &&
          ts.isIdentifier(n.initializer) &&
          this.checker.getSymbolAtLocation(n.initializer) === symbol
        ) {
          uses.push(n);
        }
        ts.forEachChild(n, visit);
      };
      visit(this.source);
      if (uses.length === 1) {
        const use = uses[0]!;
        return { name: propertyName(ts, use.name, () => this.location(use)), object: use.parent };
      }
      throw notSupportedYet(
        `${what} \`${parent.name.text}\` used ${uses.length === 0 ? "nowhere as a property" : `as ${uses.length} properties`}`,
        this.location(parent),
        "upgrade needs each table and column used as exactly one property of the schema",
      );
    }
    throw notSupportedYet(
      `${what} that isn't a property of an object literal`,
      this.location(expr),
    );
  }

  /** Which table, and which field of it, a computed or transformed column call defines. */
  placeInTable(
    node: TS.CallExpression,
    role: "computed" | "column",
  ): { table: string; field: string } {
    const { ts } = this;
    const top = role === "column" ? this.chainTop(node) : node;
    const what = role === "computed" ? "an s.computed(...)" : "a transformed column";
    const field = this.propertyOf(top, what);
    const tableCall = field.object.parent;
    if (!ts.isCallExpression(tableCall) || !this.isDslCall(tableCall, "table")) {
      throw notSupportedYet(`${what} outside an s.table(...) call`, this.location(node));
    }
    const argIndex = tableCall.arguments.indexOf(field.object);
    if (role === "column" ? argIndex !== 0 : argIndex !== 1) {
      throw new UpgradeRefusal(
        `${what} must be in s.table's ${role === "column" ? "first (columns)" : "second (relations and computed)"} argument`,
        this.location(node),
      );
    }
    const table = this.propertyOf(this.chainTop(tableCall), "a table");
    return { table: table.name, field: field.name };
  }

  /** The function a callback argument is: inline, or a function of schema.ts it names. */
  functionOf(node: TS.Expression): TS.SignatureDeclaration & { body?: TS.ConciseBody } {
    const { ts } = this;
    if (ts.isArrowFunction(node) || ts.isFunctionExpression(node)) return node;
    if (ts.isIdentifier(node)) {
      const declaration = this.declarationOf(node);
      if (declaration && ts.isFunctionDeclaration(declaration)) return declaration;
      if (
        declaration &&
        ts.isVariableDeclaration(declaration) &&
        declaration.initializer &&
        (ts.isArrowFunction(declaration.initializer) ||
          ts.isFunctionExpression(declaration.initializer))
      ) {
        return declaration.initializer;
      }
    }
    throw notSupportedYet(
      "a computed callback that isn't a function written in schema.ts",
      this.location(node),
    );
  }

  methodOrFunction(prop: TS.ObjectLiteralElementLike): TS.SignatureDeclaration {
    const { ts } = this;
    if (ts.isMethodDeclaration(prop)) return prop;
    if (ts.isPropertyAssignment(prop)) return this.functionOf(prop.initializer);
    if (ts.isShorthandPropertyAssignment(prop)) return this.functionOf(prop.name);
    throw notSupportedYet("a transform's `from` that isn't a function", this.location(prop));
  }

  /** The declared type of `to`'s parameter, when the author wrote one. */
  firstParamOfTo(literal: TS.ObjectLiteralExpression): TS.Type | undefined {
    const { ts } = this;
    const to = literal.properties.find(
      (p) => p.name && ts.isIdentifier(p.name) && p.name.text === "to",
    );
    if (!to) return undefined;
    const fn = this.methodOrFunction(to);
    const param = fn.parameters[0];
    if (!param?.type) return undefined;
    return this.checker.getTypeFromTypeNode(param.type);
  }

  returnTypeOf(fn: TS.SignatureDeclaration): TS.Type {
    const signature = this.checker.getSignatureFromDeclaration(fn);
    if (!signature)
      throw new UpgradeRefusal("can't read a transform's `from` return type", this.location(fn));
    return this.checker.getReturnTypeOfSignature(signature);
  }

  objectLiteralOf(node: TS.Expression): TS.ObjectLiteralExpression {
    const { ts } = this;
    if (ts.isObjectLiteralExpression(node)) return node;
    if (ts.isIdentifier(node)) {
      const declaration = this.declarationOf(node);
      if (
        declaration &&
        ts.isVariableDeclaration(declaration) &&
        declaration.initializer &&
        ts.isObjectLiteralExpression(declaration.initializer)
      ) {
        return declaration.initializer;
      }
    }
    throw notSupportedYet(
      "a transform that isn't an object literal in schema.ts",
      this.location(node),
    );
  }

  /** A named transform keeps its name; an inline one is named for its column. */
  transformName(arg: TS.Expression, found: Found): string {
    if (this.ts.isIdentifier(arg)) return arg.text;
    return `${found.table}${found.field[0]!.toUpperCase()}${found.field.slice(1)}`;
  }

  private declarationOf(node: TS.Identifier): TS.Declaration | undefined {
    let symbol = this.checker.getSymbolAtLocation(node);
    if (!symbol) return undefined;
    if (symbol.flags & this.ts.SymbolFlags.Alias) return undefined; // imported: not ours to move
    const declaration = symbol.declarations?.[0];
    return declaration && declaration.getSourceFile() === this.source ? declaration : undefined;
  }

  /**
   * The columns a computed callback reads from its row: the properties of a
   * destructured parameter, or every `row.x` / `row["x"]` of a named one.
   */
  readColumns(
    fn: TS.SignatureDeclaration & { body?: TS.ConciseBody },
    table: JsonObject,
    marked: Record<string, string>,
    found: Found,
    notes: string[],
  ): string[] {
    const { ts } = this;
    const stored = Object.keys((table.columns as JsonObject | undefined) ?? {});
    const param = fn.parameters[0];
    if (!param) return [];
    if (fn.parameters.length > 1) {
      throw new UpgradeRefusal(
        "a computed callback takes one parameter, the row",
        this.location(fn),
      );
    }
    let names: string[] | undefined = [];
    if (ts.isObjectBindingPattern(param.name)) {
      for (const element of param.name.elements) {
        if (element.dotDotDotToken) {
          names = undefined;
          break;
        }
        const key = element.propertyName ?? element.name;
        if (ts.isIdentifier(key) || ts.isStringLiteral(key)) names.push(key.text);
        else {
          names = undefined;
          break;
        }
      }
    } else if (ts.isIdentifier(param.name)) {
      const symbol = this.checker.getSymbolAtLocation(param.name);
      const visit = (node: TS.Node): void => {
        if (names === undefined) return;
        if (
          ts.isIdentifier(node) &&
          node !== param.name &&
          this.checker.getSymbolAtLocation(node) === symbol
        ) {
          const parent = node.parent;
          if (ts.isPropertyAccessExpression(parent) && parent.expression === node) {
            names.push(parent.name.text);
          } else if (
            ts.isElementAccessExpression(parent) &&
            parent.expression === node &&
            ts.isStringLiteralLike(parent.argumentExpression)
          ) {
            names.push(parent.argumentExpression.text);
          } else {
            names = undefined;
          }
          return;
        }
        ts.forEachChild(node, visit);
      };
      if (fn.body) visit(fn.body);
    } else {
      names = undefined;
    }
    if (names === undefined) {
      notes.push(
        `${found.table}.${found.field}: the callback uses its whole row, so \`from\` lists every stored column; narrow it in jazz.app.json to the ones it reads.`,
      );
      return stored;
    }
    const out: string[] = [];
    for (const name of names) {
      if (out.includes(name)) continue;
      if (name !== "id" && !stored.includes(name)) {
        if (marked[name] === "computed") {
          throw notSupportedYet(
            `a computed field reading another computed field (${found.table}.${found.field} reads ${name})`,
            this.location(fn),
            "computed fields are computed from stored columns only; inline the other field's computation",
          );
        }
        throw notSupportedYet(
          `a computed field reading \`${name}\`, which isn't a stored column of ${found.table}`,
          this.location(fn),
        );
      }
      out.push(name);
    }
    // In the file's column order, as the generated `from` type lists them.
    return out.sort((a, b) => order(a) - order(b));
    function order(name: string): number {
      return name === "id" ? -1 : stored.indexOf(name);
    }
  }

  /** The jazz.app.json type of a TS type, or json with a TODO when it can't tell. */
  typeSpec(
    type: TS.Type,
    fn: TS.SignatureDeclaration & { body?: TS.ConciseBody },
    what: string,
    table: JsonObject | undefined,
  ): { spec: TypeSpec; todo?: boolean; note?: string } {
    const { ts, checker } = this;
    let nullable = false;
    let members = type.isUnion() ? type.types : [type];
    const nonNull = members.filter(
      (t) => !(t.flags & (ts.TypeFlags.Null | ts.TypeFlags.Undefined | ts.TypeFlags.Void)),
    );
    if (nonNull.length !== members.length) nullable = true;
    members = nonNull;
    const optional = (spec: TypeSpec): TypeSpec =>
      !nullable ? spec : typeof spec === "string" ? `${spec}?` : { ...spec, optional: true };
    const todo = (reason: string, schema?: Json) => ({
      spec: optional({ json: schema ?? `TODO: ${reason}` }),
      todo: schema === undefined,
    });
    if (members.length === 0) return todo(`${what} returns only null/undefined; give it a type`);
    if (members.every((t) => t.flags & ts.TypeFlags.StringLiteral)) {
      const variants = members.map((t) => (t as TS.StringLiteralType).value);
      return { spec: optional({ enum: variants }) };
    }
    if (members.every((t) => t.flags & ts.TypeFlags.BooleanLike))
      return { spec: optional("boolean") };
    if (members.every((t) => t.flags & ts.TypeFlags.StringLike))
      return { spec: optional("string") };
    if (members.every((t) => t.flags & ts.TypeFlags.BigIntLike))
      return { spec: optional("bigint") };
    if (members.length === 1 && checker.typeToString(members[0]!) === "Date")
      return { spec: optional("timestamp") };
    if (members.length === 1 && checker.typeToString(members[0]!) === "Uint8Array")
      return { spec: optional("bytes") };
    if (members.every((t) => t.flags & ts.TypeFlags.NumberLike)) {
      if (members.every((t) => t.flags & ts.TypeFlags.NumberLiteral)) {
        const values = members.map((t) => (t as TS.NumberLiteralType).value);
        return { spec: optional(values.every(Number.isInteger) ? "int" : "float") };
      }
      const kind = this.numberKind(fn, table);
      if (kind !== "unknown") return { spec: optional(kind) };
      return {
        ...todo(`${what} returns a number; make this "int" or "float"`),
        note: `${what}: upgrade couldn't tell whether the number is an int or a float; resolve the TODO in jazz.app.json.`,
      };
    }
    const schema = this.jsonSchema(type, new Set());
    if (schema !== undefined) {
      return {
        spec: { json: schema },
        note: `${what}: typed as json with a JSON Schema read from its TypeScript type; check it in jazz.app.json.`,
      };
    }
    return {
      ...todo(`${what} returns ${checker.typeToString(type)}; give it a type`),
      note: `${what}: upgrade can't express ${checker.typeToString(type)} as a jazz.app.json type; resolve the TODO.`,
    };
  }

  /** A JSON Schema for a JSON-shaped TS type (objects, arrays, literals), or undefined. */
  private jsonSchema(type: TS.Type, seen: Set<TS.Type>): Json | undefined {
    const { ts, checker } = this;
    if (seen.has(type)) return undefined;
    const f = type.flags;
    if (f & ts.TypeFlags.StringLiteral) return { const: (type as TS.StringLiteralType).value };
    if (f & ts.TypeFlags.NumberLiteral) return { const: (type as TS.NumberLiteralType).value };
    if (f & ts.TypeFlags.BooleanLiteral) return { const: checker.typeToString(type) === "true" };
    if (f & ts.TypeFlags.String) return { type: "string" };
    if (f & ts.TypeFlags.Number) return { type: "number" };
    if (f & ts.TypeFlags.Boolean) return { type: "boolean" };
    if (f & ts.TypeFlags.Null) return { type: "null" };
    if (type.isUnion()) {
      if (type.types.every((t) => t.flags & ts.TypeFlags.BooleanLiteral))
        return { type: "boolean" };
      const options = type.types
        .filter((t) => !(t.flags & ts.TypeFlags.Undefined))
        .map((t) => this.jsonSchema(t, seen));
      if (options.some((o) => o === undefined)) return undefined;
      return { anyOf: options as Json[] };
    }
    if (checker.isArrayType(type)) {
      const [element] = checker.getTypeArguments(type as TS.TypeReference);
      const items = element && this.jsonSchema(element, new Set([...seen, type]));
      return items === undefined ? undefined : { type: "array", items };
    }
    if (f & ts.TypeFlags.Object) {
      if (checker.getSignaturesOfType(type, ts.SignatureKind.Call).length > 0) return undefined;
      const name = checker.typeToString(type);
      if (name === "Date" || name === "Uint8Array" || name === "Map" || name === "Set")
        return undefined;
      const properties: JsonObject = {};
      const required: Json[] = [];
      for (const property of checker.getPropertiesOfType(type)) {
        const declaration = property.valueDeclaration ?? property.declarations?.[0];
        if (!declaration) return undefined;
        const propertyType = checker.getTypeOfSymbolAtLocation(property, declaration);
        const schema = this.jsonSchema(propertyType, new Set([...seen, type]));
        if (schema === undefined) return undefined;
        properties[property.name] = schema;
        if (!(property.flags & ts.SymbolFlags.Optional)) required.push(property.name);
      }
      return { type: "object", properties, required, additionalProperties: false };
    }
    return undefined;
  }

  /** Whether a number-returning callback returns ints or floats, from its return expressions. */
  private numberKind(
    fn: TS.SignatureDeclaration & { body?: TS.ConciseBody },
    table: JsonObject | undefined,
  ): NumberKind {
    const { ts } = this;
    if (!fn.body) return "unknown";
    const returns: TS.Expression[] = [];
    if (!ts.isBlock(fn.body)) returns.push(fn.body);
    else {
      const visit = (node: TS.Node): void => {
        if (ts.isFunctionLike(node)) return;
        if (ts.isReturnStatement(node) && node.expression) returns.push(node.expression);
        ts.forEachChild(node, visit);
      };
      visit(fn.body);
    }
    if (returns.length === 0) return "unknown";
    const columns = (table?.columns as JsonObject | undefined) ?? {};
    const param = fn.parameters[0];
    const paramSymbol =
      param && ts.isIdentifier(param.name)
        ? this.checker.getSymbolAtLocation(param.name)
        : undefined;
    const bound = new Map<TS.Symbol, string>();
    if (param && ts.isObjectBindingPattern(param.name)) {
      for (const element of param.name.elements) {
        const key = element.propertyName ?? element.name;
        const symbol = ts.isIdentifier(element.name)
          ? this.checker.getSymbolAtLocation(element.name)
          : undefined;
        if (symbol && (ts.isIdentifier(key) || ts.isStringLiteral(key)))
          bound.set(symbol, key.text);
      }
    }
    const columnKind = (name: string): NumberKind => {
      const spec = columns[name];
      if (spec === undefined) return "unknown";
      const base = columnType(spec).type;
      return base === "int" ? "int" : base === "float" ? "float" : "unknown";
    };
    const kindOf = (e: TS.Expression): NumberKind => {
      if (ts.isParenthesizedExpression(e) || ts.isAsExpression(e) || ts.isNonNullExpression(e)) {
        return kindOf(e.expression);
      }
      if (ts.isNumericLiteral(e))
        return Number.isInteger(Number(e.text)) && !/[.eE]/.test(e.text) ? "int" : "float";
      if (ts.isPrefixUnaryExpression(e)) {
        if (e.operator === ts.SyntaxKind.MinusToken || e.operator === ts.SyntaxKind.PlusToken)
          return kindOf(e.operand);
        if (e.operator === ts.SyntaxKind.TildeToken) return "int";
        return "unknown";
      }
      if (ts.isConditionalExpression(e)) return join(kindOf(e.whenTrue), kindOf(e.whenFalse));
      if (ts.isBinaryExpression(e)) {
        const op = e.operatorToken.kind;
        if (op === ts.SyntaxKind.SlashToken || op === ts.SyntaxKind.AsteriskAsteriskToken)
          return "float";
        if (
          op === ts.SyntaxKind.BarToken ||
          op === ts.SyntaxKind.AmpersandToken ||
          op === ts.SyntaxKind.CaretToken ||
          op === ts.SyntaxKind.LessThanLessThanToken ||
          op === ts.SyntaxKind.GreaterThanGreaterThanToken ||
          op === ts.SyntaxKind.GreaterThanGreaterThanGreaterThanToken
        ) {
          return "int";
        }
        if (
          op === ts.SyntaxKind.PlusToken ||
          op === ts.SyntaxKind.MinusToken ||
          op === ts.SyntaxKind.AsteriskToken ||
          op === ts.SyntaxKind.PercentToken
        ) {
          return arith(kindOf(e.left), kindOf(e.right));
        }
        if (op === ts.SyntaxKind.QuestionQuestionToken || op === ts.SyntaxKind.BarBarToken) {
          return join(kindOf(e.left), kindOf(e.right));
        }
        return "unknown";
      }
      if (ts.isPropertyAccessExpression(e)) {
        if (e.name.text === "length") return "int";
        if (
          paramSymbol &&
          ts.isIdentifier(e.expression) &&
          this.checker.getSymbolAtLocation(e.expression) === paramSymbol
        ) {
          return columnKind(e.name.text);
        }
        return "unknown";
      }
      if (ts.isIdentifier(e)) {
        const symbol = this.checker.getSymbolAtLocation(e);
        const column = symbol && bound.get(symbol);
        return column ? columnKind(column) : "unknown";
      }
      if (ts.isCallExpression(e)) {
        const callee = e.expression;
        if (ts.isPropertyAccessExpression(callee) && ts.isIdentifier(callee.expression)) {
          const object = callee.expression.text;
          const method = callee.name.text;
          if (object === "Math") {
            if (INT_MATH.has(method)) return "int";
            if (FLOAT_MATH.has(method)) return "float";
            if (method === "abs") return e.arguments[0] ? kindOf(e.arguments[0]) : "unknown";
            if (method === "min" || method === "max") {
              return e.arguments
                .map(kindOf)
                .reduce<NumberKind>(
                  (a, b) => join(a, b),
                  e.arguments.length ? kindOf(e.arguments[0]!) : "unknown",
                );
            }
          }
          if (object === "Number" && method === "parseInt") return "int";
          if (object === "Number" && method === "parseFloat") return "float";
          if (method === "getTime" || (method === "valueOf" && false)) return "int";
          if (
            method === "indexOf" ||
            method === "lastIndexOf" ||
            method === "findIndex" ||
            method === "charCodeAt" ||
            method === "getFullYear" ||
            method === "getMonth" ||
            method === "getDate" ||
            method === "getDay" ||
            method === "getHours" ||
            method === "getMinutes" ||
            method === "getSeconds" ||
            method === "getUTCFullYear" ||
            method === "getUTCMonth" ||
            method === "getUTCDate" ||
            method === "getUTCDay" ||
            method === "getUTCHours"
          ) {
            return "int";
          }
        }
        if (ts.isIdentifier(callee) && callee.text === "parseInt") return "int";
        if (ts.isIdentifier(callee) && callee.text === "parseFloat") return "float";
        return "unknown";
      }
      return "unknown";
    };
    return returns.map(kindOf).reduce(join);
  }

  /** Mark a moved node's top-level dependencies in schema.ts for jazz.computed.ts. */
  move(node: TS.Node): void {
    const { ts } = this;
    const visit = (n: TS.Node): void => {
      if (ts.isIdentifier(n) && !isPropertyNameOnly(ts, n)) this.require(n, node);
      ts.forEachChild(n, visit);
    };
    visit(node);
    // A callback named by identifier moves with its declaration.
    if (ts.isIdentifier(node)) this.require(node, node);
  }

  private require(identifier: TS.Identifier, within: TS.Node): void {
    const { ts } = this;
    const symbol = this.checker.getSymbolAtLocation(identifier);
    if (!symbol) return;
    if (this.dslBindings.has(symbol)) {
      throw notSupportedYet(
        "a computed field or transform that uses the schema DSL itself",
        this.location(identifier),
        "jazz.computed.ts can't import the removed DSL; use plain TypeScript in the callback",
      );
    }
    for (const declaration of symbol.declarations ?? []) {
      if (declaration.getSourceFile() !== this.source) continue;
      if (declaration.pos >= within.pos && declaration.end <= within.end) continue;
      const statement = topLevelStatement(ts, declaration);
      if (!statement) continue;
      if (ts.isImportDeclaration(statement)) {
        const names = this.importNames.get(statement) ?? new Set<string>();
        names.add(identifier.text);
        this.importNames.set(statement, names);
        continue;
      }
      if (this.support.has(statement)) continue;
      this.support.add(statement);
      const visit = (n: TS.Node): void => {
        if (ts.isIdentifier(n) && !isPropertyNameOnly(ts, n)) this.require(n, statement);
        ts.forEachChild(n, visit);
      };
      visit(statement);
    }
  }

  /** The imports and top-level declarations moved code needs, and which of them schema.ts exported. */
  supportText(): { text: string; exports: string[] } {
    const { ts } = this;
    const lines: string[] = [];
    for (const [statement, names] of this.importNames) {
      const specifier = (statement.moduleSpecifier as TS.StringLiteral).text;
      if (isJazzTools(statement.moduleSpecifier) && names.has("schema")) continue;
      const clause = statement.importClause;
      const parts: string[] = [];
      let defaultName: string | undefined;
      let namespace: string | undefined;
      if (clause?.name && names.has(clause.name.text)) defaultName = clause.name.text;
      const bindings = clause?.namedBindings;
      if (bindings && ts.isNamespaceImport(bindings) && names.has(bindings.name.text)) {
        namespace = bindings.name.text;
      }
      if (bindings && ts.isNamedImports(bindings)) {
        for (const element of bindings.elements) {
          if (!names.has(element.name.text)) continue;
          const typeOnly = clause!.isTypeOnly || element.isTypeOnly;
          const name = element.propertyName
            ? `${element.propertyName.text} as ${element.name.text}`
            : element.name.text;
          parts.push(typeOnly && !clause!.isTypeOnly ? `type ${name}` : name);
        }
      }
      const typePrefix = clause?.isTypeOnly ? "type " : "";
      const quoted = JSON.stringify(specifier);
      if (namespace) lines.push(`import ${typePrefix}* as ${namespace} from ${quoted};`);
      if (defaultName || parts.length > 0) {
        const head = [defaultName, parts.length > 0 ? `{ ${parts.join(", ")} }` : undefined]
          .filter(Boolean)
          .join(", ");
        lines.push(`import ${typePrefix}${head} from ${quoted};`);
      }
    }
    const exports: string[] = [];
    const statements = [...this.support].sort((a, b) => a.pos - b.pos);
    if (statements.length > 0 && lines.length > 0) lines.push("");
    for (const statement of statements) {
      const modifiers = ts.canHaveModifiers(statement) ? ts.getModifiers(statement) : undefined;
      if (modifiers?.some((m) => m.kind === ts.SyntaxKind.ExportKeyword)) {
        exports.push(...declaredNames(ts, statement));
      }
      lines.push(
        `${statement
          .getFullText(this.source)
          .replace(/^\s*\n/, "")
          .replace(/^\n+/, "")}\n`,
      );
    }
    return { text: lines.join("\n").replace(/\n+$/, ""), exports };
  }
}

function join(a: NumberKind, b: NumberKind): NumberKind {
  if (a === "unknown" || b === "unknown") return "unknown";
  return a === b ? a : "float";
}

function arith(a: NumberKind, b: NumberKind): NumberKind {
  if (a === "float" || b === "float") return "float";
  if (a === "int" && b === "int") return "int";
  return "unknown";
}

function isJazzTools(specifier: TS.Expression): boolean {
  const text = (specifier as TS.StringLiteral).text;
  return text === "jazz-tools" || text?.startsWith("jazz-tools/");
}

function propertyName(ts: Ts, name: TS.PropertyName, where: () => string): string {
  if (ts.isIdentifier(name) || ts.isStringLiteral(name) || ts.isNumericLiteral(name))
    return name.text;
  throw notSupportedYet("a computed property name in the schema", where());
}

/** An identifier that only names a property (`row.x`'s `x`, `{ x: 1 }`'s `x`), not a binding. */
function isPropertyNameOnly(ts: Ts, node: TS.Identifier): boolean {
  const parent = node.parent;
  if (ts.isPropertyAccessExpression(parent) && parent.name === node) return true;
  if (
    (ts.isPropertyAssignment(parent) ||
      ts.isMethodDeclaration(parent) ||
      ts.isPropertySignature(parent)) &&
    parent.name === node
  ) {
    return true;
  }
  if (ts.isQualifiedName(parent) && parent.right === node) return true;
  return false;
}

function topLevelStatement(ts: Ts, node: TS.Node): TS.Statement | undefined {
  let current: TS.Node = node;
  while (current.parent && !ts.isSourceFile(current.parent)) current = current.parent;
  return current.parent && ts.isSourceFile(current.parent) ? (current as TS.Statement) : undefined;
}

function declaredNames(ts: Ts, statement: TS.Statement): string[] {
  if (ts.isVariableStatement(statement)) {
    return statement.declarationList.declarations.flatMap((d) =>
      ts.isIdentifier(d.name) ? [d.name.text] : [],
    );
  }
  const named = statement as TS.Statement & { name?: TS.Identifier };
  return named.name && ts.isIdentifier(named.name) ? [named.name.text] : [];
}

/** For messages: a path shown relative to the project. */
export function shown(path: string, root: string): string {
  return relative(root, path) || basename(path) || dirname(path);
}
