/**
 * The named values the old permissions used (`invitableRoles`,
 * `ANNOUNCEMENTS_CHAT_ID`), so the printed permissions keep the names rather
 * than the literals jazz.permissions.json holds. A constant counts when the
 * old permissions.ts (or schema.ts's inline permissions) names it and its
 * value is a plain literal: a string, number, boolean, or an array of them.
 */
import { existsSync, readFileSync } from "node:fs";
import { dirname, relative } from "node:path";
import type * as TS from "typescript";
import { jsLiteral } from "../printer/names.js";

type Ts = typeof TS;

export interface NamedConstant {
  /** The identifier the old permissions used. */
  name: string;
  /** The module that exports it, when imported; undefined when declared in the old file itself. */
  file?: string;
  /** The exported name, when imported (`import { roles as r }`). */
  exported?: string;
  /** Its declaration, when declared in the old file (which upgrade deletes). */
  declaration?: string;
}

export interface Constants {
  /** A value's TS literal → its identifier. */
  byValue: Map<string, string>;
  byName: Map<string, NamedConstant>;
}

export function collectConstants(
  ts: Ts,
  file: string,
  resolve: (specifier: string, from: string) => string | undefined,
): Constants {
  const byValue = new Map<string, string>();
  const byName = new Map<string, NamedConstant>();
  if (!existsSync(file)) return { byValue, byName };
  const source = parse(ts, file);
  const used = new Set<string>();
  const visit = (node: TS.Node) => {
    if (ts.isIdentifier(node)) used.add(node.text);
    ts.forEachChild(node, visit);
  };
  for (const statement of source.statements) if (!ts.isImportDeclaration(statement)) visit(statement);

  const add = (name: string, value: unknown, constant: NamedConstant) => {
    const text = jsLiteral(value);
    // Two names for one value: neither, as the printer couldn't tell which was meant.
    if (byValue.has(text) && byValue.get(text) !== name) byValue.set(text, "");
    else byValue.set(text, name);
    byName.set(name, constant);
  };
  for (const statement of source.statements) {
    if (ts.isImportDeclaration(statement) && ts.isStringLiteral(statement.moduleSpecifier)) {
      const named = statement.importClause?.namedBindings;
      if (!named || !ts.isNamedImports(named) || statement.importClause?.isTypeOnly) continue;
      const target = resolve(statement.moduleSpecifier.text, file);
      if (!target) continue;
      const exports = literalConstants(ts, parse(ts, target), true);
      for (const element of named.elements) {
        const local = element.name.text;
        const exported = (element.propertyName ?? element.name).text;
        if (!used.has(local) || !exports.has(exported)) continue;
        add(local, exports.get(exported)!.value, { name: local, file: target, exported });
      }
    } else if (ts.isVariableStatement(statement)) {
      for (const [name, { value, text }] of literalConstants(ts, source, false, statement)) {
        if (used.has(name)) add(name, value, { name, declaration: text });
      }
    }
  }
  for (const [text, name] of byValue) if (name === "") byValue.delete(text);
  return { byValue, byName };
}

function parse(ts: Ts, file: string): TS.SourceFile {
  return ts.createSourceFile(file, readFileSync(file, "utf8"), ts.ScriptTarget.Latest, true);
}

/** `const NAME = <literal>` declarations (exported ones only, with `exportedOnly`). */
function literalConstants(
  ts: Ts,
  source: TS.SourceFile,
  exportedOnly: boolean,
  only?: TS.VariableStatement,
): Map<string, { value: unknown; text: string }> {
  const out = new Map<string, { value: unknown; text: string }>();
  for (const statement of only ? [only] : source.statements) {
    if (!ts.isVariableStatement(statement)) continue;
    if (!(statement.declarationList.flags & ts.NodeFlags.Const)) continue;
    const exported = (ts.getModifiers(statement) ?? []).some((m) => m.kind === ts.SyntaxKind.ExportKeyword);
    if (exportedOnly && !exported) continue;
    for (const declaration of statement.declarationList.declarations) {
      if (!ts.isIdentifier(declaration.name) || !declaration.initializer) continue;
      const value = literal(ts, declaration.initializer);
      if (value === undefined) continue;
      out.set(declaration.name.text, {
        value,
        text: `const ${declaration.getText(source).replace(/^export\s+/, "")};`,
      });
    }
  }
  return out;
}

function literal(ts: Ts, node: TS.Expression): unknown {
  while (ts.isAsExpression(node) || ts.isSatisfiesExpression(node) || ts.isParenthesizedExpression(node)) {
    node = node.expression;
  }
  if (ts.isStringLiteral(node) || ts.isNoSubstitutionTemplateLiteral(node)) return node.text;
  if (ts.isNumericLiteral(node)) return Number(node.text);
  if (node.kind === ts.SyntaxKind.TrueKeyword) return true;
  if (node.kind === ts.SyntaxKind.FalseKeyword) return false;
  if (ts.isArrayLiteralExpression(node)) {
    const items = node.elements.map((e) => literal(ts, e as TS.Expression));
    if (items.some((i) => i === undefined || (typeof i === "object" && i !== null))) return undefined;
    return items;
  }
  return undefined;
}

/** The import lines (or local declarations) for the constants `names`, as seen from `file`. */
export function constantImports(
  constants: Constants,
  names: Iterable<string>,
  file: string,
  jsExtensions: boolean,
): { imports: string[]; declarations: string[] } {
  const byModule = new Map<string, string[]>();
  const declarations: string[] = [];
  for (const name of names) {
    const constant = constants.byName.get(name);
    if (!constant) continue;
    if (constant.declaration) {
      declarations.push(constant.declaration);
      continue;
    }
    let specifier = relative(dirname(file), constant.file!).split("\\").join("/");
    specifier = specifier.replace(/\.(ts|tsx|mts|cts|js|mjs)$/, jsExtensions ? ".js" : "");
    if (!specifier.startsWith(".")) specifier = `./${specifier}`;
    const element = constant.exported && constant.exported !== name ? `${constant.exported} as ${name}` : name;
    byModule.set(specifier, [...(byModule.get(specifier) ?? []), element]);
  }
  const imports = [...byModule].map(([spec, elements]) => `import { ${elements.sort().join(", ")} } from ${JSON.stringify(spec)};`);
  return { imports, declarations };
}
