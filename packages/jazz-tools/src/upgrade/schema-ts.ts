/**
 * The alpha.65 → alpha.66 `schema.ts` codemod, as text edits that keep the
 * author's comments and layout:
 *
 * ```ts
 * const schema = { todos: s.table(…) };                 // →  export const schema = s.defineSchema({ todos: s.table(…) });
 * type AppSchema = s.Schema<typeof schema>;              // →  (gone)
 * export const app = s.defineApp(schema, { migrations }); // →  (gone: `app` is in jazz.generated.ts)
 * export type Todo = s.RowOf<typeof app.todos>;          // →  (gone: jazz.generated.ts declares it)
 * export const STATUSES = ["a", "b"] as const;           // →  in app-extras.ts, imported back when used
 * ```
 *
 * plus the permissions, printed from `jazz.permissions.json`. A module whose
 * tables the schema spreads (Better Auth's `schema-better-auth/schema.ts`)
 * now exports `tables` and `permissions`.
 */
import { relative } from "node:path";
import type * as TS from "typescript";
import { applyEdits, type TextEdit } from "./edits.js";
import { notSupportedYet } from "./errors.js";
import type { MovedExport } from "./extras.js";

type Ts = typeof TS;

export interface SpreadModule {
  /** The module's file. */
  file: string;
  /** The specifier schema.ts imports it by. */
  specifier: string;
  /** The local name schema.ts spreads. */
  local: string;
}

export interface SchemaCodemodInput {
  ts: Ts;
  file: string;
  text: string;
  /** Where each export of schema.ts lives now (from `convertExtras`). */
  exports: Map<string, MovedExport>;
  /** The specifier of app-extras.ts from schema.ts (`./app-extras`). */
  extrasSpecifier: string;
  /** Resolve a relative import of schema.ts to a file. */
  resolve: (specifier: string) => string | undefined;
  root: string;
}

export interface SchemaCodemod {
  /** The edited text, without the permissions. */
  text: string;
  /** Modules whose tables the schema spreads. */
  spreads: SpreadModule[];
  /** Whether schema.ts declared permissions itself (`s.definePermissions(app, …)`). */
  inlinePermissions: boolean;
  changes: string[];
}

export function codemodSchema(input: SchemaCodemodInput): SchemaCodemod {
  const { ts, file, text } = input;
  const source = ts.createSourceFile(file, text, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
  const where = (node: TS.Node) =>
    `${relative(input.root, file) || file}:${source.getLineAndCharacterOfPosition(node.getStart(source)).line + 1}`;
  const edits: TextEdit[] = [];
  const changes: string[] = [];
  const removed = new Set<TS.Node>();
  const removeStatement = (statement: TS.Statement) => {
    removed.add(statement);
    // The statement and its line break; its leading comments go with it.
    const start = statement.getFullStart();
    let end = statement.getEnd();
    while (text[end] === " " || text[end] === "\t") end++;
    if (text[end] === "\n") end++;
    const leading = text.slice(start, statement.getStart(source));
    edits.push({
      start: start + leading.length - leading.replace(/^\s*\n/, "").length,
      end,
      text: "",
    });
  };

  // The DSL namespace: `import { schema as s } from "jazz-tools"`.
  let dsl: string | undefined;
  for (const statement of source.statements) {
    if (!ts.isImportDeclaration(statement) || !ts.isStringLiteral(statement.moduleSpecifier))
      continue;
    if (statement.moduleSpecifier.text !== "jazz-tools") continue;
    const named = statement.importClause?.namedBindings;
    if (named && ts.isNamedImports(named)) {
      for (const element of named.elements) {
        if ((element.propertyName ?? element.name).text === "schema") dsl = element.name.text;
      }
    }
  }
  if (!dsl)
    throw notSupportedYet(
      "a schema.ts that doesn't import `schema` from jazz-tools",
      where(source),
    );
  const isDslCall = (node: TS.Node | undefined, names: string[]): node is TS.CallExpression =>
    node !== undefined &&
    ts.isCallExpression(node) &&
    ts.isPropertyAccessExpression(node.expression) &&
    ts.isIdentifier(node.expression.expression) &&
    node.expression.expression.text === dsl &&
    names.includes(node.expression.name.text);
  const unwrap = (node: TS.Expression): TS.Expression => {
    let current = node;
    while (
      ts.isAsExpression(current) ||
      ts.isSatisfiesExpression(current) ||
      ts.isParenthesizedExpression(current)
    )
      current = current.expression;
    return current;
  };

  // The app and the tables object it is defined from.
  let appCall: TS.CallExpression | undefined;
  let appStatement: TS.VariableStatement | undefined;
  let inlinePermissions = false;
  for (const statement of source.statements) {
    if (!ts.isVariableStatement(statement)) continue;
    for (const declaration of statement.declarationList.declarations) {
      const init = declaration.initializer && unwrap(declaration.initializer);
      if (isDslCall(init, ["defineApp", "defineSliceableApp"])) {
        if (appCall) throw notSupportedYet("a schema.ts that defines two apps", where(declaration));
        appCall = init;
        appStatement = statement;
      }
      if (isDslCall(init, ["definePermissions"])) {
        inlinePermissions = true;
        removeStatement(statement);
        changes.push(`${where(statement)}: the permissions are printed from jazz.permissions.json`);
      }
    }
  }
  if (!appCall || !appStatement) {
    throw notSupportedYet(
      "a schema.ts without `s.defineApp(...)`",
      where(source.statements[0] ?? source),
    );
  }
  if (appStatement.declarationList.declarations.length > 1) {
    throw notSupportedYet(
      "declaring other names in the same statement as the app",
      where(appStatement),
    );
  }
  const tablesArg = appCall.arguments[0] && unwrap(appCall.arguments[0]);
  let tablesObject: TS.ObjectLiteralExpression | undefined;
  let tablesStatement: TS.VariableStatement | undefined;
  let tablesName: string | undefined;
  if (tablesArg && ts.isObjectLiteralExpression(tablesArg)) {
    tablesObject = tablesArg;
  } else if (tablesArg && ts.isIdentifier(tablesArg)) {
    tablesName = tablesArg.text;
    for (const statement of source.statements) {
      if (!ts.isVariableStatement(statement)) continue;
      for (const declaration of statement.declarationList.declarations) {
        if (!ts.isIdentifier(declaration.name) || declaration.name.text !== tablesName) continue;
        const init = declaration.initializer && unwrap(declaration.initializer);
        if (init && ts.isObjectLiteralExpression(init)) {
          tablesObject = init;
          tablesStatement = statement;
        }
      }
    }
  }
  if (!tablesObject) {
    throw notSupportedYet(
      "an app whose tables aren't an object literal in schema.ts",
      where(appCall),
    );
  }
  if (tablesStatement && tablesStatement.declarationList.declarations.length > 1) {
    throw notSupportedYet(
      "declaring other names in the same statement as the tables",
      where(tablesStatement),
    );
  }

  // `schema` must be free for the new export.
  const declared = new Set<string>();
  const visitNames = (node: TS.Node) => {
    if (
      (ts.isVariableDeclaration(node) ||
        ts.isFunctionDeclaration(node) ||
        ts.isClassDeclaration(node) ||
        ts.isImportSpecifier(node) ||
        ts.isNamespaceImport(node)) &&
      node.name &&
      ts.isIdentifier(node.name)
    )
      declared.add(node.name.text);
    ts.forEachChild(node, visitNames);
  };
  visitNames(source);
  if (tablesName !== "schema" && declared.has("schema")) {
    throw notSupportedYet(
      "a schema.ts that declares another `schema`",
      where(source),
      "rename it, then run upgrade again",
    );
  }

  // 1. `const schema = {…}` → `export const schema = s.defineSchema({…})`.
  if (tablesStatement) {
    edits.push({
      start: tablesStatement.getStart(source),
      end: tablesObject.getStart(source),
      text: `export const schema = ${dsl}.defineSchema(`,
    });
    const after = tablesStatement.declarationList.declarations[0]!.initializer!;
    edits.push({ start: tablesObject.getEnd(), end: after.getEnd(), text: ")" });
    removeStatement(appStatement);
    removed.add(appStatement);
  } else {
    // `export const app = s.defineApp({…}, …)`: the call becomes the schema.
    edits.push({
      start: appStatement.getStart(source),
      end: tablesObject.getStart(source),
      text: `export const schema = ${dsl}.defineSchema(`,
    });
    edits.push({ start: tablesObject.getEnd(), end: appStatement.getEnd(), text: ");" });
  }
  changes.push(
    `${where(appCall)}: \`${dsl}.defineApp(…)\` → \`export const schema = ${dsl}.defineSchema({…})\``,
  );

  // 2. What moved out: `type AppSchema = s.Schema<…>`, row types, extras; `migrations` imports.
  const extrasUsed = new Set<string>();
  const movedToExtras = new Set(
    [...input.exports].filter(([, m]) => m.module === "extras").map(([name]) => name),
  );
  for (const statement of source.statements) {
    if (statement === tablesStatement || removed.has(statement)) continue;
    if (ts.isTypeAliasDeclaration(statement)) {
      const type = statement.type;
      if (
        ts.isTypeReferenceNode(type) &&
        ts.isQualifiedName(type.typeName) &&
        ts.isIdentifier(type.typeName.left) &&
        type.typeName.left.text === dsl &&
        ["Schema", "App", "SliceableApp"].includes(type.typeName.right.text) &&
        !isExportedNode(ts, statement)
      ) {
        removeStatement(statement);
        removed.add(statement);
        continue;
      }
    }
    if (ts.isImportDeclaration(statement) && ts.isStringLiteral(statement.moduleSpecifier)) {
      const target = input.resolve(statement.moduleSpecifier.text);
      if (target && /[\\/]migrations[\\/]index\.[cm]?[jt]s$/.test(target)) {
        removeStatement(statement);
        removed.add(statement);
        changes.push(`${where(statement)}: dropped the import of migrations/index.ts`);
      }
      continue;
    }
    const names = declaredNamesOf(ts, statement);
    if (
      names.length > 0 &&
      isExportedNode(ts, statement) &&
      names.every((n) => input.exports.has(n))
    ) {
      if (names.some((n) => input.exports.get(n)!.module === "computed")) {
        throw notSupportedYet("computed columns", where(statement));
      }
      removeStatement(statement);
      removed.add(statement);
    }
  }
  // Names that moved to app-extras.ts but the schema still uses.
  const visitUses = (node: TS.Node) => {
    if (removed.has(node)) return;
    if (ts.isIdentifier(node) && movedToExtras.has(node.text)) {
      const parent = node.parent;
      const isDeclaration =
        (ts.isVariableDeclaration(parent) ||
          ts.isTypeAliasDeclaration(parent) ||
          ts.isFunctionDeclaration(parent)) &&
        parent.name === node;
      const isProperty = ts.isPropertyAccessExpression(parent) && parent.name === node;
      if (!isDeclaration && !isProperty) extrasUsed.add(node.text);
    }
    ts.forEachChild(node, visitUses);
  };
  visitUses(source);

  // 3. Spread tables modules: `...betterAuthSchema` from a module exporting `schema`.
  const spreads: SpreadModule[] = [];
  for (const property of tablesObject.properties) {
    if (!ts.isSpreadAssignment(property) || !ts.isIdentifier(property.expression)) continue;
    const local = property.expression.text;
    const found = findImport(ts, source, local);
    if (!found) {
      throw notSupportedYet(
        `spreading \`${local}\`, which schema.ts doesn't import`,
        where(property),
      );
    }
    const target = input.resolve(found.declaration.moduleSpecifier.getText(source).slice(1, -1));
    if (!target) throw notSupportedYet(`spreading tables from a package`, where(property));
    if ((found.element.propertyName ?? found.element.name).text === "schema") {
      edits.push({
        start: found.element.getStart(source),
        end: found.element.getEnd(),
        text: `tables as ${local}`,
      });
      changes.push(
        `${where(found.element)}: imports \`tables\` (the module's new name for its tables)`,
      );
    }
    spreads.push({
      file: target,
      specifier: (found.declaration.moduleSpecifier as TS.StringLiteral).text,
      local,
    });
  }

  if (extrasUsed.size > 0) {
    // After the last import that stays, or before the first statement that does.
    const kept = source.statements.filter((statement) => !removed.has(statement));
    const last = kept.filter(ts.isImportDeclaration).pop();
    const line = `import { ${[...extrasUsed].sort().join(", ")} } from ${JSON.stringify(input.extrasSpecifier)};`;
    if (last) edits.push({ start: last.getEnd(), end: last.getEnd(), text: `\n${line}` });
    else {
      const at = kept[0]?.getStart(source) ?? text.length;
      edits.push({ start: at, end: at, text: `${line}\n\n` });
    }
    changes.push(
      `${relative(input.root, file)}: imports ${[...extrasUsed].join(", ")} from app-extras.ts`,
    );
  }

  let out = applyEdits(text, edits);
  if (tablesName && tablesName !== "schema")
    out = renameIdentifier(ts, file, out, tablesName, "schema");
  return {
    text: out.replace(/\n{3,}/g, "\n\n").replace(/\s*$/, "\n"),
    spreads,
    inlinePermissions,
    changes,
  };
}

/**
 * A spread tables module (Better Auth's): `export const schema = {…}` →
 * `export const tables = {…}`; its own app goes; its callback permissions are
 * replaced by `permissionsText` (or dropped, when the app prints them).
 */
export function codemodSpreadModule(input: {
  ts: Ts;
  file: string;
  text: string;
  /** `export const permissions = …;` to put in place of the old one, or undefined to drop it. */
  permissionsText: string | undefined;
}): { text: string; hadPermissions: boolean } {
  const { ts, file, text } = input;
  const source = ts.createSourceFile(file, text, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
  const edits: TextEdit[] = [];
  let hadPermissions = false;
  for (const statement of source.statements) {
    if (!ts.isVariableStatement(statement)) continue;
    const declaration = statement.declarationList.declarations[0]!;
    if (!ts.isIdentifier(declaration.name)) continue;
    const name = declaration.name.text;
    const init = declaration.initializer?.getText(source) ?? "";
    if (name === "schema" && isExportedNode(ts, statement)) {
      edits.push({
        start: declaration.name.getStart(source),
        end: declaration.name.getEnd(),
        text: "tables",
      });
    } else if (/\.defineApp\(/.test(init)) {
      edits.push({ start: statement.getFullStart(), end: statement.getEnd(), text: "" });
    } else if (name === "permissions" && isExportedNode(ts, statement)) {
      hadPermissions = true;
      edits.push({
        start: statement.getStart(source),
        end: statement.getEnd(),
        text: input.permissionsText ?? "",
      });
    }
  }
  const out = applyEdits(text, edits);
  return {
    text: renameIdentifier(ts, file, out, "schema", "tables", true).replace(/\n{3,}/g, "\n\n"),
    hadPermissions,
  };
}

function findImport(
  ts: Ts,
  source: TS.SourceFile,
  local: string,
): { declaration: TS.ImportDeclaration; element: TS.ImportSpecifier } | undefined {
  for (const statement of source.statements) {
    if (!ts.isImportDeclaration(statement)) continue;
    const named = statement.importClause?.namedBindings;
    if (!named || !ts.isNamedImports(named)) continue;
    for (const element of named.elements) {
      if (element.name.text === local) return { declaration: statement, element };
    }
  }
  return undefined;
}

function isExportedNode(ts: Ts, statement: TS.Statement): boolean {
  return (
    ts.canHaveModifiers(statement) &&
    (ts.getModifiers(statement) ?? []).some((m) => m.kind === ts.SyntaxKind.ExportKeyword)
  );
}

function declaredNamesOf(ts: Ts, statement: TS.Statement): string[] {
  if (ts.isVariableStatement(statement)) {
    return statement.declarationList.declarations.flatMap((d) =>
      ts.isIdentifier(d.name) ? [d.name.text] : [],
    );
  }
  if (
    (ts.isTypeAliasDeclaration(statement) ||
      ts.isInterfaceDeclaration(statement) ||
      ts.isFunctionDeclaration(statement) ||
      ts.isClassDeclaration(statement) ||
      ts.isEnumDeclaration(statement)) &&
    statement.name
  ) {
    return [statement.name.text];
  }
  return [];
}

/** Rename free uses of `from` to `to` (not property names or other bindings' keys). */
function renameIdentifier(
  ts: Ts,
  file: string,
  text: string,
  from: string,
  to: string,
  valuesOnly = false,
): string {
  const source = ts.createSourceFile(file, text, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
  const edits: TextEdit[] = [];
  const visit = (node: TS.Node) => {
    if (ts.isIdentifier(node) && node.text === from) {
      const parent = node.parent;
      const skip =
        (ts.isPropertyAccessExpression(parent) && parent.name === node) ||
        (ts.isPropertyAssignment(parent) && parent.name === node) ||
        (ts.isQualifiedName(parent) && parent.right === node) ||
        ts.isImportSpecifier(parent) ||
        ts.isExportSpecifier(parent) ||
        (valuesOnly && ts.isVariableDeclaration(parent));
      if (!skip) edits.push({ start: node.getStart(source), end: node.getEnd(), text: to });
    }
    ts.forEachChild(node, visit);
  };
  visit(source);
  return applyEdits(text, edits);
}
