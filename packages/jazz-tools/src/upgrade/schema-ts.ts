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
import { UpgradeRefusal, notSupportedYet } from "./errors.js";
import type { MovedExport } from "./extras.js";

type Ts = typeof TS;

export interface SpreadModule {
  /** The module's file. */
  file: string;
  /** The specifier schema.ts imports it by. */
  specifier: string;
  /** The local name schema.ts spreads. */
  local: string;
  /** The name the module exports its tables under (`schema`, `betterAuthSchema`, …). */
  exported: string;
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
  /** The name schema.ts has jazz-tools' `schema` namespace under (`s`). */
  dsl: string;
  changes: string[];
}

export function codemodSchema(input: SchemaCodemodInput): SchemaCodemod {
  // `import { schema } from "jazz-tools"`: the new export needs the name, so the
  // namespace becomes `s` (`schema.table(…)` → `s.table(…)`) first.
  const bare = bareSchemaImport(input.ts, input.file, input.text, input.root);
  if (bare) {
    const result = codemodSchema({ ...input, text: bare.text });
    result.changes.unshift(
      `${relative(input.root, input.file) || input.file}:${bare.line}: \`import { schema }\` from jazz-tools → \`import { schema as s }\`, the namespace renamed \`s\``,
    );
    return result;
  }
  return codemodSchemaText(input);
}

/**
 * The text with a bare `schema` import from jazz-tools renamed `s`, or
 * undefined when there's none. Only what resolves to the import changes, by
 * the TypeScript checker on the file alone: a property key stays (`{ schema }`
 * becomes `{ schema: s }`), and a local `schema` (a destructured binding, a
 * parameter, a class field) and its uses are left as they are. Refuses what
 * the rename can't keep: an unaliased re-export of the namespace (alpha.66's
 * schema.ts exports its own `schema`), or an `s` the file already binds or uses.
 */
function bareSchemaImport(
  ts: Ts,
  file: string,
  text: string,
  root: string,
): { text: string; line: number } | undefined {
  // A program of this file alone: enough for the checker to resolve its names.
  const options: TS.CompilerOptions = { noLib: true, noResolve: true, types: [], allowJs: false };
  const host = ts.createCompilerHost(options);
  const parsed = ts.createSourceFile(file, text, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
  host.getSourceFile = (name) => (name === file ? parsed : undefined);
  host.fileExists = (name) => name === file;
  host.readFile = (name) => (name === file ? text : undefined);
  const program = ts.createProgram([file], options, host);
  const source = program.getSourceFile(file)!;
  const checker = program.getTypeChecker();
  const where = (node: TS.Node) =>
    `${relative(root, file) || file}:${source.getLineAndCharacterOfPosition(node.getStart(source)).line + 1}`;
  let element: TS.ImportSpecifier | undefined;
  for (const statement of source.statements) {
    if (!ts.isImportDeclaration(statement) || !ts.isStringLiteral(statement.moduleSpecifier))
      continue;
    if (statement.moduleSpecifier.text !== "jazz-tools") continue;
    const named = statement.importClause?.namedBindings;
    if (!named || !ts.isNamedImports(named)) continue;
    element ??= named.elements.find((e) => !e.propertyName && e.name.text === "schema");
  }
  if (!element) return undefined;

  /** A name's use as a value or type, not as a property name or key. */
  const isReference = (node: TS.Identifier): boolean => {
    const parent = node.parent;
    if (ts.isPropertyAccessExpression(parent) && parent.name === node) return false;
    if (ts.isQualifiedName(parent) && parent.right === node) return false;
    if (
      (ts.isPropertyAssignment(parent) ||
        ts.isPropertySignature(parent) ||
        ts.isPropertyDeclaration(parent) ||
        ts.isMethodDeclaration(parent) ||
        ts.isMethodSignature(parent) ||
        ts.isGetAccessor(parent) ||
        ts.isSetAccessor(parent) ||
        ts.isEnumMember(parent)) &&
      parent.name === node
    )
      return false;
    if (ts.isImportSpecifier(parent) && parent.propertyName === node) return false;
    if (ts.isExportSpecifier(parent) && parent.name === node && parent.propertyName) return false;
    if (ts.isBindingElement(parent) && parent.propertyName === node) return false;
    if (ts.isLabeledStatement(parent) || ts.isBreakOrContinueStatement(parent)) return false;
    return true;
  };

  const edits: TextEdit[] = [
    { start: element.getStart(source), end: element.getEnd(), text: "schema as s" },
  ];
  let takenAt: TS.Node | undefined;
  let reexport: TS.Node | undefined;
  const imported = checker.getSymbolAtLocation(element.name);
  const visit = (node: TS.Node) => {
    if (ts.isIdentifier(node) && node !== element!.name) {
      if (node.text === "s" && isReference(node)) takenAt ??= node;
      if (node.text === "schema" && imported) {
        const parent = node.parent;
        if (ts.isShorthandPropertyAssignment(parent) && parent.name === node) {
          // The identifier is the key too: keep it.
          if (checker.getShorthandAssignmentValueSymbol(parent) === imported) {
            edits.push({ start: node.getStart(source), end: node.getEnd(), text: "schema: s" });
          }
        } else if (ts.isExportSpecifier(parent)) {
          if (checker.getExportSpecifierLocalTargetSymbol(parent) === imported) {
            // `export { schema as ns }` keeps its exported name; a bare `export { schema }` can't.
            if (parent.propertyName) {
              edits.push({ start: node.getStart(source), end: node.getEnd(), text: "s" });
            } else reexport ??= parent;
          }
        } else if (isReference(node) && checker.getSymbolAtLocation(node) === imported) {
          edits.push({ start: node.getStart(source), end: node.getEnd(), text: "s" });
        }
      }
    }
    ts.forEachChild(node, visit);
  };
  visit(source);
  if (reexport) {
    throw new UpgradeRefusal(
      "schema.ts re-exports jazz-tools' `schema` namespace, but alpha.66's schema.ts exports its own `schema` (the tables): " +
        "import the namespace from jazz-tools where it's used instead, then run upgrade again",
      where(reexport),
    );
  }
  if (takenAt) {
    throw new UpgradeRefusal(
      "schema.ts imports `schema` from jazz-tools bare and already uses `s`, the name upgrade gives it: " +
        "import it as `schema as s` (or another name) yourself, then run upgrade again",
      where(takenAt),
    );
  }
  const line = source.getLineAndCharacterOfPosition(element.getStart(source)).line + 1;
  return { text: applyEdits(text, edits), line };
}

function codemodSchemaText(input: SchemaCodemodInput): SchemaCodemod {
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
  const movedToExtrasOrUnused = (name: string) =>
    !input.exports.has(name) || movedToExtras.has(name);
  // Whatever still uses the app (a query for a row type, say) went to
  // app-extras.ts with the exports that need it, or is dead: alpha.66's
  // schema.ts has no app.
  const appName = appStatement.declarationList.declarations[0]!.name;
  if (ts.isIdentifier(appName)) {
    const usesApp = (node: TS.Node): boolean =>
      (ts.isIdentifier(node) && node.text === appName.text) ||
      (ts.forEachChild(node, usesApp) ?? false);
    for (const statement of source.statements) {
      if (statement === tablesStatement || statement === appStatement || removed.has(statement))
        continue;
      if (ts.isImportDeclaration(statement) || !usesApp(statement)) continue;
      removeStatement(statement);
      const names = declaredNamesOf(ts, statement);
      const moved = names.length > 0 && names.every((n) => input.exports.has(n));
      if (!moved && names.some((n) => movedToExtrasOrUnused(n))) {
        changes.push(
          `${where(statement)}: dropped \`${names.join(", ")}\`: it used the app, which schema.ts no longer defines`,
        );
      }
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
    const exported = (found.element.propertyName ?? found.element.name).text;
    if (exported !== "tables") {
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
      exported,
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
    dsl: dsl!,
    text: out.replace(/\n{3,}/g, "\n\n").replace(/\s*$/, "\n"),
    spreads,
    inlinePermissions,
    changes,
  };
}

/**
 * A spread tables module (Better Auth's), found by what the schema spreads
 * from it, whatever it calls its tables: the tables export becomes `tables`;
 * its own app, the `s.Schema` / `s.App` aliases and whatever used the app
 * go; its callback permissions (`s.definePermissions(…)`, under any name) are
 * replaced by `permissionsText` (or dropped, when the app prints them).
 */
export function codemodSpreadModule(input: {
  ts: Ts;
  file: string;
  text: string;
  /** The name the module exports its tables under. */
  exported: string;
  /** `export const permissions = …;` to put in place of the old one, or undefined to drop it. */
  permissionsText: string | undefined;
}): { text: string; hadPermissions: boolean; dropped: string[] } {
  const { ts, file, text } = input;
  const source = ts.createSourceFile(file, text, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
  const edits: TextEdit[] = [];
  const dropped: string[] = [];
  let hadPermissions = false;
  const remove = (statement: TS.Statement) => {
    // The statement, its line break and its own comments, not the blank line before them.
    const start = statement.getFullStart();
    const leading = text.slice(start, statement.getStart(source));
    let end = statement.getEnd();
    while (text[end] === " " || text[end] === "\t") end++;
    if (text[end] === "\n") end++;
    edits.push({
      start: start + leading.length - leading.replace(/^\s*\n/, "").length,
      end,
      text: "",
    });
  };
  const call = (statement: TS.VariableStatement, name: string) => {
    let init = statement.declarationList.declarations[0]?.initializer;
    while (init && (ts.isAsExpression(init) || ts.isSatisfiesExpression(init)))
      init = init.expression;
    return (
      init !== undefined &&
      ts.isCallExpression(init) &&
      ts.isPropertyAccessExpression(init.expression) &&
      init.expression.name.text === name
    );
  };
  // The module's own apps, and everything that uses them.
  const apps = new Set<string>();
  for (const statement of source.statements) {
    if (!ts.isVariableStatement(statement) || !call(statement, "defineApp")) continue;
    const name = statement.declarationList.declarations[0]!.name;
    if (ts.isIdentifier(name)) apps.add(name.text);
  }
  const usesApp = (node: TS.Node): boolean =>
    (ts.isIdentifier(node) && apps.has(node.text)) || (ts.forEachChild(node, usesApp) ?? false);
  let permissionsDone = false;
  for (const statement of source.statements) {
    if (ts.isTypeAliasDeclaration(statement)) {
      const type = statement.type;
      if (
        ts.isTypeReferenceNode(type) &&
        ts.isQualifiedName(type.typeName) &&
        ["Schema", "App", "SliceableApp"].includes(type.typeName.right.text)
      ) {
        remove(statement);
      }
      continue;
    }
    if (!ts.isVariableStatement(statement)) continue;
    const declaration = statement.declarationList.declarations[0]!;
    if (!ts.isIdentifier(declaration.name)) continue;
    const name = declaration.name.text;
    if (name === input.exported && isExportedNode(ts, statement)) {
      if (name !== "tables") {
        edits.push({
          start: declaration.name.getStart(source),
          end: declaration.name.getEnd(),
          text: "tables",
        });
      }
    } else if (call(statement, "definePermissions")) {
      hadPermissions = true;
      if (input.permissionsText && !permissionsDone) {
        edits.push({
          start: statement.getStart(source),
          end: statement.getEnd(),
          text: input.permissionsText,
        });
        permissionsDone = true;
      } else {
        remove(statement);
      }
    } else if (apps.has(name)) {
      remove(statement);
    } else if (usesApp(statement)) {
      remove(statement);
      if (isExportedNode(ts, statement)) dropped.push(name);
    }
  }
  // A module without permissions gets them, so the app spreads them like Better Auth's own.
  if (!permissionsDone && input.permissionsText) {
    edits.push({
      start: text.length,
      end: text.length,
      text: `${text.endsWith("\n") ? "" : "\n"}\n${input.permissionsText}\n`,
    });
  }
  const out = applyEdits(text, edits);
  const renamed =
    input.exported === "tables"
      ? out
      : renameIdentifier(ts, file, out, input.exported, "tables", true);
  return { text: renamed.replace(/\n{3,}/g, "\n\n"), hadPermissions, dropped };
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
