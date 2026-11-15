/**
 * What schema.ts exported besides its schema. Each export ends up in one of:
 * - `jazz.generated.ts`: `app`, and row/insert aliases (`type Todo =
 *   s.RowOf<typeof app.todos>`) and enum column aliases (`Issue["status"]`)
 *   the generated module declares under some name;
 * - `app-extras.ts`, a hand-written module upgrade starts: everything else
 *   that is plain TypeScript (constants, other types), rewritten to the
 *   generated types;
 * - nowhere, for schema DSL values nothing imports.
 */
import { relative } from "node:path";
import type * as TS from "typescript";
import { applyEdits, type TextEdit } from "./edits.js";
import { UpgradeRefusal, notSupportedYet } from "./errors.js";
import { pascal, type TableTypeNames } from "./names.js";

type Ts = typeof TS;

/** Where an export of schema.ts lives after upgrade. */
export interface MovedExport {
  module: "generated" | "extras" | "computed";
  /** Its name there. */
  name: string;
  /** Whether it is only a type. */
  typeOnly: boolean;
}

export interface ExtrasResult {
  exports: Map<string, MovedExport>;
  /** The text of app-extras.ts, or null when nothing needs it. */
  extrasFile: string | null;
  /** Exports dropped because they were schema DSL values. */
  dropped: string[];
}

export interface ExtrasInput {
  ts: Ts;
  program: TS.Program;
  schemaFile: string;
  /** Table → generated type names. */
  tables: Record<string, TableTypeNames>;
  /** Every name jazz.generated.ts exports. */
  generatedExports: Set<string>;
  /** Exports of schema.ts that moved into jazz.computed.ts with a computed callback. */
  movedToComputed: string[];
  /** Whether relative imports in this project carry `.js` (`./schema.js`). */
  jsExtensions: boolean;
  /** Where to put notes for the report. */
  notes: string[];
  /** Names of schema.ts other project files import, to tell dead DSL exports from live ones. */
  importedNames: Set<string>;
}

const TYPE_HELPERS: Record<string, "row" | "insert" | "keep"> = {
  RowOf: "row",
  InsertOf: "insert",
  WhereOf: "keep",
  LargeValueUpdateOf: "keep",
  StreamingInsertOf: "keep",
  StreamingUpdateOf: "keep",
  StreamingUpsertOf: "keep",
  TableMetaOf: "keep",
};

export function convertExtras(input: ExtrasInput): ExtrasResult {
  const { ts, program, schemaFile, tables, generatedExports } = input;
  const source = program.getSourceFile(schemaFile)!;
  const checker = program.getTypeChecker();
  const where = (node: TS.Node) =>
    `${relative(process.cwd(), schemaFile) || schemaFile}:${source.getLineAndCharacterOfPosition(node.getStart(source)).line + 1}`;

  // `schema` from jazz-tools, under whatever local name.
  const dsl = new Set<TS.Symbol>();
  for (const statement of source.statements) {
    if (!ts.isImportDeclaration(statement) || !isJazzTools(statement.moduleSpecifier)) continue;
    const named = statement.importClause?.namedBindings;
    if (named && ts.isNamedImports(named)) {
      for (const element of named.elements) {
        if ((element.propertyName ?? element.name).text === "schema") {
          const symbol = checker.getSymbolAtLocation(element.name);
          if (symbol) dsl.add(symbol);
        }
      }
    }
  }
  const isDsl = (node: TS.Node | undefined): boolean => {
    if (!node || !ts.isIdentifier(node)) return false;
    const symbol = checker.getSymbolAtLocation(node);
    return symbol !== undefined && dsl.has(symbol);
  };

  const exports = new Map<string, MovedExport>();
  for (const name of input.movedToComputed)
    exports.set(name, { module: "computed", name, typeOnly: false });

  // The app: a const made by s.defineApp / s.defineSliceableApp, and any
  // slice of it (`full.slice("a", "b")`). Slicing is gone, so every one of
  // them becomes the generated `app`.
  const appSymbols = new Set<TS.Symbol>();
  let defined: TS.Node | undefined;
  for (const statement of source.statements) {
    if (!ts.isVariableStatement(statement)) continue;
    for (const declaration of statement.declarationList.declarations) {
      const init = declaration.initializer && unwrap(ts, declaration.initializer);
      if (
        !init ||
        !ts.isCallExpression(init) ||
        !ts.isPropertyAccessExpression(init.expression) ||
        !ts.isIdentifier(declaration.name)
      )
        continue;
      const callee = init.expression;
      const isDefine =
        isDsl(callee.expression) &&
        (callee.name.text === "defineApp" || callee.name.text === "defineSliceableApp");
      const isSlice =
        callee.name.text === "slice" &&
        ts.isIdentifier(callee.expression) &&
        appSymbols.has(checker.getSymbolAtLocation(callee.expression)!);
      if (!isDefine && !isSlice) continue;
      if (isDefine) {
        if (defined) throw notSupportedYet("a schema.ts that defines two apps", where(declaration));
        defined = declaration;
      }
      const symbol = checker.getSymbolAtLocation(declaration.name);
      if (symbol) appSymbols.add(symbol);
      if (isExported(ts, statement)) {
        exports.set(declaration.name.text, { module: "generated", name: "app", typeOnly: false });
      }
    }
  }
  if (!defined) {
    throw notSupportedYet(
      "a schema.ts without `export const app = s.defineApp(...)`",
      where(source.statements[0] ?? source),
    );
  }
  refuseMissingMigrations(ts, checker, defined as TS.VariableDeclaration, where, input.notes);
  const isApp = (node: TS.Node): boolean =>
    ts.isIdentifier(node) && appSymbols.has(checker.getSymbolAtLocation(node)!);

  /** `typeof app.<table>` → the table. */
  const tableOfTypeQuery = (node: TS.TypeNode | undefined): string | undefined => {
    if (!node || !ts.isTypeQueryNode(node)) return undefined;
    const name = node.exprName;
    if (!ts.isQualifiedName(name) || !isApp(name.left)) return undefined;
    return tables[name.right.text] ? name.right.text : undefined;
  };
  /** `s.RowOf<typeof app.t>` / `s.InsertOf<…>` → its generated name. */
  const generatedHelper = (node: TS.Node): string | undefined => {
    if (
      !ts.isTypeReferenceNode(node) ||
      !ts.isQualifiedName(node.typeName) ||
      !isDsl(node.typeName.left)
    )
      return undefined;
    const role = TYPE_HELPERS[node.typeName.right.text];
    if (role !== "row" && role !== "insert") return undefined;
    const table = tableOfTypeQuery(node.typeArguments?.[0]);
    return table ? tables[table]![role] : undefined;
  };

  // Pass 1: exports the generated module declares.
  const aliasTables = new Map<TS.Symbol, string>(); // RowOf alias → table
  const extrasStatements: TS.Statement[] = [];
  const dropped: string[] = [];
  const movedToComputed = new Set(input.movedToComputed);
  for (const statement of source.statements) {
    if (ts.isImportDeclaration(statement) || !isExported(ts, statement)) continue;
    if (declaredNames(ts, statement).some((name) => movedToComputed.has(name))) continue;
    if (
      ts.isVariableStatement(statement) &&
      statement.declarationList.declarations.some(
        (d) => ts.isIdentifier(d.name) && appSymbols.has(checker.getSymbolAtLocation(d.name)!),
      )
    ) {
      if (statement.declarationList.declarations.length > 1) {
        throw notSupportedYet(
          "declaring other names in the same statement as the app",
          where(statement),
        );
      }
      continue;
    }
    if (ts.isTypeAliasDeclaration(statement)) {
      const generated = generatedHelper(statement.type);
      if (generated) {
        exports.set(statement.name.text, { module: "generated", name: generated, typeOnly: true });
        const table = tableOfTypeQuery((statement.type as TS.TypeReferenceNode).typeArguments?.[0]);
        const symbol = checker.getSymbolAtLocation(statement.name);
        if (
          symbol &&
          table &&
          (statement.type as TS.TypeReferenceNode).typeName.getText(source).endsWith("RowOf")
        ) {
          aliasTables.set(symbol, table);
        }
        continue;
      }
    }
    extrasStatements.push(statement);
  }
  // Pass 2: `Row["column"]` of a row alias, where the column is an enum the generated module names.
  for (const statement of [...extrasStatements]) {
    if (!ts.isTypeAliasDeclaration(statement)) continue;
    const type = statement.type;
    if (
      !ts.isIndexedAccessTypeNode(type) ||
      !ts.isLiteralTypeNode(type.indexType) ||
      !ts.isStringLiteral(type.indexType.literal)
    )
      continue;
    let table: string | undefined;
    if (ts.isTypeReferenceNode(type.objectType) && ts.isIdentifier(type.objectType.typeName)) {
      const symbol = checker.getSymbolAtLocation(type.objectType.typeName);
      table = symbol && aliasTables.get(symbol);
    } else {
      const generated = generatedHelper(type.objectType);
      if (generated) {
        table = Object.keys(tables).find((t) => tables[t]!.row === generated);
      }
    }
    if (!table) continue;
    const enumName = `${tables[table]!.row}${pascal(type.indexType.literal.text)}`;
    if (!generatedExports.has(enumName)) continue;
    exports.set(statement.name.text, { module: "generated", name: enumName, typeOnly: true });
    extrasStatements.splice(extrasStatements.indexOf(statement), 1);
  }

  // Pass 3: the rest, with what it needs, rewritten off the DSL.
  const kept = new Set<TS.Statement>();
  const importNames = new Map<TS.ImportDeclaration, Set<string>>();
  const generatedImports = new Map<string, { local: string; typeOnly: boolean }>(); // generated name → local name
  const jazzToolsTypes = new Set<string>();
  const edits = new Map<TS.Statement, TextEdit[]>();
  const extrasNames = new Set<string>();
  const usesDsl = (statement: TS.Statement): TS.Node | undefined => {
    let found: TS.Node | undefined;
    const visit = (node: TS.Node): void => {
      if (found) return;
      if (
        ts.isTypeReferenceNode(node) &&
        ts.isQualifiedName(node.typeName) &&
        isDsl(node.typeName.left) &&
        TYPE_HELPERS[node.typeName.right.text]
      ) {
        node.typeArguments?.forEach(visit);
        return;
      }
      if (ts.isIdentifier(node) && isDsl(node)) {
        found = node;
        return;
      }
      ts.forEachChild(node, visit);
    };
    visit(statement);
    return found;
  };

  for (const statement of extrasStatements) {
    const names = declaredNames(ts, statement);
    const dslNode = usesDsl(statement);
    if (dslNode) {
      const live = names.filter((n) => input.importedNames.has(n));
      if (live.length > 0) {
        throw notSupportedYet(
          `moving \`${live.join("`, `")}\` out of schema.ts: it is built with the schema DSL, and other files import it`,
          where(dslNode),
          "replace its uses with the generated types from jazz.generated.ts, then upgrade again",
        );
      }
      dropped.push(...names);
      continue;
    }
    for (const name of names) {
      extrasNames.add(name);
      exports.set(name, { module: "extras", name, typeOnly: isTypeOnly(ts, statement) });
    }
  }

  const local = (generatedName: string, typeOnly: boolean): string => {
    const existing = generatedImports.get(generatedName);
    if (existing) {
      existing.typeOnly &&= typeOnly;
      return existing.local;
    }
    const name = extrasNames.has(generatedName) ? `Generated${generatedName}` : generatedName;
    generatedImports.set(generatedName, { local: name, typeOnly });
    return name;
  };

  const keep = (statement: TS.Statement): void => {
    if (kept.has(statement)) return;
    kept.add(statement);
    const list: TextEdit[] = [];
    edits.set(statement, list);
    const visit = (node: TS.Node): void => {
      if (
        ts.isTypeReferenceNode(node) &&
        ts.isQualifiedName(node.typeName) &&
        isDsl(node.typeName.left)
      ) {
        const generated = generatedHelper(node);
        if (generated) {
          list.push({
            start: node.getStart(source),
            end: node.getEnd(),
            text: local(generated, true),
          });
          return;
        }
        const helper = node.typeName.right.text;
        if (TYPE_HELPERS[helper]) {
          jazzToolsTypes.add(helper);
          list.push({
            start: node.typeName.getStart(source),
            end: node.typeName.getEnd(),
            text: helper,
          });
          node.typeArguments?.forEach(visit);
          return;
        }
      }
      if (ts.isIdentifier(node) && !isPropertyNameOnly(ts, node)) {
        if (isApp(node)) {
          const inType = isInTypePosition(ts, node);
          list.push({
            start: node.getStart(source),
            end: node.getEnd(),
            text: local("app", inType),
          });
          return;
        }
        const symbol = checker.getSymbolAtLocation(node);
        for (const declaration of symbol?.declarations ?? []) {
          if (declaration.getSourceFile() !== source) continue;
          const top = topLevel(ts, declaration);
          if (!top || top === statement) continue;
          if (ts.isImportDeclaration(top)) {
            const set = importNames.get(top) ?? new Set<string>();
            set.add(node.text);
            importNames.set(top, set);
            continue;
          }
          const moved = exports.get(node.text);
          if (moved?.module === "generated") {
            list.push({
              start: node.getStart(source),
              end: node.getEnd(),
              text: local(moved.name, true),
            });
            continue;
          }
          if (moved?.module === "computed") {
            const set = computedImports;
            set.add(node.text);
            continue;
          }
          const dslNode = usesDsl(top);
          if (dslNode) {
            throw notSupportedYet(
              `moving \`${declaredNames(ts, statement).join("`, `")}\` out of schema.ts: it uses \`${node.text}\`, which is built with the schema DSL`,
              where(node),
            );
          }
          keep(top);
        }
      }
      ts.forEachChild(node, visit);
    };
    visit(statement);
  };
  const computedImports = new Set<string>();
  for (const statement of extrasStatements) {
    if (!usesDsl(statement)) keep(statement);
  }
  if (kept.size === 0) return { exports, extrasFile: null, dropped };

  const ext = input.jsExtensions ? ".js" : "";
  const lines: string[] = [
    "// What schema.ts exported besides the schema, moved here by `jazz-tools upgrade`.",
    "// The schema itself is jazz.app.json; its types are in jazz.generated.ts.",
  ];
  if (jazzToolsTypes.size > 0)
    lines.push(`import type { ${[...jazzToolsTypes].sort().join(", ")} } from "jazz-tools";`);
  for (const [statement, names] of importNames) {
    const line = importLine(ts, statement, names);
    if (line) lines.push(line);
  }
  if (generatedImports.size > 0) {
    const allTypes = [...generatedImports.values()].every((g) => g.typeOnly);
    const parts = [...generatedImports].map(([name, g]) => {
      const spec = g.local === name ? name : `${name} as ${g.local}`;
      return !allTypes && g.typeOnly ? `type ${spec}` : spec;
    });
    lines.push(
      `import ${allTypes ? "type " : ""}{ ${parts.join(", ")} } from "./jazz.generated${ext}";`,
    );
  }
  if (computedImports.size > 0)
    lines.push(`import { ${[...computedImports].join(", ")} } from "./jazz.computed${ext}";`);
  lines.push("");
  for (const statement of [...kept].sort((a, b) => a.pos - b.pos)) {
    const start = statement.getFullStart();
    const text = applyEdits(
      source.text.slice(start, statement.getEnd()),
      edits.get(statement)!.map((e) => ({ ...e, start: e.start - start, end: e.end - start })),
    );
    lines.push(text.replace(/^\s*\n/, ""));
  }
  return { exports, extrasFile: `${lines.join("\n")}\n`, dropped };
}

/**
 * An app ships its history through the migrations module schema.ts passes
 * to s.defineApp: alpha.64's validate writes it only then, and otherwise
 * just warns. Without it, stores may hold versions jazz.app.json doesn't
 * carry, so upgrade refuses rather than start the history empty. An explicit
 * `migrations: "none"` is the app saying it keeps no history.
 */
function refuseMissingMigrations(
  ts: Ts,
  checker: TS.TypeChecker,
  declaration: TS.VariableDeclaration,
  where: (node: TS.Node) => string,
  notes: string[],
): void {
  const call = unwrap(ts, declaration.initializer!) as TS.CallExpression;
  const options = call.arguments[1] && unwrap(ts, call.arguments[1]);
  const property =
    options && ts.isObjectLiteralExpression(options)
      ? options.properties.find(
          (p) =>
            (ts.isPropertyAssignment(p) || ts.isShorthandPropertyAssignment(p)) &&
            p.name.getText() === "migrations",
        )
      : undefined;
  const value =
    property && ts.isPropertyAssignment(property)
      ? unwrap(ts, property.initializer)
      : property && ts.isShorthandPropertyAssignment(property)
        ? property.name
        : undefined;
  if (value && ts.isStringLiteral(value) && value.text === "none") {
    notes.push(
      'schema.ts declares `migrations: "none"`: jazz.app.json starts with no history, as the app shipped none.',
    );
    return;
  }
  // The value must come from an import of the migrations module.
  let imported = false;
  if (value && ts.isIdentifier(value)) {
    const symbol =
      property && ts.isShorthandPropertyAssignment(property)
        ? checker.getShorthandAssignmentValueSymbol(property)
        : checker.getSymbolAtLocation(value);
    const declarationNode = symbol?.declarations?.[0];
    let importDecl: TS.Node | undefined = declarationNode;
    while (importDecl && !ts.isImportDeclaration(importDecl)) importDecl = importDecl.parent;
    if (
      importDecl &&
      ts.isImportDeclaration(importDecl) &&
      ts.isStringLiteral(importDecl.moduleSpecifier)
    ) {
      imported = /(^|\/)migrations(\/index)?(\.[mc]?[jt]s)?$/.test(importDecl.moduleSpecifier.text);
    }
  }
  if (imported) return;
  throw new UpgradeRefusal(
    "schema.ts doesn't pass its migrations module to s.defineApp, so alpha.64 shipped no schema history and a store may " +
      'hold versions jazz.app.json doesn\'t record. Add `import { migrations } from "./migrations";` and ' +
      "`s.defineApp(schema, { migrations })`, run `npx jazz-tools validate` with the app's alpha.64 (it writes the history " +
      'into jazz.app.json), commit, then upgrade again. If the app really keeps no history, say `{ migrations: "none" }`.',
    where(call),
  );
}

function unwrap(ts: Ts, node: TS.Expression): TS.Expression {
  while (
    ts.isAsExpression(node) ||
    ts.isSatisfiesExpression(node) ||
    ts.isParenthesizedExpression(node)
  )
    node = node.expression;
  return node;
}

function isJazzTools(specifier: TS.Expression): boolean {
  const text = (specifier as TS.StringLiteral).text;
  return text === "jazz-tools" || text?.startsWith("jazz-tools/");
}

function isExported(ts: Ts, statement: TS.Statement): boolean {
  const modifiers = ts.canHaveModifiers(statement) ? ts.getModifiers(statement) : undefined;
  return modifiers?.some((m) => m.kind === ts.SyntaxKind.ExportKeyword) ?? false;
}

function isTypeOnly(ts: Ts, statement: TS.Statement): boolean {
  return ts.isTypeAliasDeclaration(statement) || ts.isInterfaceDeclaration(statement);
}

export function declaredNames(ts: Ts, statement: TS.Statement): string[] {
  if (ts.isVariableStatement(statement)) {
    return statement.declarationList.declarations.flatMap((d) =>
      ts.isIdentifier(d.name) ? [d.name.text] : [],
    );
  }
  const named = statement as TS.Statement & { name?: TS.Identifier };
  return named.name && ts.isIdentifier(named.name) ? [named.name.text] : [];
}

function topLevel(ts: Ts, node: TS.Node): TS.Statement | undefined {
  let current: TS.Node = node;
  while (current.parent && !ts.isSourceFile(current.parent)) current = current.parent;
  return current.parent ? (current as TS.Statement) : undefined;
}

function isPropertyNameOnly(ts: Ts, node: TS.Identifier): boolean {
  const parent = node.parent;
  if (ts.isPropertyAccessExpression(parent) && parent.name === node) return true;
  if (
    (ts.isPropertyAssignment(parent) ||
      ts.isPropertySignature(parent) ||
      ts.isMethodDeclaration(parent)) &&
    parent.name === node
  )
    return true;
  if (ts.isQualifiedName(parent) && parent.right === node) return true;
  if (
    ts.isTypeAliasDeclaration(parent) ||
    ts.isInterfaceDeclaration(parent) ||
    ts.isVariableDeclaration(parent) ||
    ts.isFunctionDeclaration(parent)
  ) {
    return (parent as { name?: TS.Node }).name === node;
  }
  return false;
}

function isInTypePosition(ts: Ts, node: TS.Node): boolean {
  for (let current = node.parent; current; current = current.parent) {
    if (ts.isTypeQueryNode(current)) return true;
    if (ts.isTypeNode(current) && !ts.isExpressionWithTypeArguments(current)) return true;
    if (ts.isStatement(current) || ts.isExpressionStatement(current)) return false;
  }
  return false;
}

/** An import of only `names` from `statement`, or null when it imports none of them. */
export function importLine(
  ts: Ts,
  statement: TS.ImportDeclaration,
  names: Set<string>,
): string | null {
  const specifier = JSON.stringify((statement.moduleSpecifier as TS.StringLiteral).text);
  const clause = statement.importClause;
  if (!clause) return null;
  const typePrefix = clause.isTypeOnly ? "type " : "";
  const parts: string[] = [];
  const head: string[] = [];
  if (clause.name && names.has(clause.name.text)) head.push(clause.name.text);
  const bindings = clause.namedBindings;
  if (bindings && ts.isNamespaceImport(bindings) && names.has(bindings.name.text)) {
    return `import ${typePrefix}* as ${bindings.name.text} from ${specifier};`;
  }
  if (bindings && ts.isNamedImports(bindings)) {
    for (const element of bindings.elements) {
      if (!names.has(element.name.text)) continue;
      const name = element.propertyName
        ? `${element.propertyName.text} as ${element.name.text}`
        : element.name.text;
      parts.push(element.isTypeOnly ? `type ${name}` : name);
    }
  }
  if (parts.length > 0) head.push(`{ ${parts.join(", ")} }`);
  if (head.length === 0) return null;
  return `import ${typePrefix}${head.join(", ")} from ${specifier};`;
}

export { UpgradeRefusal };
