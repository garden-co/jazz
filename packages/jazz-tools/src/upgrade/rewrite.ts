/**
 * Step 5 of `jazz-tools upgrade`: point the project's code at the new files.
 * - imports of schema.ts go to jazz.generated.ts (or app-extras.ts /
 *   jazz.computed.ts, for what moved there);
 * - imports of permissions.ts become `import permissions from
 *   "./jazz.permissions.json" with { type: "json" }`;
 * - `s.RowOf<typeof app.x>` (and `s.InsertOf`) become the generated types,
 *   other `s.RowOf<…>` become jazz-tools' `RowOf<…>`;
 * - `app.slice(...)` becomes `app`: schema slicing is gone.
 * Code still built on the schema DSL is reported, not rewritten.
 */
import { dirname, extname, join, relative, sep } from "node:path";
import { existsSync, readFileSync } from "node:fs";
import type * as TS from "typescript";
import { applyEdits, specifierLike, type TextEdit } from "./edits.js";
import { notSupportedYet } from "./errors.js";
import type { MovedExport } from "./extras.js";
import type { TableTypeNames } from "./names.js";
import type { Project } from "./project.js";
import type { PackageAliases } from "./aliases.js";

type Ts = typeof TS;

export interface RewriteInput {
  /** package.json `imports` aliases the rewrite retargets. */
  aliases: PackageAliases;
  project: Project;
  /** Where each export of schema.ts lives now. */
  exports: Map<string, MovedExport>;
  tables: Record<string, TableTypeNames>;
  /** Files upgrade deletes: never rewritten, and imports of them are what it rewrites. */
  deleted: Set<string>;
  generatedFile: string;
  extrasFile: string;
  computedFile: string;
}

export interface FileRewrite {
  file: string;
  text: string;
  /** One line per rewrite, `file:line: what`. */
  changes: string[];
  /** Whether the file now imports a JSON module. */
  importsJson: boolean;
}

export interface RewriteResult {
  files: FileRewrite[];
  /** Code upgrade can't move off the removed APIs, as `file:line: what`. */
  manual: string[];
}

const SCRIPT_BLOCK = /(<script\b[^>]*>)([\s\S]*?)(<\/script>)/g;

export function rewriteProject(input: RewriteInput): RewriteResult {
  const { project } = input;
  const files: FileRewrite[] = [];
  const manual: string[] = [];
  for (const file of project.sources) {
    if (input.deleted.has(file)) continue;
    const text = readFileSync(file, "utf8");
    const ext = extname(file);
    let out = text;
    const changes: string[] = [];
    let importsJson = false;
    const blocks: Array<{ offset: number; code: string }> = [];
    if (ext === ".svelte" || ext === ".vue" || ext === ".html") {
      for (const match of text.matchAll(SCRIPT_BLOCK)) {
        blocks.push({ offset: match.index! + match[1]!.length, code: match[2]! });
      }
    } else {
      blocks.push({ offset: 0, code: text });
    }
    const edits: TextEdit[] = [];
    for (const block of blocks) {
      const result = rewriteModule(input, file, block.code, block.offset, text);
      edits.push(
        ...result.edits.map((e) => ({
          ...e,
          start: e.start + block.offset,
          end: e.end + block.offset,
        })),
      );
      changes.push(...result.changes);
      manual.push(...result.manual);
      importsJson ||= result.importsJson;
    }
    if (edits.length === 0) continue;
    out = applyEdits(text, edits);
    files.push({ file, text: out, changes, importsJson });
  }
  return { files, manual };
}

interface ModuleResult {
  edits: TextEdit[];
  changes: string[];
  manual: string[];
  importsJson: boolean;
}

function rewriteModule(
  input: RewriteInput,
  file: string,
  code: string,
  offset: number,
  whole: string,
): ModuleResult {
  const { project } = input;
  const ts = project.ts;
  const kind = /\.[jt]sx$/.test(file) ? ts.ScriptKind.TSX : ts.ScriptKind.TS;
  const source = ts.createSourceFile(file, code, ts.ScriptTarget.Latest, true, kind);
  const edits: TextEdit[] = [];
  const changes: string[] = [];
  const manual: string[] = [];
  let importsJson = false;
  const line = (node: TS.Node) => lineOf(whole, offset + node.getStart(source));
  const at = (node: TS.Node) => `${shown(project.root, file)}:${line(node)}`;
  const quote = (spec: TS.StringLiteral) =>
    source.text[spec.getStart(source)] === "'" ? "'" : '"';

  /** Local names this module binds to the schema's `app`. */
  const appNames = new Set<string>();
  /** The specifier this module imports the generated module by, once known. */
  let generatedSpecifier: { text: string; quote: string } | undefined;
  /** Generated names this module needs that no import gives it yet. */
  const needGenerated = new Set<string>();
  const needJazzTypes = new Set<string>();

  for (const statement of source.statements) {
    const decl =
      ts.isImportDeclaration(statement) || ts.isExportDeclaration(statement)
        ? statement
        : undefined;
    const spec = decl?.moduleSpecifier;
    if (!decl || !spec || !ts.isStringLiteral(spec)) continue;
    const target = resolveImport(project, spec.text, file);
    if (!target) continue;
    const q = quote(spec);
    if (target === project.schemaFile) {
      const rewritten = rewriteSchemaImport(ts, input, decl, spec.text, q, file, at);
      edits.push({ start: decl.getStart(source), end: decl.getEnd(), text: rewritten.text });
      changes.push(`${at(decl)}: ${rewritten.summary}`);
      for (const name of rewritten.appNames) appNames.add(name);
      generatedSpecifier ??= {
        text: specifierFor(spec.text, input.generatedFile, file, project, input.aliases),
        quote: q,
      };
    } else if (project.permissionsSources.includes(target)) {
      const json = permissionsSpecifier(spec.text, file, project, input.aliases, target);
      const rewritten = rewritePermissionsImport(ts, decl, json, q, at);
      edits.push({ start: decl.getStart(source), end: decl.getEnd(), text: rewritten });
      changes.push(`${at(decl)}: permissions now come from jazz.permissions.json`);
      importsJson = true;
    } else if (project.migrationsDir && isWithin(target, project.migrationsDir)) {
      manual.push(
        `${at(decl)}: imports migrations/, which upgrade deletes; the app's history and migrations are in jazz.app.json now`,
      );
    } else if (input.deleted.has(target)) {
      manual.push(
        `${at(decl)}: imports ${shown(project.root, target)}, part of the schema upgrade deletes`,
      );
    }
  }

  // Dynamic import("./schema") and import("./permissions").
  const visitDynamic = (node: TS.Node): void => {
    if (
      ts.isCallExpression(node) &&
      node.expression.kind === ts.SyntaxKind.ImportKeyword &&
      node.arguments[0] &&
      ts.isStringLiteral(node.arguments[0])
    ) {
      const spec = node.arguments[0];
      const target = resolveImport(project, spec.text, file);
      if (target === project.schemaFile) {
        const next = specifierFor(spec.text, input.generatedFile, file, project, input.aliases);
        edits.push({ start: spec.getStart(source) + 1, end: spec.getEnd() - 1, text: next });
        changes.push(`${at(node)}: import("${spec.text}") → import("${next}")`);
      } else if (target && project.permissionsSources.includes(target)) {
        manual.push(
          `${at(node)}: a dynamic import of permissions.ts; import jazz.permissions.json instead`,
        );
      }
    }
    ts.forEachChild(node, visitDynamic);
  };
  visitDynamic(source);

  // The schema DSL in this module: `schema as s` from jazz-tools.
  const dslImports: Array<{
    decl: TS.ImportDeclaration;
    element: TS.ImportSpecifier;
    local: string;
  }> = [];
  for (const statement of source.statements) {
    if (!ts.isImportDeclaration(statement) || !ts.isStringLiteral(statement.moduleSpecifier))
      continue;
    const from = statement.moduleSpecifier.text;
    if (from !== "jazz-tools" && !from.startsWith("jazz-tools/")) continue;
    const named = statement.importClause?.namedBindings;
    if (!named || !ts.isNamedImports(named)) continue;
    for (const element of named.elements) {
      const imported = (element.propertyName ?? element.name).text;
      if (imported === "schema")
        dslImports.push({ decl: statement, element, local: element.name.text });
      if (
        imported === "definePermissions" ||
        imported === "defineMigration" ||
        imported === "migration"
      ) {
        manual.push(
          `${at(element)}: uses \`${imported}\` from ${from}, which alpha.66 removes; move this code to jazz.app.json / jazz.permissions.json`,
        );
      }
    }
  }
  const dslLocals = new Set(dslImports.map((d) => d.local));
  // `jazzAdapter` from jazz-tools/better-auth-adapter, under whatever local name.
  const adapterLocals = new Set<string>();
  for (const statement of source.statements) {
    if (!ts.isImportDeclaration(statement) || !ts.isStringLiteral(statement.moduleSpecifier))
      continue;
    if (statement.moduleSpecifier.text !== "jazz-tools/better-auth-adapter") continue;
    const named = statement.importClause?.namedBindings;
    if (named && ts.isNamedImports(named)) {
      for (const element of named.elements) {
        if ((element.propertyName ?? element.name).text === "jazzAdapter")
          adapterLocals.add(element.name.text);
      }
    }
  }
  const remainingDsl: TS.Node[] = [];
  const visit = (node: TS.Node): void => {
    // Better Auth's generation config: `jazzAdapter({ …, schema: s.defineApp({}, …).wasmSchema })`
    // only generates tables, and now passes `schema: {}`.
    if (
      ts.isPropertyAssignment(node) &&
      ts.isIdentifier(node.name) &&
      node.name.text === "schema" &&
      ts.isObjectLiteralExpression(node.parent) &&
      ts.isCallExpression(node.parent.parent) &&
      ts.isIdentifier(node.parent.parent.expression) &&
      adapterLocals.has(node.parent.parent.expression.text)
    ) {
      let value: TS.Expression = node.initializer;
      if (ts.isPropertyAccessExpression(value) && value.name.text === "wasmSchema")
        value = value.expression;
      if (
        ts.isCallExpression(value) &&
        ts.isPropertyAccessExpression(value.expression) &&
        ts.isIdentifier(value.expression.expression) &&
        dslLocals.has(value.expression.expression.text) &&
        value.expression.name.text === "defineApp" &&
        value.arguments[0] &&
        ts.isObjectLiteralExpression(value.arguments[0]) &&
        value.arguments[0].properties.length === 0
      ) {
        edits.push({
          start: node.initializer.getStart(source),
          end: node.initializer.getEnd(),
          text: "{}",
        });
        changes.push(
          `${at(node)}: Better Auth generation config: jazzAdapter's schema: ${node.initializer.getText(source).replace(/\s+/g, " ")} → {}`,
        );
        return;
      }
    }
    // s.RowOf<typeof app.x> and friends.
    if (
      ts.isTypeReferenceNode(node) &&
      ts.isQualifiedName(node.typeName) &&
      ts.isIdentifier(node.typeName.left) &&
      dslLocals.has(node.typeName.left.text)
    ) {
      const helper = node.typeName.right.text;
      const arg = node.typeArguments?.[0];
      const table = arg && tableOfTypeQuery(ts, arg, appNames, input.tables);
      if ((helper === "RowOf" || helper === "InsertOf") && table) {
        const name = input.tables[table]![helper === "RowOf" ? "row" : "insert"];
        edits.push({ start: node.getStart(source), end: node.getEnd(), text: name });
        needGenerated.add(name);
        changes.push(`${at(node)}: ${node.getText(source)} → ${name}`);
        return;
      }
      if (
        [
          "RowOf",
          "InsertOf",
          "WhereOf",
          "TableMetaOf",
          "LargeValueUpdateOf",
          "StreamingInsertOf",
          "StreamingUpdateOf",
          "StreamingUpsertOf",
        ].includes(helper)
      ) {
        edits.push({
          start: node.typeName.getStart(source),
          end: node.typeName.getEnd(),
          text: helper,
        });
        needJazzTypes.add(helper);
        changes.push(
          `${at(node)}: ${node.typeName.getText(source)}<…> → ${helper}<…> from jazz-tools`,
        );
        node.typeArguments?.forEach(visit);
        return;
      }
    }
    if (
      ts.isIdentifier(node) &&
      dslLocals.has(node.text) &&
      !isImportBinding(ts, node) &&
      !isPropertyName(ts, node)
    ) {
      remainingDsl.push(node);
    }
    // app.slice(...) → app.
    if (
      ts.isCallExpression(node) &&
      ts.isPropertyAccessExpression(node.expression) &&
      node.expression.name.text === "slice" &&
      ts.isIdentifier(node.expression.expression) &&
      appNames.has(node.expression.expression.text)
    ) {
      edits.push({
        start: node.getStart(source),
        end: node.getEnd(),
        text: node.expression.expression.text,
      });
      changes.push(
        `${at(node)}: ${node.getText(source).replace(/\s+/g, " ")} → ${node.expression.expression.text} (schema slicing is gone)`,
      );
      return;
    }
    if (ts.isIdentifier(node) && node.text === "defineSliceableApp") {
      manual.push(
        `${at(node)}: defineSliceableApp is gone with schema slicing; use the generated app`,
      );
    }
    ts.forEachChild(node, visit);
  };
  visit(source);

  if (remainingDsl.length > 0) {
    const first = remainingDsl[0]!;
    const usage = ts.isPropertyAccessExpression(first.parent)
      ? first.parent.getText(source)
      : first.getText(source);
    manual.push(
      `${at(first)}: still builds with the schema DSL (\`${usage.replace(/\s+/g, " ").slice(0, 60)}\`), which alpha.66 removes; upgrade converts schema.ts only, so move this to app files by hand`,
    );
  } else {
    for (const { decl, element } of dslImports) {
      edits.push(...removeImportElement(ts, source, decl, element));
      changes.push(`${at(decl)}: dropped the schema DSL import`);
    }
  }

  // Imports the rewrites above need.
  if (needGenerated.size > 0 || needJazzTypes.size > 0) {
    const lines: string[] = [];
    if (needJazzTypes.size > 0)
      lines.push(`import type { ${[...needJazzTypes].sort().join(", ")} } from "jazz-tools";`);
    if (needGenerated.size > 0) {
      const spec = generatedSpecifier ?? {
        text: relativeSpecifier(file, input.generatedFile, project),
        quote: '"',
      };
      lines.push(
        `import type { ${[...needGenerated].sort().join(", ")} } from ${spec.quote}${spec.text}${spec.quote};`,
      );
    }
    const anchor = lastImport(ts, source);
    const position = anchor ? anchor.getEnd() : 0;
    edits.push({
      start: position,
      end: position,
      text: anchor ? `\n${lines.join("\n")}` : `${lines.join("\n")}\n`,
    });
  }
  return { edits, changes, manual, importsJson };
}

function rewriteSchemaImport(
  ts: Ts,
  input: RewriteInput,
  decl: TS.ImportDeclaration | TS.ExportDeclaration,
  specifier: string,
  q: string,
  file: string,
  at: (node: TS.Node) => string,
): { text: string; summary: string; appNames: string[] } {
  const { project } = input;
  const moduleFile = {
    generated: input.generatedFile,
    extras: input.extrasFile,
    computed: input.computedFile,
  };
  const spec = (module: keyof typeof moduleFile) =>
    specifierFor(specifier, moduleFile[module], file, project, input.aliases);
  const isExport = ts.isExportDeclaration(decl);
  const clauseTypeOnly = isExport ? decl.isTypeOnly : (decl.importClause?.isTypeOnly ?? false);
  let elements: readonly TS.ImportSpecifier[] | readonly TS.ExportSpecifier[] = [];
  if (ts.isImportDeclaration(decl)) {
    const clause = decl.importClause;
    if (!clause) {
      return {
        text: `import ${q}${spec("generated")}${q};`,
        summary: `side-effect import → ${spec("generated")}`,
        appNames: [],
      };
    }
    if (clause.name) throw notSupportedYet("a default import of schema.ts", at(decl));
    const bindings = clause.namedBindings;
    if (bindings && ts.isNamespaceImport(bindings)) {
      return {
        text: `import ${clauseTypeOnly ? "type " : ""}* as ${bindings.name.text} from ${q}${spec("generated")}${q};`,
        summary: `${specifier} → ${spec("generated")}`,
        appNames: [],
      };
    }
    elements = bindings && ts.isNamedImports(bindings) ? bindings.elements : [];
  } else {
    if (!decl.exportClause || !ts.isNamedExports(decl.exportClause)) {
      return {
        text: `export * from ${q}${spec("generated")}${q};`,
        summary: `${specifier} → ${spec("generated")}`,
        appNames: [],
      };
    }
    elements = decl.exportClause.elements;
  }
  const groups = new Map<keyof typeof moduleFile, string[]>();
  const appNames: string[] = [];
  for (const element of elements) {
    const imported = (element.propertyName ?? element.name).text;
    const local = element.name.text;
    const moved = input.exports.get(imported);
    if (!moved) {
      throw notSupportedYet(
        `importing \`${imported}\` from schema.ts, which upgrade can't place (it isn't an export it moves)`,
        at(element),
      );
    }
    if (moved.module === "generated" && moved.name === "app") appNames.push(local);
    const name = moved.name === local ? local : `${moved.name} as ${local}`;
    const typeOnly = !clauseTypeOnly && element.isTypeOnly;
    const list = groups.get(moved.module) ?? [];
    list.push(typeOnly ? `type ${name}` : name);
    groups.set(moved.module, list);
  }
  const keyword = isExport ? "export" : "import";
  const typeKeyword = clauseTypeOnly ? "type " : "";
  const lines: string[] = [];
  const order: Array<keyof typeof moduleFile> = ["generated", "extras", "computed"];
  for (const module of order) {
    const names = groups.get(module);
    if (!names) continue;
    lines.push(`${keyword} ${typeKeyword}{ ${names.join(", ")} } from ${q}${spec(module)}${q};`);
  }
  if (lines.length === 0)
    lines.push(`${keyword} ${typeKeyword}{} from ${q}${spec("generated")}${q};`);
  return {
    text: lines.join("\n"),
    summary: `${specifier} → ${order
      .filter((m) => groups.has(m))
      .map(spec)
      .join(" + ")}`,
    appNames,
  };
}

function rewritePermissionsImport(
  ts: Ts,
  decl: TS.ImportDeclaration | TS.ExportDeclaration,
  specifier: string,
  q: string,
  at: (node: TS.Node) => string,
): string {
  const json = specifier;
  if (ts.isImportDeclaration(decl)) {
    const clause = decl.importClause;
    if (!clause?.name || clause.namedBindings) {
      throw notSupportedYet(
        "importing anything but permissions.ts's default export",
        at(decl),
        'import the default (`import permissions from "./permissions"`) and run upgrade again',
      );
    }
    return `import ${clause.name.text} from ${q}${json}${q} with { type: ${q}json${q} };`;
  }
  const exportClause = decl.exportClause;
  if (
    exportClause &&
    ts.isNamedExports(exportClause) &&
    exportClause.elements.every((e) => (e.propertyName ?? e.name).text === "default")
  ) {
    const names = exportClause.elements.map((e) =>
      e.name.text === "default" ? "default" : `default as ${e.name.text}`,
    );
    return `export { ${names.join(", ")} } from ${q}${json}${q} with { type: ${q}json${q} };`;
  }
  throw notSupportedYet("re-exporting anything but permissions.ts's default export", at(decl));
}

function removeImportElement(
  ts: Ts,
  source: TS.SourceFile,
  decl: TS.ImportDeclaration,
  element: TS.ImportSpecifier,
): TextEdit[] {
  const named = decl.importClause!.namedBindings as TS.NamedImports;
  if (named.elements.length === 1 && !decl.importClause!.name) {
    // The whole statement, with its line.
    let end = decl.getEnd();
    if (source.text[end] === "\n") end += 1;
    return [{ start: decl.getStart(source), end, text: "" }];
  }
  const index = named.elements.indexOf(element);
  const next = named.elements[index + 1];
  const previous = named.elements[index - 1];
  if (next) return [{ start: element.getStart(source), end: next.getStart(source), text: "" }];
  return [{ start: previous!.getEnd(), end: element.getEnd(), text: "" }];
}

function lastImport(ts: Ts, source: TS.SourceFile): TS.Statement | undefined {
  let last: TS.Statement | undefined;
  for (const statement of source.statements) {
    if (ts.isImportDeclaration(statement)) last = statement;
  }
  return last;
}

function tableOfTypeQuery(
  ts: Ts,
  node: TS.TypeNode,
  appNames: Set<string>,
  tables: Record<string, TableTypeNames>,
): string | undefined {
  if (!ts.isTypeQueryNode(node) || !ts.isQualifiedName(node.exprName)) return undefined;
  const left = node.exprName.left;
  if (!ts.isIdentifier(left) || !appNames.has(left.text)) return undefined;
  const table = node.exprName.right.text;
  return tables[table] ? table : undefined;
}

function isImportBinding(ts: Ts, node: TS.Identifier): boolean {
  return (
    ts.isImportSpecifier(node.parent) ||
    ts.isImportClause(node.parent) ||
    ts.isNamespaceImport(node.parent)
  );
}

function isPropertyName(ts: Ts, node: TS.Identifier): boolean {
  const parent = node.parent;
  return (
    (ts.isPropertyAccessExpression(parent) && parent.name === node) ||
    (ts.isPropertyAssignment(parent) && parent.name === node) ||
    (ts.isQualifiedName(parent) && parent.right === node)
  );
}

/** The file a module specifier of `file` resolves to, if it is one of the project's. */
export function resolveImport(
  project: Project,
  specifier: string,
  file: string,
): string | undefined {
  const ts = project.ts;
  const resolved = ts.resolveModuleName(
    specifier,
    file,
    project.compilerOptions,
    ts.sys,
  ).resolvedModule;
  if (resolved && !resolved.isExternalLibraryImport) return resolved.resolvedFileName;
  if (!specifier.startsWith(".")) return undefined;
  // Without the tsconfig's help (`.js` for `.ts`, extensionless, `index`).
  const base = join(dirname(file), specifier);
  const stem = base.replace(/\.(js|mjs|cjs|jsx|ts|mts|tsx)$/, "");
  for (const candidate of [base, `${stem}.ts`, `${stem}.tsx`, join(base, "index.ts")]) {
    if (existsSync(candidate) && !candidate.endsWith(sep)) return candidate;
  }
  return undefined;
}

/** The specifier for `target`, written in the style of `like` when it named the old file by basename. */
function specifierFor(
  like: string,
  target: string,
  file: string,
  project: Project,
  aliases?: PackageAliases,
  from: string = project.schemaFile,
): string {
  if (like.startsWith("#")) {
    if (aliases?.retarget(like, file, from, target)) return like;
    return relativeSpecifier(file, target, project);
  }
  const base = target.slice(target.lastIndexOf(sep) + 1).replace(/\.ts$/, "");
  const last = like.slice(like.lastIndexOf("/") + 1).replace(/\.(js|ts|mjs|mts|jsx|tsx)$/, "");
  if (last === "schema" || last === "permissions") return specifierLike(like, base);
  return relativeSpecifier(file, target, project, like.endsWith(".js"));
}

/** How `file` imports jazz.permissions.json, in the style it imported permissions.ts. */
function permissionsSpecifier(
  like: string,
  file: string,
  project: Project,
  aliases: PackageAliases,
  from: string,
): string {
  const target = project.permissionsJsonFile;
  if (like.startsWith("#")) {
    if (aliases.retarget(like, file, from, target)) return like;
  } else {
    const last = like.slice(like.lastIndexOf("/") + 1).replace(/\.(js|ts|mjs|mts)$/, "");
    if (last === "permissions" && from === project.permissionsFile) {
      return specifierLike(like, "jazz.permissions.json", false);
    }
  }
  let path = relative(dirname(file), target).split(sep).join("/");
  if (!path.startsWith(".")) path = `./${path}`;
  return path;
}

function relativeSpecifier(file: string, target: string, _project: Project, js = false): string {
  let path = relative(dirname(file), target)
    .split(sep)
    .join("/")
    .replace(/\.ts$/, js ? ".js" : "");
  if (!path.startsWith(".")) path = `./${path}`;
  return path;
}

function isWithin(path: string, dir: string): boolean {
  const rel = relative(dir, path);
  return rel !== "" && !rel.startsWith("..");
}

function lineOf(text: string, position: number): number {
  let line = 1;
  for (let i = 0; i < position && i < text.length; i++) if (text.charCodeAt(i) === 10) line++;
  return line;
}

function shown(root: string, file: string): string {
  return relative(root, file) || file;
}
