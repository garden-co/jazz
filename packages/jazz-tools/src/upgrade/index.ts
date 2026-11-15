/**
 * `jazz-tools upgrade`: convert an alpha.64 app (schema.ts, permissions.ts,
 * migrations/) to alpha.65, where jazz.app.json and jazz.permissions.json
 * are the only source and jazz.generated.ts holds the typed bindings.
 *
 * 1. The app's own jazz-tools (≤ alpha.64) must find its app files current.
 * 2. TS-only columns become `computed` fields and `transforms`, their
 *    callbacks moved unchanged into jazz.computed.ts.
 * 3. jazz.app.json must already carry migrations/'s history and migrations.
 * 4. schema.ts, permissions.ts and migrations/ are deleted (and helper files
 *    only they used); what else schema.ts exported moves to app-extras.ts.
 * 5. Imports are rewritten to the new files; slicing goes.
 * 6. jazz.generated.ts is generated and the project typechecked, errors
 *    reported as file:line and left in place.
 * 7. A closing note on factoring repeated permissions into `defs`.
 *
 * Every refusal comes before the first write.
 */
import {
  existsSync,
  readdirSync,
  readFileSync,
  realpathSync,
  rmSync,
  rmdirSync,
  writeFileSync,
} from "node:fs";
import { createRequire } from "node:module";
import { dirname, extname, join, relative, resolve } from "node:path";
import type * as TS from "typescript";
import {
  appTables,
  editAppJson,
  isObject,
  parseAppJson,
  printAppJson,
  tableColumns,
  tsOnlyMarkers,
  type Json,
} from "./app-json.js";
import { convertComputed } from "./computed.js";
import { UpgradeRefusal, notSupportedYet } from "./errors.js";
import { convertExtras } from "./extras.js";
import { checkLegacyFiles, dslAlpha, findLegacyJazzTools } from "./legacy.js";
import { checkMigrations, shippedHashes, straySnapshots } from "./migrations.js";
import { pascal, readGeneratedNames, rowTypeName, type TableTypeNames } from "./names.js";
import { createProgram, typeErrors } from "./program.js";
import {
  COMPUTED_FILE,
  EXTRAS_FILE,
  GENERATED_FILE,
  locateProject,
  packageVersion,
  runningPackageRoot,
  type Project,
} from "./project.js";
import { refuseDroppedConditions, refuseEnvironmentReads } from "./permissions-check.js";
import { resolveImport, rewriteProject } from "./rewrite.js";
import { PackageAliases } from "./aliases.js";

export { UpgradeRefusal } from "./errors.js";

export interface UpgradeOptions {
  /** The app's directory (default: cwd). */
  dir?: string;
  /** Where schema.ts is, when not in `dir`, `dir/src` or `dir/src/lib` (and not found below `dir`). */
  schemaDir?: string;
  /** Plan and report, write nothing. */
  dryRun?: boolean;
  /** Skip the closing typecheck. */
  skipTypecheck?: boolean;
  /** jazz.generated.ts from jazz.app.json's text (default: jazz-napi's `generate`). */
  generate?: (appJson: string) => string | Promise<string>;
}

export interface UpgradeReport {
  dir: string;
  /** Files written, relative to `dir`. */
  written: string[];
  /** Files deleted, relative to `dir`. */
  deleted: string[];
  /** Every rewrite, as `file:line: what`. */
  rewrites: string[];
  /** Code upgrade left for the author, as `file:line: what`. */
  manual: string[];
  /** Decisions and guesses worth a look. */
  notes: string[];
  /** Typecheck errors after the conversion, as `file:line:col: message`. */
  typeErrors: string[];
  /** Typecheck errors the project had before the conversion too: reported, not counted. */
  preexistingTypeErrors: string[];
  /** Whether the typecheck ran. */
  typechecked: boolean;
  /** `"TODO: …"` strings left in jazz.app.json. */
  todos: number;
}

export async function upgradeProject(options: UpgradeOptions = {}): Promise<UpgradeReport> {
  const project = locateProject(options.dir ?? process.cwd(), options.schemaDir);
  const { ts } = project;
  const rel = (path: string) => relative(project.root, path) || path;
  const notes: string[] = [];

  // 1. The app files must be current.
  const legacy = findLegacyJazzTools(project);
  notes.push(...checkLegacyFiles(project, legacy));
  const scan = scanImports(project);
  // Fail closed on policies alpha.64 compiled from values the source doesn't hold.
  refuseEnvironmentReads(project, schemaSources(project, scan));
  refuseDroppedConditions(project);

  const appText = readFileSync(project.appFile, "utf8");
  const app = parseAppJson(appText);
  const tables = appTables(app);
  const markers = tsOnlyMarkers(app);

  // 2. TS-only columns → computed / transforms.
  const schemaProgram = createProgram(ts, [project.schemaFile], project.compilerOptions, undefined);
  let computed;
  let extras;
  try {
    computed = convertComputed({
      ts,
      program: schemaProgram.program,
      schemaFile: project.schemaFile,
      tables,
      markers,
    });
    notes.push(...computed.notes);
    const edited = editAppJson(app, computed);
    if (edited.droppedSlices.length > 0) {
      notes.push(
        `Dropped \`slices\` (${edited.droppedSlices.join(", ")}) from jazz.app.json: schema slicing is gone.`,
      );
    }

    // 3. Migrations are in the file already.
    const migrations = checkMigrations(app, project.migrationsDir);
    if (migrations.unshipped.length > 0) {
      notes.push(
        `Deleted with migrations/, never shipped (no snapshot of their versions): ${migrations.unshipped.join(", ")}.`,
      );
    }
    if (migrations.versions > 1) {
      notes.push(
        `jazz.app.json carries ${migrations.versions} schema versions and ${migrations.migrations} migration${migrations.migrations === 1 ? "" : "s"}.`,
      );
    }

    var newAppText = printAppJson(edited.app);
    // 6a. The bindings, from the edited file (unless TODOs block them).
    var generatedText: string | null = null;
    if (computed.todos === 0) {
      try {
        generatedText = await (options.generate ?? napiGenerate)(newAppText);
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        throw new UpgradeRefusal(
          `generating jazz.generated.ts from the converted jazz.app.json failed: ${message}`,
        );
      }
    }
    var names = generatedText ? readGeneratedNames(generatedText) : fallbackNames(edited.app);

    // 4. What schema.ts exported besides the schema.
    extras = convertExtras({
      ts,
      program: schemaProgram.program,
      schemaFile: project.schemaFile,
      tables: names.tables,
      generatedExports: names.exports,
      movedToComputed: computed.movedExports,
      jsExtensions: scan.jsExtensions,
      importedNames: scan.schemaNames,
      notes,
    });
    for (const name of extras.dropped)
      notes.push(
        `Dropped \`${name}\` from schema.ts's exports: it was schema DSL, and nothing imports it.`,
      );
    var deleted = deletedFiles(project, scan);
    const versions = new Set([
      ...(isObject(app.history) ? Object.keys(app.history) : []),
      ...(project.migrationsDir ? shippedHashes(project.migrationsDir) : []),
    ]);
    const stray = straySnapshots(project.root, project.migrationsDir, versions);
    for (const file of stray) deleted.add(file);
    if (stray.length > 0) {
      notes.push(
        `Deleted this app's schema snapshots outside migrations/ too: ${stray.map((f) => relative(project.root, f)).join(", ")}.`,
      );
    }
  } finally {
    schemaProgram.dispose();
  }

  // 5. Imports.
  const generatedFile = join(project.schemaDir, GENERATED_FILE);
  const extrasFile = join(project.schemaDir, EXTRAS_FILE);
  const computedFile = join(project.schemaDir, COMPUTED_FILE);
  for (const path of [extrasFile, computedFile, generatedFile]) {
    if (existsSync(path) && !deleted.has(path)) {
      throw new UpgradeRefusal(
        `${rel(path)} exists already; upgrade writes it: move it away and run upgrade again`,
      );
    }
  }
  const aliases = new PackageAliases(project.root);
  const rewrites = rewriteProject({
    aliases,
    project,
    exports: extras.exports,
    tables: names.tables,
    deleted,
    generatedFile,
    extrasFile,
    computedFile,
  });
  const config = configEdits(
    project,
    rewrites.files.some((f) => f.importsJson),
    deleted,
  );
  const manifest = manifestEdit(project, deleted);
  // package.json `imports` aliases, on top of the root manifest's edits.
  const aliasEdits = aliases.apply(
    manifest ? new Map([[manifest.file, manifest.text]]) : undefined,
  );

  const report: UpgradeReport = {
    dir: project.root,
    written: [],
    deleted: [...deleted].map(rel).sort(),
    rewrites: [
      ...rewrites.files.flatMap((f) => f.changes),
      ...config.changes,
      ...(manifest?.changes ?? []),
      ...[...aliasEdits.values()].flatMap((e) => e.changes),
    ],
    manual: rewrites.manual,
    notes,
    typeErrors: [],
    preexistingTypeErrors: [],
    typechecked: false,
    todos: computed.todos,
  };
  report.notes.push(...defsNote(project));

  const writes: Array<[string, string]> =
    newAppText === appText ? [] : [[project.appFile, newAppText]];
  if (computed.computedFile) writes.push([computedFile, computed.computedFile]);
  if (extras.extrasFile) writes.push([extrasFile, extras.extrasFile]);
  // A Rust app (a crate on `jazz-app`) gets Rust bindings instead, as `jazz-tools generate` writes them.
  const rustApp = isRustApp(project.schemaDir);
  if (generatedText && !rustApp) writes.push([generatedFile, generatedText]);
  if (rustApp && computed.todos === 0) {
    const rust = await rustBindings(newAppText);
    if (typeof rust === "string") writes.push([join(project.schemaDir, RUST_BINDINGS_FILE), rust]);
    else {
      report.notes.push(
        `${RUST_BINDINGS_FILE} was not written (${rust.reason}); run \`npx jazz-tools generate\` once this jazz-tools generates Rust bindings.`,
      );
    }
  }
  for (const file of rewrites.files) writes.push([file.file, file.text]);
  if (config.file) writes.push([config.file, config.text]);
  if (manifest && !aliasEdits.has(manifest.file)) writes.push([manifest.file, manifest.text]);
  for (const [file, edit] of aliasEdits) writes.push([file, edit.text]);
  report.written = writes.map(([path]) => rel(path));

  if (options.dryRun) return report;

  // The project's errors before the conversion, against its own jazz-tools,
  // so the closing typecheck tells upgrade's errors from ones it found there.
  let before: Map<string, number> | undefined;
  if (!options.skipTypecheck && !isRustApp(project.schemaDir)) {
    const roots = new Set(project.configFileNames);
    for (const file of rewrites.files)
      if (/\.(ts|tsx|mts|cts)$/.test(file.file)) roots.add(file.file);
    if (roots.size > 0) {
      const handle = createProgram(ts, [...roots], project.compilerOptions, undefined);
      try {
        before = countTypeErrors(typeErrors(ts, handle.program, project.root));
      } finally {
        handle.dispose();
      }
    }
  }

  for (const [path, text] of writes) writeFileSync(path, text);
  for (const path of deleted) rmSync(path, { force: true });
  removeEmptyDirs([...deleted].map(dirname), project.root);

  if (computed.todos > 0) {
    report.notes.push(
      `jazz.app.json has ${computed.todos} "TODO: …" upgrade couldn't decide. Resolve them, then run \`npx jazz-tools generate\` to write jazz.generated.ts, and typecheck.`,
    );
    return report;
  }
  // 6b. Typecheck against this jazz-tools (a Rust app has no TS to check).
  if (rustApp)
    report.notes.push(
      "This is a Rust app: run `cargo check` to typecheck it against the new bindings.",
    );
  if (!options.skipTypecheck && !rustApp) {
    const roots = new Set(project.configFileNames.filter((f) => !deleted.has(f)));
    for (const [path] of writes) if (/\.(ts|tsx|mts|cts)$/.test(path)) roots.add(path);
    const runtime = runningPackageRoot();
    const installedNow = installedJazzTools(project);
    const override =
      installedNow &&
      dslAlpha(packageVersion(installedNow)) !== null &&
      realpathSync(installedNow) !== realpathSync(runtime)
        ? runtime
        : undefined;
    const handle = createProgram(ts, [...roots], project.compilerOptions, override);
    try {
      const split = splitTypeErrors(before, typeErrors(ts, handle.program, project.root));
      report.typeErrors = split.added;
      report.preexistingTypeErrors = split.preexisting;
      report.typechecked = true;
    } finally {
      handle.dispose();
    }
    const views = project.sources.filter((f) => /\.(svelte|vue)$/.test(f));
    if (views.length > 0) {
      report.notes.push(
        `The typecheck covers .ts/.tsx; run svelte-check or vue-tsc for the ${views.length} .svelte/.vue file${views.length === 1 ? "" : "s"}.`,
      );
    }
  }
  return report;
}

async function napiGenerate(appJson: string): Promise<string> {
  const napi = (await import("jazz-napi")) as unknown as {
    generate?: (lang: string, app: string) => string;
  };
  if (typeof napi.generate !== "function") {
    throw new Error(
      "this jazz-napi has no `generate`: install the jazz-napi that ships with this jazz-tools",
    );
  }
  return napi.generate("ts", appJson);
}

const RUST_BINDINGS_FILE = join("src", "jazz_app.rs");

/**
 * Whether `dir` is a Rust app, as `jazz-tools generate` tells: a crate (a
 * `Cargo.toml` with `[package]`) that depends on `jazz-app`, or one with
 * committed `src/jazz_app.rs`. A workspace root or an unrelated crate isn't.
 */
export function isRustApp(dir: string): boolean {
  const manifest = join(dir, "Cargo.toml");
  if (!existsSync(manifest)) return false;
  const text = readFileSync(manifest, "utf8");
  if (!/^\s*\[package\]/m.test(text)) return false;
  if (existsSync(join(dir, RUST_BINDINGS_FILE))) return true;
  return /^\s*jazz-app\s*=|^\s*\[(?:dev-|build-)?dependencies\.jazz-app\]/m.test(text);
}

/** The `jazz-app` crate's bindings, or why this jazz-napi can't generate them. */
async function rustBindings(appJson: string): Promise<string | { reason: string }> {
  try {
    const napi = (await import("jazz-napi")) as unknown as {
      generate?: (lang: string, app: string) => string;
    };
    if (typeof napi.generate !== "function") return { reason: "this jazz-napi has no `generate`" };
    return napi.generate("rust", appJson);
  } catch (error) {
    return {
      reason: (error instanceof Error ? error.message : String(error)).replace(/[.\s]*$/, ""),
    };
  }
}

/** The project's resolved jazz-tools package directory, if installed. */
function installedJazzTools(project: Project): string | undefined {
  try {
    return dirname(
      createRequire(join(project.schemaDir, "package.json")).resolve("jazz-tools/package.json"),
    );
  } catch {
    return undefined;
  }
}

/** Generated names by jazz-codegen's rules, for a file whose TODOs block generating. */
function fallbackNames(app: { [key: string]: Json }): {
  tables: Record<string, TableTypeNames>;
  exports: Set<string>;
} {
  const tables: Record<string, TableTypeNames> = {};
  const exports = new Set<string>(["app"]);
  for (const [name, table] of Object.entries(appTables(app))) {
    const row = rowTypeName(name);
    tables[name] = { row, insert: `${row}Insert` };
    exports.add(row).add(`${row}Insert`);
    for (const [column, spec] of Object.entries(tableColumns(table))) {
      if (isObject(spec) && "enum" in spec) exports.add(`${row}${pascal(column)}`);
    }
  }
  return { tables, exports };
}

interface ImportScan {
  /** Per file, the project files it imports. */
  imports: Map<string, Set<string>>;
  /** Names imported from schema.ts by files other than permissions.ts. */
  schemaNames: Set<string>;
  /** Whether relative imports of schema.ts carry `.js`. */
  jsExtensions: boolean;
}

function scanImports(project: Project): ImportScan {
  const { ts } = project;
  const imports = new Map<string, Set<string>>();
  const schemaNames = new Set<string>();
  let js = 0;
  let plain = 0;
  for (const file of project.sources) {
    const text = readFileSync(file, "utf8");
    const code = /\.(svelte|vue|html)$/.test(file)
      ? [...text.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/g)].map((m) => m[1]).join("\n")
      : text;
    const targets = new Set<string>();
    const source = ts.createSourceFile(
      file,
      code,
      ts.ScriptTarget.Latest,
      true,
      /\.[jt]sx$/.test(file) ? ts.ScriptKind.TSX : ts.ScriptKind.TS,
    );
    const visit = (node: TS.Node): void => {
      let spec: TS.Expression | undefined;
      if ((ts.isImportDeclaration(node) || ts.isExportDeclaration(node)) && node.moduleSpecifier)
        spec = node.moduleSpecifier;
      if (ts.isCallExpression(node) && node.expression.kind === ts.SyntaxKind.ImportKeyword)
        spec = node.arguments[0];
      if (spec && ts.isStringLiteral(spec)) {
        const target = resolveImport(project, spec.text, file);
        if (target) {
          targets.add(target);
          if (target === project.schemaFile && !project.permissionsSources.includes(file)) {
            if (spec.text.startsWith(".")) spec.text.endsWith(".js") ? js++ : plain++;
            if (ts.isImportDeclaration(node)) {
              const bindings = node.importClause?.namedBindings;
              if (bindings && ts.isNamedImports(bindings)) {
                for (const element of bindings.elements)
                  schemaNames.add((element.propertyName ?? element.name).text);
              }
            }
          }
        }
      }
      ts.forEachChild(node, visit);
    };
    visit(source);
    imports.set(file, targets);
  }
  return { imports, schemaNames, jsExtensions: js > plain };
}

/** schema.ts, permissions.ts and the project files they import, transitively. */
function schemaSources(project: Project, scan: ImportScan): string[] {
  const out = new Set<string>();
  const queue = [project.schemaFile, ...project.permissionsSources];
  while (queue.length > 0) {
    const file = queue.pop()!;
    if (
      out.has(file) ||
      relative(project.root, file).startsWith("..") ||
      file.includes("node_modules")
    )
      continue;
    out.add(file);
    queue.push(...(scan.imports.get(file) ?? []));
  }
  return [...out];
}

const DSL_IMPORT =
  /import\s*(?:type\s*)?\{[^}]*\b(?:schema|definePermissions)\b[^}]*\}\s*from\s*["']jazz-tools(?:\/permissions)?["']/;

/**
 * schema.ts, permissions.ts, migrations/, and the schema DSL files only they
 * import (generated Better Auth tables, say): once nothing else imports them.
 */
function deletedFiles(project: Project, scan: ImportScan): Set<string> {
  const deleted = new Set<string>([project.schemaFile]);
  for (const file of project.permissionsSources) deleted.add(file);
  if (project.migrationsDir) {
    const walk = (dir: string): void => {
      for (const entry of readdirSync(dir, { withFileTypes: true })) {
        const path = join(dir, entry.name);
        if (entry.isDirectory()) walk(path);
        else deleted.add(path);
      }
    };
    walk(project.migrationsDir);
  }
  // Grow the set while a DSL file is imported only by deleted files.
  let changed = true;
  while (changed) {
    changed = false;
    for (const [file] of scan.imports) {
      if (deleted.has(file)) continue;
      const importers = [...scan.imports]
        .filter(([, targets]) => targets.has(file))
        .map(([f]) => f);
      if (importers.length === 0 || !importers.every((f) => deleted.has(f))) continue;
      if (!DSL_IMPORT.test(readFileSync(file, "utf8"))) continue;
      deleted.add(file);
      changed = true;
    }
  }
  return deleted;
}

function removeEmptyDirs(dirs: string[], root: string): void {
  for (const start of new Set(dirs)) {
    let dir = start;
    while (
      dir.startsWith(root) &&
      dir !== root &&
      existsSync(dir) &&
      readdirSync(dir).length === 0
    ) {
      rmdirSync(dir);
      dir = dirname(dir);
    }
  }
}

/** tsconfig: `include` entries naming deleted files, and JSON imports. */
function configEdits(
  project: Project,
  importsJson: boolean,
  deleted: Set<string>,
): { file?: string; text: string; changes: string[] } {
  const file = project.tsconfig;
  if (!file) return { text: "", changes: [] };
  let text = readFileSync(file, "utf8");
  const original = text;
  const changes: string[] = [];
  const dir = dirname(file);
  const generated = relative(dir, join(project.schemaDir, GENERATED_FILE)).split("\\").join("/");
  for (const path of deleted) {
    if (extname(path) !== ".ts") continue;
    const name = relative(dir, path).split("\\").join("/");
    for (const spelled of [name, `./${name}`]) {
      const quoted = JSON.stringify(spelled);
      if (!text.includes(quoted)) continue;
      if (path === project.schemaFile) {
        text = text.replace(quoted, JSON.stringify(generated));
        changes.push(`${relative(project.root, file)}: include ${quoted} → "${generated}"`);
      } else {
        text = text.replace(
          new RegExp(`\\s*${escape(quoted)}\\s*,|,\\s*${escape(quoted)}|${escape(quoted)}`),
          "",
        );
        changes.push(`${relative(project.root, file)}: dropped ${quoted} from include`);
      }
    }
  }
  if (importsJson && project.compilerOptions.resolveJsonModule !== true) {
    const next = text.replace(
      /("compilerOptions"\s*:\s*\{)(\s*)/,
      `$1$2"resolveJsonModule": true,$2`,
    );
    if (next !== text) {
      text = next;
      changes.push(
        `${relative(project.root, file)}: "resolveJsonModule": true, for the jazz.permissions.json import`,
      );
    }
  }
  return text === original ? { text, changes: [] } : { file, text, changes };
}

function escape(text: string): string {
  return text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/**
 * package.json: scripts calling `jazz-tools validate` call `generate` (its
 * alpha.65 name), and a pinned alpha jazz-tools / jazz-napi moves to this one.
 */
function manifestEdit(
  project: Project,
  deleted: Set<string>,
): { file: string; text: string; changes: string[] } | undefined {
  const file = join(project.root, "package.json");
  if (!existsSync(file)) return undefined;
  const text = readFileSync(file, "utf8");
  const changes: string[] = [];
  let next = text;
  let manifest: { scripts?: Record<string, string> };
  try {
    manifest = JSON.parse(text);
  } catch {
    return undefined;
  }
  for (const [name, script] of Object.entries(manifest.scripts ?? {})) {
    let renamed = renameValidate(script);
    if (renamed !== script) {
      changes.push(
        `package.json: script "${name}": \`jazz-tools validate\` → \`jazz-tools generate\``,
      );
    }
    const retargeted = retargetAuthOutput(renamed, project, deleted);
    if (retargeted !== renamed) {
      changes.push(
        `package.json: script "${name}": the Better Auth CLI now writes its tables into ${relative(project.root, project.appFile)}`,
      );
      renamed = retargeted;
    }
    if (renamed === script) continue;
    const quoted = JSON.stringify(script);
    if (!next.includes(quoted)) continue;
    next = next.replace(quoted, JSON.stringify(renamed));
  }
  const version = packageVersion(runningPackageRoot());
  // A pre-release build of upgrade itself has no version to pin.
  if (dslAlpha(version) === null) {
    next = next.replace(
      /("jazz-(?:tools|napi)"\s*:\s*")([~^]?)(2\.0\.0-alpha\.\d+)"/g,
      (whole, head: string, range: string, old: string) => {
        if (dslAlpha(old) === null) return whole;
        changes.push(
          `package.json: ${head.match(/"([^"]+)"/)![1]} ${range}${old} → ${range}${version}`,
        );
        return `${head}${range}${version}"`;
      },
    );
  }
  return next === text ? undefined : { file, text: next, changes };
}

/**
 * A Better Auth CLI `generate` that wrote its DSL module (`--output
 * ./schema-better-auth/schema.ts`, which upgrade deletes) writes jazz.app.json
 * now: the adapter merges its tables into the app file (by default
 * ./jazz.app.json, relative to where the script runs).
 */
export function retargetAuthOutput(script: string, project: Project, deleted: Set<string>): string {
  if (!/\bgenerate\b/.test(script) || !/(?:better-auth\/cli|\bauth(?:@\S+)?)\s/.test(script))
    return script;
  return script.replace(
    /(\s)--output(?:=|\s+)(["']?)([^\s"']+\.ts)\2/g,
    (whole, space: string, _q, path: string) => {
      if (!deleted.has(resolve(project.root, path))) return whole;
      const app = relative(project.root, project.appFile).split("\\").join("/");
      return app === "jazz.app.json" ? "" : `${space}--output ./${app}`;
    },
  );
}

/** A shell command with every jazz-tools CLI `validate` call renamed to `generate`. */
export function renameValidate(script: string): string {
  return script.replace(
    /((?:^|[\s;&|('"])(?:(?:npx|pnpm(?: exec)?|yarn|bunx)\s+)?(?:jazz-tools(?:@\S+)?|\S*jazz-tools\/(?:dist\/cli\.js|bin\/jazz-tools\.js))\s+)validate\b/g,
    "$1generate",
  );
}

/** Step 7: permission expressions repeated across policies. */
function defsNote(project: Project): string[] {
  let permissions;
  try {
    permissions = JSON.parse(readFileSync(project.permissionsJsonFile, "utf8")) as Json;
  } catch {
    return [];
  }
  const counts = new Map<string, number>();
  const walk = (value: Json | undefined, key?: string): void => {
    if (typeof value === "string") {
      if (key === "$schema" || value === "true" || value === "false" || value.length < 24) return;
      counts.set(value, (counts.get(value) ?? 0) + 1);
    } else if (Array.isArray(value)) value.forEach((v) => walk(v));
    else if (isObject(value)) {
      for (const [k, v] of Object.entries(value)) if (k !== "defs" && k !== "$comment") walk(v, k);
    }
  };
  walk(permissions);
  const repeated = [...counts].filter(([, n]) => n > 1).sort((a, b) => b[1] - a[1]);
  if (repeated.length === 0) return [];
  const [top, n] = repeated[0]!;
  return [
    `jazz.permissions.json repeats ${repeated.length} expression${repeated.length === 1 ? "" : "s"} (e.g. \`${top}\`, ${n}×): ` +
      "consider factoring them into `defs`, which policies reference by name.",
  ];
}

/** The report as the CLI prints it. */
export function formatReport(report: UpgradeReport, dryRun = false): string {
  const lines: string[] = [];
  const section = (title: string, items: string[]) => {
    if (items.length === 0) return;
    lines.push(`${title}:`);
    for (const item of items) lines.push(`  ${item.replace(/\n/g, "\n  ")}`);
  };
  section(dryRun ? "Would write" : "Wrote", report.written);
  section(dryRun ? "Would delete" : "Deleted", report.deleted);
  section("Rewrote", report.rewrites);
  section("Left for you (upgrade can't convert these)", report.manual);
  section("Notes", report.notes);
  if (report.typechecked) {
    section(
      `Typecheck: ${report.preexistingTypeErrors.length} error${report.preexistingTypeErrors.length === 1 ? "" : "s"} the project had before the conversion too (not upgrade's, and not counted)`,
      report.preexistingTypeErrors,
    );
    if (report.typeErrors.length === 0) {
      lines.push(
        report.preexistingTypeErrors.length === 0
          ? "Typecheck passed."
          : "Typecheck passed: no errors besides the ones the project already had.",
      );
    } else
      section(
        `Typecheck: ${report.typeErrors.length} error${report.typeErrors.length === 1 ? "" : "s"} (the conversion is left in place for you to fix)`,
        report.typeErrors,
      );
  }
  return lines.join("\n");
}

export { notSupportedYet };

/** How many times each error (file, code, first message line) occurs. */
export function countTypeErrors(errors: readonly { key: string }[]): Map<string, number> {
  const counts = new Map<string, number>();
  for (const { key } of errors) counts.set(key, (counts.get(key) ?? 0) + 1);
  return counts;
}

/**
 * The closing typecheck's errors, split into the ones the conversion added
 * and the ones the project had before (`before`, counted per key). Each error
 * before excuses one identical error after, whatever line it moved to, so a
 * second identical error the conversion adds to the same file still counts.
 */
export function splitTypeErrors(
  before: ReadonlyMap<string, number> | undefined,
  after: readonly { key: string; text: string }[],
): { added: string[]; preexisting: string[] } {
  const excused = new Map(before);
  const added: string[] = [];
  const preexisting: string[] = [];
  for (const error of after) {
    const left = excused.get(error.key) ?? 0;
    if (left > 0) {
      excused.set(error.key, left - 1);
      preexisting.push(error.text);
    } else {
      added.push(error.text);
    }
  }
  return { added, preexisting };
}
