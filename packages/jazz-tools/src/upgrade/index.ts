/**
 * `jazz-tools upgrade`: convert an alpha.65 app (schema.ts with `defineApp`,
 * the callback permissions.ts, migrations/ with its index.ts) to alpha.66,
 * where TS is the source again: `schema.ts` exports `schema`
 * (`s.defineSchema`) and `permissions` (`definePermissions`), and `generate`
 * writes jazz.app.json, jazz.permissions.json and jazz.generated.ts from it.
 *
 * 1. The app's own jazz-tools (alpha.65) must find its JSON files fresh.
 * 2. schema.ts: `defineApp(schema, { migrations })` → `export const schema =
 *    s.defineSchema({…})`, keeping the author's comments; the app and row
 *    types come from jazz.generated.ts, other exports move to app-extras.ts.
 * 3. The permissions are printed from the fresh jazz.permissions.json into
 *    schema.ts (or permissions.ts, when the old one was large); a spread
 *    tables module (Better Auth's) exports `tables` and `permissions`.
 * 4. Imports of `./schema` go to `./jazz.generated`, `app.slice(…)` becomes
 *    `app`, and scripts calling `validate` call `generate`.
 * 5. migrations/index.ts is deleted; the stubs and snapshots stay.
 * 6. `generate` runs.
 * 7. The schema hashes and the bundle id must be what they were: otherwise
 *    every file is put back and upgrade refuses.
 *
 * Every refusal before step 6 comes before the first write.
 */
import {
  existsSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  realpathSync,
  rmSync,
  rmdirSync,
  writeFileSync,
} from "node:fs";
import { createRequire } from "node:module";
import { dirname, extname, join, relative } from "node:path";
import type * as TS from "typescript";
import { printPermissions, type AppJson, type PermissionsJson } from "../printer/index.js";
import { formatTs, layout, loadPrettier } from "../printer/format.js";
import { isObject, parseAppJson, tsOnlyMarkers } from "./app-json.js";
import { UpgradeRefusal, notSupportedYet } from "./errors.js";
import { convertExtras } from "./extras.js";
import { checkLegacyFiles, dslAlpha, findLegacyJazzTools } from "./fresh.js";
import { readGeneratedNames } from "./names.js";
import { createProgram, typeErrors } from "./program.js";
import {
  EXTRAS_FILE,
  GENERATED_FILE,
  locateProject,
  packageVersion,
  runningPackageRoot,
  type Project,
} from "./project.js";
import { environmentReads, refuseDroppedConditions } from "./permissions-check.js";
import { refusePayloadEnums } from "./payload-enums.js";
import { resolveImport, rewriteProject } from "./rewrite.js";
import { codemodSchema, codemodSpreadModule } from "./schema-ts.js";
import { collectConstants, constantImports } from "./constants.js";
import { PackageAliases } from "./aliases.js";
import { specifierLike } from "./edits.js";

export { UpgradeRefusal } from "./errors.js";

/** A permissions.ts longer than this keeps its own file. */
const SEPARATE_PERMISSIONS_LINES = 150;

export interface UpgradeOptions {
  /** The app's directory (default: cwd). */
  dir?: string;
  /** Where schema.ts is, when not in `dir`, `dir/src` or `dir/src/lib`. */
  schemaDir?: string;
  /** Plan and report, write nothing. */
  dryRun?: boolean;
  /** Skip the closing typecheck. */
  skipTypecheck?: boolean;
  /** Skip step 1 (tests whose fixtures have no installed alpha.65). */
  skipFreshCheck?: boolean;
  /** Step 6: write the JSON files and bindings from the new TS (default: this jazz-tools' `generate`). */
  generate?: (schemaDir: string) => void | Promise<void>;
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
  /** The schema hashes and bundle id, which upgrade keeps. */
  identity?: ArtifactIdentity;
}

/** What a deployment of the app is: its bundle id and every version's schema hash. */
export interface ArtifactIdentity {
  bundle: string;
  current: string;
  schemas: string[];
}

export async function upgradeProject(options: UpgradeOptions = {}): Promise<UpgradeReport> {
  const project = locateProject(options.dir ?? process.cwd(), options.schemaDir);
  const { ts } = project;
  const rel = (path: string) => relative(project.root, path) || path;
  const notes: string[] = [];
  if (!existsSync(project.schemaFile)) {
    throw new UpgradeRefusal(
      `${rel(project.schemaFile)} is missing: upgrade converts an alpha.65 schema.ts`,
    );
  }

  if (/\.defineSchema\(/.test(readFileSync(project.schemaFile, "utf8"))) {
    throw new UpgradeRefusal(
      `${rel(project.schemaFile)} already calls \`defineSchema\`: this app is on alpha.66's layout, run \`npx jazz-tools generate\``,
    );
  }

  // 1. The JSON files must be fresh: the permissions are printed from them.
  if (!options.skipFreshCheck) {
    const legacy = findLegacyJazzTools(project);
    notes.push(...checkLegacyFiles(project, legacy));
  }
  const scan = scanImports(project);
  const envReads = environmentReads(project, schemaSources(project, scan));
  if (envReads.length > 0) {
    notes.push(
      "The old schema or permissions read the environment. The printed permissions hold the values `validate` resolved them to, " +
        `so the env-dependent logic needs a look by hand:\n${envReads.map((r) => `  ${r}`).join("\n")}`,
    );
  }
  refuseDroppedConditions(project);
  const appText = readFileSync(project.appFile, "utf8");
  const permissionsText = readFileSync(project.permissionsJsonFile, "utf8");
  const appJson = parseAppJson(appText);
  refusePayloadEnums(appJson);
  if (Object.keys(tsOnlyMarkers(appJson)).length > 0 || /"computed"\s*:/.test(appText)) {
    throw notSupportedYet("computed columns and transforms", rel(project.appFile));
  }
  const app = JSON.parse(appText) as AppJson;
  const permissions = JSON.parse(permissionsText) as PermissionsJson;
  const before = await artifactIdentity(appText, permissionsText);

  // 2. schema.ts.
  const generatedText = await napiGenerate(appText);
  const names = readGeneratedNames(generatedText);
  const schemaProgram = createProgram(ts, [project.schemaFile], project.compilerOptions, undefined);
  let extras;
  try {
    extras = convertExtras({
      ts,
      program: schemaProgram.program,
      schemaFile: project.schemaFile,
      tables: names.tables,
      generatedExports: names.exports,
      movedToComputed: [],
      jsExtensions: scan.jsExtensions,
      importedNames: scan.schemaNames,
      notes,
    });
  } finally {
    schemaProgram.dispose();
  }
  const schemaText = readFileSync(project.schemaFile, "utf8");
  const extrasFile = join(project.schemaDir, EXTRAS_FILE);
  const codemod = codemodSchema({
    ts,
    file: project.schemaFile,
    text: schemaText,
    exports: extras.exports,
    extrasSpecifier: scan.jsExtensions ? "./app-extras.js" : "./app-extras",
    resolve: (specifier) => resolveImport(project, specifier, project.schemaFile),
    root: project.root,
  });

  // 3. The permissions, printed from jazz.permissions.json.
  const prettier = await loadPrettier(project.root);
  const format = async (text: string, file: string) =>
    prettier ? formatTs(text, { file, prettier }) : layout(text);
  const writes: Array<[string, string]> = [];
  const spreadPermissions: string[] = [];
  const spreadImports: string[] = [];
  const skipTables = new Set<string>();
  for (const spread of codemod.spreads) {
    const text = readFileSync(spread.file, "utf8");
    const tables = spreadTables(ts, spread.file, text, spread.exported);
    // Tables whose rules are all `true`/`false` keep living in the module.
    const plain = tables.every((t) => plainRules(permissions.tables[t]));
    const permissionsText = plain
      ? `export const permissions = ${plainPermissions(tables, permissions)};`
      : undefined;
    const edited = codemodSpreadModule({
      ts,
      file: spread.file,
      text,
      exported: spread.exported,
      permissionsText,
    });
    for (const name of edited.dropped) {
      notes.push(
        `Dropped \`${name}\` from ${rel(spread.file)}: it used the module's own app, which alpha.66 doesn't have.`,
      );
    }
    if (plain) {
      const local = `${spread.local.replace(/(Schema|Tables)$/, "")}Permissions`;
      spreadPermissions.push(local);
      spreadImports.push(
        `import { permissions as ${local} } from ${JSON.stringify(spread.specifier)};`,
      );
      for (const t of tables) skipTables.add(t);
    }
    writes.push([spread.file, await formatSpread(edited.text, spread.file, prettier)]);
    codemod.changes.push(`${rel(spread.file)}: exports \`tables\` and \`permissions\``);
  }
  // The old permissions' named values keep their names; what came from the environment gets a TODO.
  const oldSource =
    codemod.inlinePermissions || !project.permissionsFile
      ? project.schemaFile
      : project.permissionsFile;
  const constants = collectConstants(ts, oldSource, (spec, from) =>
    resolveImport(project, spec, from),
  );
  const printed = printPermissions(app, permissions, {
    spreads: spreadPermissions,
    skipTables,
    constants: constants.byValue,
    notes: envReads.map(
      (read) =>
        `TODO: ${read.replace(/: reads /, " read ")}; the value it had when \`validate\` ran is printed below.`,
    ),
  });
  if (printed.unsupported.length > 0) {
    throw new UpgradeRefusal(
      `the TS DSL can't express these permissions of ${rel(project.permissionsJsonFile)} yet:\n` +
        printed.unsupported.map((u) => `  ${u.path}: ${u.text}\n    ${u.reason}`).join("\n"),
    );
  }
  const oldPermissions = project.permissionsFile
    ? readFileSync(project.permissionsFile, "utf8")
    : "";
  const separate =
    project.permissionsFile !== undefined &&
    oldPermissions.split("\n").length > SEPARATE_PERMISSIONS_LINES &&
    !codemod.inlinePermissions;
  const deleted = new Set<string>(project.permissionsSources);
  if (separate) deleted.delete(project.permissionsFile!);
  const permissionsOut = separate ? project.permissionsFile! : project.schemaFile;
  const named = constantImports(
    constants,
    printed.constantsUsed,
    permissionsOut,
    scan.jsExtensions,
  );
  const permissionsBody = await format(
    [...named.declarations, ...(named.declarations.length ? [""] : []), printed.code].join("\n"),
    project.schemaFile,
  );
  let newSchema: string;
  if (separate) {
    newSchema = withImports(ts, project.schemaFile, codemod.text, []);
    const schemaSpecifier = scan.jsExtensions ? "./schema.js" : "./schema";
    const head = [
      `import { schema as s } from "jazz-tools";`,
      `import { schema } from ${JSON.stringify(schemaSpecifier)};`,
      ...spreadImports,
      ...named.imports,
    ].join("\n");
    writes.push([project.permissionsFile!, `${head}\n\n${permissionsBody}`]);
    notes.push(
      `${rel(project.permissionsFile!)} was long: the printed permissions stay in their own file.`,
    );
  } else {
    newSchema = `${withImports(ts, project.schemaFile, codemod.text, [...spreadImports, ...named.imports])}\n${permissionsBody}`;
  }
  writes.unshift([project.schemaFile, newSchema]);
  notes.push(
    "The permissions were printed from jazz.permissions.json: repeated checks became helpers named for what they check, and the old permissions.ts's named values kept their names. Its comments didn't survive: bring back what they said.",
  );

  // 5. migrations/index.ts goes; the stubs and snapshots stay.
  if (project.migrationsDir) {
    for (const name of ["index.ts", "index.js"]) {
      const index = join(project.migrationsDir, name);
      if (existsSync(index)) deleted.add(index);
    }
    // A migration from a version nothing holds (no snapshot, not in jazz.app.json's history)
    // never shipped: alpha.65's validate left it out, and `generate` can't build it.
    const snapshotsDir = join(project.migrationsDir, "snapshots");
    const known = new Set([
      ...Object.keys(app.history ?? {}).map((v) => v.slice(0, 12)),
      before.current.slice(0, 12),
      ...(existsSync(snapshotsDir)
        ? readdirSync(snapshotsDir).flatMap((f) => /([0-9a-f]{12})\.json$/i.exec(f)?.[1] ?? [])
        : []),
    ]);
    for (const name of readdirSync(project.migrationsDir)) {
      const from = /-([0-9a-f]{12})-[0-9a-f]{12}\.[cm]?[jt]s$/i.exec(name)?.[1];
      if (!from || known.has(from)) continue;
      deleted.add(join(project.migrationsDir, name));
      notes.push(
        `Deleted migrations/${name}: it migrates from ${from}, a version no snapshot and no jazz.app.json history holds, so it never shipped.`,
      );
    }
  }

  // 4. Imports of schema.ts and permissions.ts.
  const generatedFile = join(project.schemaDir, GENERATED_FILE);
  if (extras.extrasFile) {
    if (existsSync(extrasFile)) {
      throw new UpgradeRefusal(
        `${rel(extrasFile)} exists already; upgrade writes it: move it away and run upgrade again`,
      );
    }
    writes.push([extrasFile, extras.extrasFile]);
  }
  const aliases = new PackageAliases(project.root);
  const untouched = new Set([
    project.schemaFile,
    ...codemod.spreads.map((s) => s.file),
    ...deleted,
    ...(separate ? [project.permissionsFile!] : []),
  ]);
  const rewrites = rewriteProject({
    aliases,
    project,
    exports: extras.exports,
    tables: names.tables,
    deleted: untouched,
    generatedFile,
    extrasFile,
    computedFile: join(project.schemaDir, "jazz.computed.ts"),
  });
  const config = configEdits(
    project,
    rewrites.files.some((f) => f.importsJson),
    deleted,
  );
  const manifest = manifestEdit(project);
  const aliasEdits = aliases.apply(
    manifest ? new Map([[manifest.file, manifest.text]]) : undefined,
  );
  for (const file of rewrites.files) writes.push([file.file, file.text]);
  if (config.file) writes.push([config.file, config.text]);
  if (manifest && !aliasEdits.has(manifest.file)) writes.push([manifest.file, manifest.text]);
  for (const [file, edit] of aliasEdits) writes.push([file, edit.text]);

  const report: UpgradeReport = {
    dir: project.root,
    written: writes.map(([path]) => rel(path)),
    deleted: [...deleted].map(rel).sort(),
    rewrites: [
      ...codemod.changes,
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
    identity: before,
  };
  if (options.dryRun) return report;

  // The project's errors before the conversion, so the closing typecheck tells upgrade's apart.
  let beforeErrors: Map<string, number> | undefined;
  if (!options.skipTypecheck)
    beforeErrors = countTypeErrors(projectTypeErrors(project, writes, new Set()));

  // Write, keeping what was there to put back.
  const originals = new Map<string, string | null>();
  for (const path of [
    ...writes.map(([p]) => p),
    ...deleted,
    project.appFile,
    project.permissionsJsonFile,
    generatedFile,
  ]) {
    if (!originals.has(path))
      originals.set(path, existsSync(path) ? readFileSync(path, "utf8") : null);
  }
  // `generate` writes files too (a first snapshot, bindings): what wasn't there goes again.
  const filesBefore = new Set(appFiles(project.schemaDir));
  const restore = () => {
    for (const [path, text] of originals) {
      if (text === null) rmSync(path, { force: true });
      else {
        mkdirSync(dirname(path), { recursive: true });
        writeFileSync(path, text);
      }
    }
    for (const path of appFiles(project.schemaDir))
      if (!filesBefore.has(path)) rmSync(path, { force: true });
  };
  for (const [path, text] of writes) writeFileSync(path, text);
  for (const path of deleted) rmSync(path, { force: true });
  // Versions of jazz.app.json's history without a snapshot get one: `generate` builds the history from them.
  for (const [path, text] of await missingSnapshots(project, app)) {
    mkdirSync(dirname(path), { recursive: true });
    writeFileSync(path, text);
    report.written.push(rel(path));
  }
  removeEmptyDirs([...deleted].map(dirname), project.root);

  // 6. generate, and 7. the same deployment.
  try {
    await (options.generate ?? runGenerate)(project.schemaDir);
    const after = await artifactIdentity(
      readFileSync(project.appFile, "utf8"),
      readFileSync(project.permissionsJsonFile, "utf8"),
    );
    const differences = identityDifferences(before, after);
    if (differences.length > 0) {
      throw new UpgradeRefusal(
        `the converted app would deploy differently, so upgrade put every file back:\n${differences.map((d) => `  ${d}`).join("\n")}\n` +
          "Please report this with the app's jazz.app.json and jazz.permissions.json.",
      );
    }
  } catch (error) {
    restore();
    if (error instanceof UpgradeRefusal) throw error;
    throw new UpgradeRefusal(
      `\`generate\` failed on the converted app, so upgrade put every file back:\n  ${(error instanceof Error ? error.message : String(error)).replace(/\n/g, "\n  ")}`,
    );
  }
  report.notes.push(
    `The schema hashes and the bundle id (${before.bundle.slice(0, 12)}) are unchanged.`,
  );
  // A Rust app (a crate on `jazz-app-support`) gets Rust bindings instead, as `generate` writes them.
  const rustApp = isRustApp(project.schemaDir);
  report.written.push(rel(project.appFile), rel(project.permissionsJsonFile));
  if (rustApp) {
    const rustFile = join(project.schemaDir, RUST_BINDINGS_FILE);
    const rust = await rustBindings(readFileSync(project.appFile, "utf8"));
    if (typeof rust === "string") {
      mkdirSync(dirname(rustFile), { recursive: true });
      writeFileSync(rustFile, rust);
      report.written.push(rel(rustFile));
    } else {
      report.notes.push(
        `${RUST_BINDINGS_FILE} was not written (${rust.reason}); run \`npx jazz-tools generate\` once this jazz-tools generates Rust bindings.`,
      );
    }
    report.notes.push(
      "This is a Rust app: run `cargo check` to typecheck it against the new bindings.",
    );
  } else {
    report.written.push(rel(generatedFile));
  }

  if (!options.skipTypecheck && !rustApp) {
    const split = splitTypeErrors(beforeErrors, projectTypeErrors(project, writes, deleted));
    report.typeErrors = split.added;
    report.preexistingTypeErrors = split.preexisting;
    report.typechecked = true;
    const views = project.sources.filter((f) => /\.(svelte|vue)$/.test(f));
    if (views.length > 0) {
      report.notes.push(
        `The typecheck covers .ts/.tsx; run svelte-check or vue-tsc for the ${views.length} .svelte/.vue file${views.length === 1 ? "" : "s"}.`,
      );
    }
  }
  return report;
}

function projectTypeErrors(
  project: Project,
  writes: Array<[string, string]>,
  deleted: Set<string>,
) {
  const { ts } = project;
  const roots = new Set(project.configFileNames.filter((f) => !deleted.has(f)));
  for (const [path] of writes)
    if (/\.(ts|tsx|mts|cts)$/.test(path) && existsSync(path)) roots.add(path);
  if (roots.size === 0) return [];
  const runtime = runningPackageRoot();
  const installed = installedJazzTools(project);
  const override =
    installed &&
    dslAlpha(packageVersion(installed)) !== null &&
    realpathSync(installed) !== realpathSync(runtime)
      ? runtime
      : undefined;
  const handle = createProgram(ts, [...roots], project.compilerOptions, override);
  try {
    return typeErrors(ts, handle.program, project.root);
  } finally {
    handle.dispose();
  }
}

/** Every file under `dir`, but dependencies and VCS data. */
function appFiles(dir: string): string[] {
  const out: string[] = [];
  const walk = (current: string) => {
    for (const entry of readdirSync(current, { withFileTypes: true })) {
      if (entry.name === "node_modules" || entry.name === ".git") continue;
      const path = join(current, entry.name);
      if (entry.isDirectory()) walk(path);
      else out.push(path);
    }
  };
  walk(dir);
  return out;
}

/**
 * Snapshots for the versions in jazz.app.json's `history` that
 * migrations/snapshots/ lacks (alpha.65 apps kept some only in the JSON), as
 * `migrations create` writes them.
 */
async function missingSnapshots(project: Project, app: AppJson): Promise<Array<[string, string]>> {
  const versions = Object.entries(app.history ?? {});
  if (versions.length === 0) return [];
  const dir = join(project.migrationsDir ?? join(project.schemaDir, "migrations"), "snapshots");
  const have = new Set(
    existsSync(dir)
      ? readdirSync(dir).flatMap((f) => /([0-9a-f]{12})\.json$/i.exec(f)?.[1] ?? [])
      : [],
  );
  const { snapshotFromVersion } = await import("../dev/migrations.js");
  const out: Array<[string, string]> = [];
  for (const [name, version] of versions) {
    if (have.has(name.slice(0, 12))) continue;
    const snapshot = await snapshotFromVersion(version.tables as never);
    const hash = snapshot.hash.slice(0, 12);
    if (have.has(hash)) continue;
    out.push([join(dir, `${hash}.json`), `${JSON.stringify(snapshot.schema, null, 2)}\n`]);
    have.add(hash);
  }
  return out;
}

/** The bundle id and schema hashes `load` computes for the two files. */
export async function artifactIdentity(
  app: string,
  permissions: string,
): Promise<ArtifactIdentity> {
  const napi = (await import("jazz-napi")) as unknown as {
    loadAppArtifact(
      app: string,
      permissions: string,
    ): {
      bundle: string;
      current: string;
      deployment: { schemas: { hash: string }[] };
    };
  };
  const loaded = napi.loadAppArtifact(app, permissions);
  return {
    bundle: loaded.bundle,
    current: loaded.current,
    schemas: loaded.deployment.schemas.map((s) => s.hash),
  };
}

export function identityDifferences(before: ArtifactIdentity, after: ArtifactIdentity): string[] {
  const out: string[] = [];
  if (before.current !== after.current)
    out.push(`current schema hash ${before.current} → ${after.current}`);
  const a = [...before.schemas].sort().join(", ");
  const b = [...after.schemas].sort().join(", ");
  if (a !== b) out.push(`schema versions [${a}] → [${b}]`);
  if (before.bundle !== after.bundle) out.push(`bundle id ${before.bundle} → ${after.bundle}`);
  return out;
}

const RUST_BINDINGS_FILE = join("src", "jazz_app.rs");

/**
 * Whether `dir` is a Rust app, as `jazz-tools generate` tells: a crate (a
 * `Cargo.toml` with `[package]`) that depends on `jazz-app-support`, or one with
 * committed `src/jazz_app.rs`. A workspace root or an unrelated crate isn't.
 */
export function isRustApp(dir: string): boolean {
  const manifest = join(dir, "Cargo.toml");
  if (!existsSync(manifest)) return false;
  const text = readFileSync(manifest, "utf8");
  if (!/^\s*\[package\]/m.test(text)) return false;
  if (existsSync(join(dir, RUST_BINDINGS_FILE))) return true;
  return /^\s*jazz-app-support\s*=|^\s*\[(?:dev-|build-)?dependencies\.jazz-app-support\]/m.test(
    text,
  );
}

/** The `jazz-app-support` crate's bindings, or why this jazz-napi can't generate them. */
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

/** This jazz-tools' `generate`, in the app's directory: the JSON files from the TS, then the bindings. */
async function runGenerate(schemaDir: string): Promise<void> {
  const { generateApp } = await import("../dev/project.js");
  await generateApp({ cwd: schemaDir });
}

/** The table names a spread module's `export const schema = {…}` declares. */
function spreadTables(ts: typeof TS, file: string, text: string, exported: string): string[] {
  const source = ts.createSourceFile(file, text, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
  for (const statement of source.statements) {
    if (!ts.isVariableStatement(statement)) continue;
    for (const declaration of statement.declarationList.declarations) {
      if (!ts.isIdentifier(declaration.name) || declaration.name.text !== exported) continue;
      let init = declaration.initializer;
      while (init && (ts.isAsExpression(init) || ts.isSatisfiesExpression(init)))
        init = init.expression;
      if (init && ts.isObjectLiteralExpression(init)) {
        return init.properties.flatMap((p) =>
          p.name && (ts.isIdentifier(p.name) || ts.isStringLiteral(p.name)) ? [p.name.text] : [],
        );
      }
    }
  }
  return [];
}

function plainRules(rules: PermissionsJson["tables"][string] | undefined): boolean {
  return Object.entries(rules ?? {}).every(
    ([key, value]) => key === "$comment" || value === "true" || value === "false",
  );
}

function plainPermissions(tables: string[], permissions: PermissionsJson): string {
  const body = tables.map((table) => {
    const rules = permissions.tables[table] ?? {};
    const entries = (["read", "insert", "update", "delete"] as const)
      .filter((op) => rules[op] !== undefined)
      .map((op) => `${op}: ${rules[op] as string}`);
    return `  ${/^[A-Za-z_$][\w$]*$/.test(table) ? table : JSON.stringify(table)}: { ${entries.join(", ")} },`;
  });
  return `{\n${body.join("\n")}\n}`;
}

async function formatSpread(
  text: string,
  file: string,
  prettier: Awaited<ReturnType<typeof loadPrettier>>,
) {
  return prettier ? formatTs(text, { file, prettier }) : text;
}

/** `text` with `lines` added after its last import. */
function withImports(ts: typeof TS, file: string, text: string, lines: string[]): string {
  if (lines.length === 0) return text;
  const source = ts.createSourceFile(file, text, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
  const last = source.statements.filter(ts.isImportDeclaration).pop();
  const at = last ? last.getEnd() : 0;
  return `${text.slice(0, at)}\n${lines.join("\n")}${text.slice(at)}`;
}

/** package.json: scripts calling `jazz-tools validate` call `generate`. */
function manifestEdit(
  project: Project,
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
    const renamed = renameValidate(script);
    if (renamed === script) continue;
    changes.push(
      `package.json: script "${name}": \`jazz-tools validate\` → \`jazz-tools generate\``,
    );
    next = next.replace(JSON.stringify(script), JSON.stringify(renamed));
  }
  return next === text ? undefined : { file, text: next, changes };
}

export { notSupportedYet, isObject, specifierLike, extname };

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
            if (spec.text.startsWith(".")) {
              if (spec.text.endsWith(".js")) js++;
              else plain++;
            }
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

/** A shell command with every jazz-tools CLI `validate` call renamed to `generate`. */
export function renameValidate(script: string): string {
  return script.replace(
    /((?:^|[\s;&|('"])(?:(?:npx|pnpm(?: exec)?|yarn|bunx)\s+)?(?:jazz-tools(?:@\S+)?|\S*jazz-tools\/(?:dist\/cli\.js|bin\/jazz-tools\.js))\s+)validate\b/g,
    "$1generate",
  );
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
