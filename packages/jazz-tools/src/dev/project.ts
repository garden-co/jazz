/**
 * The CLI's work on an app: `generate [--check] [--force]` loads the app's
 * TS (schema.ts, its permissions and migrations/), writes `jazz.app.json`
 * and `jazz.permissions.json` through the Rust writer, and (re)generates the
 * bindings of each language the app is written in; `migrations create`
 * writes a migration stub and snapshot to migrations/. The bindings come
 * from crates/jazz-codegen's `project` through jazz-napi, the same code as
 * the `jazz-tools` binary. Node only.
 */
import { existsSync } from "node:fs";
import { readFile, rm, writeFile } from "node:fs/promises";
import { join, relative } from "node:path";
import { APP_FILE, PERMISSIONS_FILE, readAppProject } from "./app-files.js";
import {
  compileApp,
  generateForDir,
  loadAppFiles,
  verifyGenerated,
  writeAppFiles,
} from "./codegen-napi.js";
import type { DeploymentBody } from "./catalogue-api.js";
import { permissionCoverageWarnings } from "./catalogue.js";
import type { LocalMigrationGraph } from "./migration-graph.js";
import {
  loadTsApp,
  migrationsTooling,
  MIGRATIONS_DIR,
  withTsLocation,
  type LoadModule,
  type TsApp,
} from "./ts-project.js";

export interface GenerateOptions {
  /** The project directory (schema.ts in it, ./src or ./src/lib). */
  cwd: string;
  /** Write nothing; fail if the files or bindings aren't what generate would write. */
  check?: boolean;
  /** Overwrite a jazz.app.json or jazz.permissions.json edited by hand. */
  force?: boolean;
  /** The bindings' languages (`--lang`); by default each the app's files name. */
  lang?: string[];
  /** Tests: load TS modules another way. */
  loadModule?: LoadModule;
}

export interface GenerateResult {
  dir: string;
  /** Whether jazz.app.json or jazz.permissions.json was (re)written. */
  wroteFiles: boolean;
  /** Whether any bindings file was (re)written. */
  wrote: boolean;
  /** The bindings files, relative to `dir`: one per language the app is written in. */
  bindings: string[];
  warnings: string[];
}

/** Who `$generated` names as the files' writer. */
const GENERATED_BY = "jazz-tools generate from schema.ts — edit the TypeScript, not this file";

function shown(dir: string, file: string): string {
  return relative(process.cwd(), join(dir, file)) || file;
}

/** The files the app's TS makes, as the Rust writer prints them. */
export async function writtenFiles(app: TsApp): Promise<{ app: string; permissions: string }> {
  try {
    return await writeAppFiles(
      `${JSON.stringify(app.app, null, 2)}\n`,
      `${JSON.stringify(app.permissions, null, 2)}\n`,
      { generated: { by: GENERATED_BY } },
    );
  } catch (error) {
    throw await withTsLocation(error, app);
  }
}

/** Tables users can't reach at all, from what a backend would publish. */
async function coverageWarnings(app: string, permissions: string): Promise<string[]> {
  const files = (await loadAppFiles(app, permissions)) as { deployment: DeploymentBody };
  return permissionCoverageWarnings(files.deployment);
}

/**
 * `jazz-tools generate`: load the app's TS, write `jazz.app.json` and
 * `jazz.permissions.json` (refusing to overwrite a file edited by hand
 * unless `force`), then regenerate the bindings of every language the app
 * is written in. With `check`, compare instead and write nothing. Refuses a
 * schema edited without `migrations create` before it touches the
 * bindings: they hold the only copy of the previous version.
 */
export async function generateApp(options: GenerateOptions): Promise<GenerateResult> {
  const app = await loadTsApp(options.cwd, { loadModule: options.loadModule });
  const files = await writtenFiles(app);
  const warnings = [...app.warnings, ...(await coverageWarnings(files.app, files.permissions))];
  const targets: [string, string][] = [
    [APP_FILE, files.app],
    [PERMISSIONS_FILE, files.permissions],
  ];
  // Every check runs before anything is written.
  const stale: { file: string; text: string; previous: string | null }[] = [];
  for (const [file, text] of targets) {
    const path = join(app.dir, file);
    const existing = existsSync(path) ? await readFile(path, "utf8") : null;
    if (existing !== null && sameJson(existing, text)) continue;
    stale.push({ file, text, previous: existing });
    if (options.check || existing === null || options.force) continue;
    const marker = await verifyGenerated(existing);
    if (marker === "mismatch")
      throw new Error(
        `${shown(app.dir, file)} was edited by hand since it was generated: it is generated from your TS, so edit schema.ts instead. ` +
          "Run `jazz-tools generate --force` to overwrite it.",
      );
    if (marker === "none")
      throw new Error(
        `${shown(app.dir, file)} was not written by \`jazz-tools generate\` (it has no \`$generated\` marker) and differs from what your TS makes. ` +
          "Run `jazz-tools generate --force` to replace it (`jazz-tools upgrade` does, for an alpha.65 app).",
      );
  }
  if (options.check && stale.length > 0) {
    throw Object.assign(
      new Error(
        `${stale.map(({ file }) => shown(app.dir, file)).join(" and ")} ${stale.length === 1 ? "is" : "are"} out of date with the app's TS: run \`jazz-tools generate\` and commit the result.`,
      ),
      { code: "JAZZ_FILES_STALE" },
    );
  }
  const bindingsOptions = {
    fallback: "ts",
    ...(options.lang?.length ? { lang: options.lang } : {}),
  };
  if (options.check) {
    // Running the npm CLI says the app is a Node one, unless the directory
    // names another language (a Package.swift, go.mod, …).
    const outcome = await generateForDir(app.dir, { ...bindingsOptions, check: true });
    return {
      dir: app.dir,
      wroteFiles: false,
      wrote: false,
      bindings: [...outcome.current, ...outcome.written],
      warnings: [...outcome.warnings, ...warnings],
    };
  }
  // The bindings are checked against the new jazz.app.json (a schema edited
  // without `migrations create` is refused there), so it is written first;
  // if the bindings refuse, both files go back to what they were.
  for (const { file, text } of stale) await writeFile(join(app.dir, file), text);
  let outcome: Awaited<ReturnType<typeof generateForDir>>;
  try {
    outcome = await generateForDir(app.dir, { ...bindingsOptions, check: false });
  } catch (error) {
    for (const { file, previous } of stale) {
      const path = join(app.dir, file);
      if (previous === null) await rm(path, { force: true });
      else await writeFile(path, previous);
    }
    throw error;
  }
  await ensureFirstSnapshot(app, options.loadModule);
  return {
    dir: app.dir,
    wroteFiles: stale.length > 0,
    wrote: outcome.written.length > 0,
    bindings: [...outcome.current, ...outcome.written],
    warnings: [...outcome.warnings, ...warnings],
  };
}

/** Whether two JSON texts hold the same value, keys in the same order (line endings and layout aside). */
function sameJson(left: string, right: string): boolean {
  try {
    return JSON.stringify(JSON.parse(left)) === JSON.stringify(JSON.parse(right));
  } catch {
    return false;
  }
}

/**
 * An app without snapshots gets one of its current schema, as alpha.64's
 * `validate` wrote it: the baseline the first `migrations create` diffs
 * against, so a schema change can always be migrated.
 */
async function ensureFirstSnapshot(app: TsApp, loadModule?: LoadModule): Promise<void> {
  const tooling = await migrationsTooling();
  const migrationsDir = join(app.dir, MIGRATIONS_DIR);
  if ((await tooling.listSnapshots(migrationsDir)).length > 0) return;
  await tooling.createMigration({
    migrationsDir,
    currentTables: app.app.tables as Parameters<typeof tooling.createMigration>[0]["currentTables"],
    loadModule: loadModule ?? (await import("../schema-loader.js")).loadModule,
  });
}

export interface CreateMigrationOptions {
  cwd: string;
  /** The migration's name, recorded in its file name. */
  name: string;
  /** Tests: load TS modules another way. */
  loadModule?: LoadModule;
}

export interface CreateMigrationResult {
  dir: string;
  /** Whether a migration was written (false: the schema is the latest snapshot's). */
  changed: boolean;
  /** The migration stub written, if any. */
  file?: string;
  /** The snapshot written, if any. */
  snapshot?: string;
  /** What the stub leaves to resolve. */
  todos: string[];
}

/**
 * `jazz-tools migrations create --name <name>`: when the schema differs from
 * the latest snapshot in migrations/snapshots, write its snapshot and a
 * migration stub (`m.defineMigration`) to fill in, as alpha.64 did.
 */
export async function createAppMigration(
  options: CreateMigrationOptions,
): Promise<CreateMigrationResult> {
  if (!options.name || !/^[A-Za-z0-9][A-Za-z0-9_-]*$/.test(options.name)) {
    throw new Error(
      "`jazz-tools migrations create` needs `--name <name>`: letters, digits, `-` and `_`, e.g. `--name add-due-date`.",
    );
  }
  const app = await loadTsApp(options.cwd, { loadModule: options.loadModule });
  const tooling = await migrationsTooling();
  const { loadModule } = await import("../schema-loader.js");
  const migrationsDir = join(app.dir, MIGRATIONS_DIR);
  const result = await tooling.createMigration({
    migrationsDir,
    currentTables: app.app.tables as Parameters<typeof tooling.createMigration>[0]["currentTables"],
    name: options.name,
    loadModule: options.loadModule ?? loadModule,
  });
  return {
    dir: app.dir,
    changed: result.changed,
    ...(result.file ? { file: join(migrationsDir, result.file) } : {}),
    ...(result.snapshot ? { snapshot: join(migrationsDir, result.snapshot) } : {}),
    todos: result.todos ?? [],
  };
}

/** The versions and migrations `jazz.app.json` ships, for `migrations graph`. */
export async function localMigrationGraph(cwd: string): Promise<LocalMigrationGraph> {
  const project = await readAppProject(cwd);
  const compiled = (await compileApp(project.app)) as {
    schema: {
      current: string;
      schemas: Array<{ hash: string }>;
      migrations: Array<{ fromHash: string; toHash: string }>;
    };
  };
  return {
    schemas: compiled.schema.schemas.map(({ hash }) => hash),
    migrations: compiled.schema.migrations.map(({ fromHash, toHash }) => ({ fromHash, toHash })),
    currentSchemaHash: compiled.schema.current,
  };
}
