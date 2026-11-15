/**
 * The CLI's work on an app's files: `generate [--check]` checks
 * `jazz.app.json` and `jazz.permissions.json` and (re)generates
 * `jazz.generated.ts`; `migrations create` records a schema change in
 * `jazz.app.json`. Both go through Jazz's Rust loader and generator
 * (jazz-napi), the same code the `jazz` binary runs. Node only.
 */
import { existsSync, readFileSync } from "node:fs";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, join, relative } from "node:path";
import {
  APP_FILE,
  GENERATED_FILE,
  PERMISSIONS_FILE,
  readAppProject,
  type AppProject,
} from "./app-files.js";
import {
  BINDINGS_STALE,
  compileApp,
  generateRust,
  generateKotlin,
  generateTs,
  loadAppFiles,
  migrationsCreate,
  validateBindings,
} from "./codegen-napi.js";
import type { DeploymentBody } from "./catalogue-api.js";
import { permissionCoverageWarnings } from "./catalogue.js";
import type { LocalMigrationGraph } from "./migration-graph.js";

export interface GenerateOptions {
  /** The project directory (`jazz.app.json` in it, ./src or ./src/lib). */
  cwd: string;
  /** Write nothing; fail if `jazz.generated.ts` isn't what generate would write. */
  check?: boolean;
}

export interface GenerateResult {
  dir: string;
  /** Whether `jazz.generated.ts` (or a Rust crate's `src/jazz_app.rs`) was (re)written. */
  wrote: boolean;
  /** The bindings files, relative to `dir`: jazz.generated.ts, and src/jazz_app.rs for a Rust crate. */
  bindings: string[];
  warnings: string[];
}

/** Where a Rust crate beside jazz.app.json keeps its generated bindings. */
export const RUST_BINDINGS_FILE = join("src", "jazz_app.rs");
export const KOTLIN_BINDINGS_FILE = join("src", "main", "kotlin", "JazzApp.kt");

/**
 * Whether `dir` is a Kotlin app: a `build.gradle.kts` beside its files that
 * depends on the `jazz-kotlin` runtime (as `Lang::Kotlin` detects it).
 */
function isKotlinApp(dir: string): boolean {
  const path = join(dir, "build.gradle.kts");
  if (!existsSync(path)) return false;
  return readFileSync(path, "utf8")
    .split("\n")
    .map((line) => line.split("//")[0])
    .some((line) => line.includes(':jazz-kotlin"') || line.includes(":jazz-kotlin:"));
}

/** The bindings besides TS an app in `dir` gets: Rust for a crate, Kotlin for a Gradle app. */
function nativeBindings(
  dir: string,
): Array<{ file: string; generate: (app: string) => Promise<string> }> {
  const out = [];
  if (existsSync(join(dir, "Cargo.toml")))
    out.push({ file: RUST_BINDINGS_FILE, generate: generateRust });
  if (isKotlinApp(dir)) out.push({ file: KOTLIN_BINDINGS_FILE, generate: generateKotlin });
  return out;
}

function codeOf(error: unknown): unknown {
  return (error as { code?: unknown } | null)?.code;
}

/** Check both files as a backend reads them, and the bindings against the app file. */
async function checkFiles(
  project: AppProject,
  check: boolean,
  tsBindings = true,
): Promise<{ warnings: string[]; stale: boolean }> {
  if (project.permissions === null) {
    throw new Error(
      `${shown(project, PERMISSIONS_FILE)} is missing. Every app has one, even if only backends read it: ` +
        `start from \`{ "format": 1, "tables": {} }\`, which denies everything.`,
    );
  }
  const files = (await loadAppFiles(project.app, project.permissions)) as {
    deployment: DeploymentBody;
    warnings?: string[];
  };
  // What a backend would publish says which tables users can't reach at all.
  const loaded = {
    warnings: [...(files.warnings ?? []), ...permissionCoverageWarnings(files.deployment)],
  };
  // The bindings are current when their `source` and format match, whatever a
  // formatter did to them; stale ones are what generate rewrites.
  let stale = false;
  if (!tsBindings) return { warnings: loaded.warnings ?? [], stale };
  try {
    await validateBindings(project.app, project.bindings);
  } catch (error) {
    if (check || codeOf(error) !== BINDINGS_STALE) throw error;
    stale = true;
  }
  return { warnings: loaded.warnings ?? [], stale };
}

function shown(project: { dir: string }, file: string): string {
  return relative(process.cwd(), join(project.dir, file)) || file;
}

/**
 * `jazz-tools generate`: check the files and regenerate `jazz.generated.ts`.
 * Refuses a schema edited without `migrations create`, before it touches the
 * bindings: they hold the only copy of the previous version.
 */
export async function generateApp(options: GenerateOptions): Promise<GenerateResult> {
  const project = await readAppProject(options.cwd);
  const native = nativeBindings(project.dir);
  // A Rust or Kotlin app without TS bindings gets only its own.
  const ts = native.length === 0 || project.bindings !== null;
  const { warnings, stale } = await checkFiles(project, options.check ?? false, ts);
  const bindings: string[] = [];
  let wrote = false;
  if (ts) {
    bindings.push(GENERATED_FILE);
    if (stale) {
      await writeFile(join(project.dir, GENERATED_FILE), await generateTs(project.app));
      wrote = true;
    }
  }
  // A Rust crate (a `jazz-app` backend) or a Kotlin app beside the files gets its bindings too.
  for (const lang of native) {
    bindings.push(lang.file);
    const path = join(project.dir, lang.file);
    const text = await lang.generate(project.app);
    const committed = await readFile(path, "utf8").catch(() => null);
    if (committed !== text) {
      if (options.check) {
        throw new Error(
          `bindings out of date: ${shown(project, lang.file)} is not what ${APP_FILE} generates. Run \`npx jazz-tools generate\` and commit it.`,
        );
      }
      await mkdir(dirname(path), { recursive: true });
      await writeFile(path, text);
      wrote = true;
    }
  }
  return { dir: project.dir, wrote, bindings, warnings };
}

export interface CreateMigrationOptions {
  cwd: string;
  /** The migration's name, recorded in its entry. */
  name: string;
}

export interface CreateMigrationResult {
  dir: string;
  /** Whether jazz.app.json changed (false: the schema is what the bindings have). */
  changed: boolean;
  /** JSON paths of the `"TODO: …"` entries left to resolve. */
  todos: string[];
}

/**
 * `jazz-tools migrations create --name <name>`: move the bindings' version
 * into `history` and write a migration from it to the edited tables, with
 * `"TODO: …"` strings where the diff can't decide. Once nothing is left to
 * resolve, it regenerates the bindings too.
 */
export async function createAppMigration(
  options: CreateMigrationOptions,
): Promise<CreateMigrationResult> {
  if (!options.name || !/^[A-Za-z0-9][A-Za-z0-9_-]*$/.test(options.name)) {
    throw new Error(
      "`jazz-tools migrations create` needs `--name <name>`: letters, digits, `-` and `_`, e.g. `--name add-due-date`.",
    );
  }
  const project = await readAppProject(options.cwd);
  const created = await migrationsCreate(project.app, project.bindings, options.name);
  if (!created.changed) return { dir: project.dir, changed: false, todos: [] };
  await writeFile(join(project.dir, APP_FILE), created.app);
  if (created.todos.length === 0) {
    await writeFile(join(project.dir, GENERATED_FILE), await generateTs(created.app));
    for (const lang of nativeBindings(project.dir)) {
      await mkdir(dirname(join(project.dir, lang.file)), { recursive: true });
      await writeFile(join(project.dir, lang.file), await lang.generate(created.app));
    }
  }
  return { dir: project.dir, changed: true, todos: created.todos };
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
