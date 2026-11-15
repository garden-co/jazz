/**
 * The CLI's work on an app's files: `generate [--check]` checks
 * `jazz.app.json` and `jazz.permissions.json` and (re)generates
 * `jazz.generated.ts`; `migrations create` records a schema change in
 * `jazz.app.json`. Both go through Jazz's Rust loader and generator
 * (jazz-napi), the same code the `jazz` binary runs. Node only.
 */
import { existsSync } from "node:fs";
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
  const crate = existsSync(join(project.dir, "Cargo.toml"));
  // A Rust-only app (a crate without TS bindings) gets only its Rust ones.
  const ts = !crate || project.bindings !== null;
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
  // A Rust crate beside the files (a `jazz-app` backend) gets its bindings too.
  if (crate) {
    bindings.push(RUST_BINDINGS_FILE);
    const path = join(project.dir, RUST_BINDINGS_FILE);
    const text = await generateRust(project.app);
    const committed = await readFile(path, "utf8").catch(() => null);
    if (committed !== text) {
      if (options.check) {
        throw new Error(
          `bindings out of date: ${shown(project, RUST_BINDINGS_FILE)} is not what ${APP_FILE} generates. Run \`npx jazz-tools generate\` and commit it.`,
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
    if (existsSync(join(project.dir, "Cargo.toml"))) {
      await writeFile(join(project.dir, RUST_BINDINGS_FILE), await generateRust(created.app));
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
