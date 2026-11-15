/**
 * The CLI's work on an app's files: `validate [--check]` checks
 * `jazz.app.json` and `jazz.permissions.json` and (re)generates
 * `jazz.generated.ts`; `migrations create` records a schema change in
 * `jazz.app.json`. Both go through Jazz's Rust loader and generator
 * (jazz-napi), the same code the `jazz` binary runs. Node only.
 */
import { writeFile } from "node:fs/promises";
import { join, relative } from "node:path";
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
  generateTs,
  loadAppFiles,
  migrationsCreate,
  validateBindings,
} from "./codegen-napi.js";
import type { LocalMigrationGraph } from "./migration-graph.js";

export interface ValidateOptions {
  /** The project directory (`jazz.app.json` in it, ./src or ./src/lib). */
  cwd: string;
  /** Write nothing; fail if `jazz.generated.ts` isn't what validate would write. */
  check?: boolean;
}

export interface ValidateResult {
  dir: string;
  /** Whether `jazz.generated.ts` was (re)written. */
  wrote: boolean;
  warnings: string[];
}

function codeOf(error: unknown): unknown {
  return (error as { code?: unknown } | null)?.code;
}

/** Check both files as a backend reads them, and the bindings against the app file. */
async function checkFiles(project: AppProject, check: boolean): Promise<string[]> {
  if (project.permissions === null) {
    throw new Error(
      `${shown(project, PERMISSIONS_FILE)} is missing. Every app has one, even if only backends read it: ` +
        `start from \`{ "format": 1, "tables": {} }\`, which denies everything.`,
    );
  }
  const loaded = (await loadAppFiles(project.app, project.permissions)) as {
    warnings?: string[];
  };
  try {
    await validateBindings(project.app, project.bindings);
  } catch (error) {
    // Stale bindings are what validate rewrites; `--check` refuses them below.
    if (check || codeOf(error) !== BINDINGS_STALE) throw error;
  }
  return loaded.warnings ?? [];
}

function shown(project: { dir: string }, file: string): string {
  return relative(process.cwd(), join(project.dir, file)) || file;
}

/**
 * `jazz-tools validate`: check the files and regenerate `jazz.generated.ts`.
 * Refuses a schema edited without `migrations create`, before it touches the
 * bindings: they hold the only copy of the previous version.
 */
export async function validateApp(options: ValidateOptions): Promise<ValidateResult> {
  const project = await readAppProject(options.cwd);
  const warnings = await checkFiles(project, options.check ?? false);
  const generated = await generateTs(project.app);
  if (project.bindings === generated) return { dir: project.dir, wrote: false, warnings };
  if (options.check) {
    throw new Error(
      `bindings out of date: ${shown(project, GENERATED_FILE)} is not what ${APP_FILE} generates. Run \`npx jazz-tools validate\` and commit it.`,
    );
  }
  await writeFile(join(project.dir, GENERATED_FILE), generated);
  return { dir: project.dir, wrote: true, warnings };
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
