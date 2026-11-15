/**
 * The CLI's work on an app's files: `generate [--check]` checks
 * `jazz.app.json` and `jazz.permissions.json` and (re)generates
 * the bindings of each language the app is written in; `migrations create`
 * records a schema change in `jazz.app.json`. Both run crates/jazz-codegen's
 * `project` through jazz-napi, the same code as the `jazz-tools` binary, so
 * they pick the same languages and write the same bytes. Node only.
 */
import { join, relative } from "node:path";
import { PERMISSIONS_FILE, readAppProject, type AppProject } from "./app-files.js";
import {
  compileApp,
  generateForDir,
  loadAppFiles,
  migrationsCreateForDir,
} from "./codegen-napi.js";
import type { DeploymentBody } from "./catalogue-api.js";
import { permissionCoverageWarnings } from "./catalogue.js";
import type { LocalMigrationGraph } from "./migration-graph.js";

export interface GenerateOptions {
  /** The project directory (`jazz.app.json` in it, ./src or ./src/lib). */
  cwd: string;
  /** Write nothing; fail if the bindings aren't what generate would write. */
  check?: boolean;
}

export interface GenerateResult {
  dir: string;
  /** Whether any bindings file was (re)written. */
  wrote: boolean;
  /** The bindings files, relative to `dir`: one per language the app is written in. */
  bindings: string[];
  warnings: string[];
}

/** Check that the permissions file exists, and warn for tables users can't reach. */
async function permissionWarnings(project: AppProject): Promise<string[]> {
  if (project.permissions === null) {
    throw new Error(
      `${shown(project, PERMISSIONS_FILE)} is missing. Every app has one, even if only backends read it: ` +
        `start from \`{ "format": 1, "tables": {} }\`, which denies everything.`,
    );
  }
  const files = (await loadAppFiles(project.app, project.permissions)) as {
    deployment: DeploymentBody;
  };
  // What a backend would publish says which tables users can't reach at all.
  return permissionCoverageWarnings(files.deployment);
}

function shown(project: { dir: string }, file: string): string {
  return relative(process.cwd(), join(project.dir, file)) || file;
}

/**
 * `jazz-tools generate`: check the files and regenerate the bindings of
 * every language the app is written in. Which languages, the checks and the
 * bytes written are the `jazz-tools` binary's: both run crates/jazz-codegen's
 * `project` (here through jazz-napi). Refuses a schema edited without
 * `migrations create`, before it touches the bindings: they hold the only
 * copy of the previous version.
 */
export async function generateApp(options: GenerateOptions): Promise<GenerateResult> {
  const project = await readAppProject(options.cwd);
  const coverage = await permissionWarnings(project);
  // Running the npm CLI says the app is a Node one, unless the directory
  // names another language (a Package.swift, go.mod, …).
  const outcome = await generateForDir(project.dir, {
    check: options.check ?? false,
    fallback: "ts",
  });
  return {
    dir: project.dir,
    wrote: outcome.written.length > 0,
    bindings: [...outcome.current, ...outcome.written],
    warnings: [...outcome.warnings, ...coverage],
  };
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
 * resolve, it regenerates the bindings too. As the `jazz-tools` binary does:
 * both run crates/jazz-codegen's `project`.
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
  const outcome = await migrationsCreateForDir(project.dir, options.name, { fallback: "ts" });
  return { dir: project.dir, changed: outcome.changed, todos: outcome.todos };
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
