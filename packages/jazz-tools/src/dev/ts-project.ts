/**
 * An app written in TS (alpha.66): `schema.ts` exports `schema`
 * (`s.defineSchema`) and `permissions` (`definePermissions`), or a sibling
 * `permissions.ts` exports `permissions`; `migrations/*.ts` and
 * `migrations/snapshots/*.json` hold its history. `jazz-tools generate`
 * turns them into `jazz.app.json` and `jazz.permissions.json` (generated
 * artifacts, written by the Rust writer) and then the bindings. Node only.
 */
import { PERMISSIONS_SCHEMA_URL } from "../permissions/types.js";
/** `jazz.app.json`'s `$schema`, which names its format version. */
export const APP_SCHEMA_URL = "https://jazz.tools/schemas/jazz.app.v1.json" as const;
import { existsSync } from "node:fs";
import { readFile } from "node:fs/promises";
import { join, relative } from "node:path";
import { loadModule as loadTsModule } from "../schema-loader.js";
import { readableSchema } from "../schema-dsl/define-schema.js";
import { readablePermissions } from "../permissions/dsl.js";
import { APP_FILE, APP_FILES_DOCS, PERMISSIONS_FILE } from "./app-files.js";

export const SCHEMA_FILE = "schema.ts";
export const PERMISSIONS_TS_FILE = "permissions.ts";
export const MIGRATIONS_DIR = "migrations";

/** Where a project keeps `schema.ts`: the root, ./src, ./src/lib. */
export const SCHEMA_ROOTS = [".", "src", "src/lib"];

export type LoadModule = (file: string) => Promise<Record<string, unknown>>;

/**
 * The refusal of an app with `jazz.app.json` but no `schema.ts`: TS is the
 * only authoring source, so `generate` and `migrations create` write only
 * from schema.ts. The `jazz-tools` binary says the same
 * (crates/jazz-cli/src/bin/jazz-tools.rs).
 */
export const JSON_ONLY_APP =
  "TS is the source in alpha.66: this app has jazz.app.json but no schema.ts. " +
  "Write schema.ts from it with `npx jazz-tools@2.0.0-alpha.70 upgrade` (it keeps the schema hashes and bundle id; " +
  "see https://jazz.tools/docs/operating/backups-and-upgrades#upgrading-from-alpha70-to-beta0), " +
  "then edit schema.ts beside your app's manifest (package.json, Cargo.toml, go.mod, Package.swift, build.gradle, …) " +
  "and run `npx jazz-tools generate`.";

/** The directory holding `schema.ts` in the project at `cwd`; refuses none or several. */
export function findSchemaDir(cwd: string): string {
  const found = SCHEMA_ROOTS.map((root) => join(cwd, root)).filter((dir) =>
    existsSync(join(dir, SCHEMA_FILE)),
  );
  if (found.length > 1) {
    throw new Error(
      `found ${SCHEMA_FILE} in both ${found.map((dir) => relative(cwd, join(dir, SCHEMA_FILE)) || SCHEMA_FILE).join(" and ")}: ` +
        "keep one, or point the command at the app with --schema-dir.",
    );
  }
  const dir = found[0];
  if (dir) return dir;
  const json = SCHEMA_ROOTS.map((root) => join(cwd, root, APP_FILE)).find(existsSync);
  throw new Error(
    json
      ? JSON_ONLY_APP
      : `No ${SCHEMA_FILE} in ${cwd} (looked in ${SCHEMA_ROOTS.join(", ")}). See ${APP_FILES_DOCS}.`,
  );
}

/** A readable `jazz.app.json` input, before the Rust writer checks and prints it. */
export interface ReadableAppFile {
  $schema: typeof APP_SCHEMA_URL;
  tables: Record<string, unknown>;
  history?: Record<string, unknown>;
  migrations?: unknown[];
  transforms?: Record<string, unknown>;
  encryption?: Record<string, unknown>;
}

/** What `loadTsApp` read: the readable inputs of both files and where they came from. */
export interface TsApp {
  dir: string;
  schemaFile: string;
  /** The file `permissions` came from (schema.ts or permissions.ts), or null when there are none. */
  permissionsFile: string | null;
  app: ReadableAppFile;
  permissions: { $schema: typeof PERMISSIONS_SCHEMA_URL; tables: Record<string, unknown> };
  warnings: string[];
}

/** The migrations tooling (`dev/migrations.ts`), loaded only when a command needs it. */
export async function migrationsTooling(): Promise<typeof import("./migrations.js")> {
  return import("./migrations.js");
}

function shown(file: string): string {
  return relative(process.cwd(), file) || file;
}

/**
 * Load the app's TS in `cwd` (or ./src, ./src/lib): `schema` from
 * schema.ts, `permissions` from schema.ts or permissions.ts, and the
 * history from migrations/.
 */
export async function loadTsApp(
  cwd: string,
  options: { loadModule?: LoadModule } = {},
): Promise<TsApp> {
  const loadModule = options.loadModule ?? loadTsModule;
  const dir = findSchemaDir(cwd);
  const schemaFile = join(dir, SCHEMA_FILE);
  const warnings: string[] = [];
  const schemaModule = await loadModule(schemaFile);
  if ("default" in schemaModule && !("schema" in schemaModule))
    throw new Error(
      `${shown(schemaFile)} has a default export: export the schema by name, \`export const schema = s.defineSchema({ … })\`.`,
    );
  const readable = readableSchema(schemaModule.schema);
  if (!readable)
    throw new Error(
      `${shown(schemaFile)} must export \`schema\`: \`export const schema = s.defineSchema({ … })\`.`,
    );

  const permissionsTs = join(dir, PERMISSIONS_TS_FILE);
  let permissionsFile: string | null = null;
  let permissionsValue: unknown;
  if ("permissions" in schemaModule) {
    permissionsFile = schemaFile;
    permissionsValue = schemaModule.permissions;
  }
  if (existsSync(permissionsTs)) {
    const permissionsModule = await loadModule(permissionsTs);
    if (!("permissions" in permissionsModule))
      throw new Error(
        `${shown(permissionsTs)} doesn't export \`permissions\`${"default" in permissionsModule ? " (it has an alpha.64-style default export)" : ""}: ` +
          "write `export const permissions = schema.definePermissions({ … })` with the new permissions DSL, " +
          "or convert an alpha.65 app with `npx jazz-tools@2.0.0-alpha.70 upgrade` (https://jazz.tools/docs/operating/backups-and-upgrades#upgrading-from-alpha70-to-beta0).",
      );
    {
      if (permissionsFile)
        throw new Error(
          `permissions are defined twice: in ${shown(schemaFile)} and in ${shown(permissionsTs)}. Keep one.`,
        );
      permissionsFile = permissionsTs;
      permissionsValue = permissionsModule.permissions;
    }
  }
  let permissions: TsApp["permissions"];
  if (permissionsFile) {
    const tree = readablePermissions(permissionsValue);
    if (!tree)
      throw new Error(
        `${shown(permissionsFile)}'s \`permissions\` must be what \`definePermissions({ … })\` returns (const { definePermissions } = schema).`,
      );
    permissions = { $schema: PERMISSIONS_SCHEMA_URL, tables: tree.tables };
  } else {
    warnings.push(
      `No permissions: ${shown(schemaFile)} exports no \`permissions\` and there is no ${PERMISSIONS_TS_FILE}, so every table denies everything. ` +
        "Export `permissions = definePermissions({ … })` from schema.ts.",
    );
    permissions = { $schema: PERMISSIONS_SCHEMA_URL, tables: {} };
  }

  const app: ReadableAppFile = { $schema: APP_SCHEMA_URL, tables: readable.tables };
  if (readable.encryption !== undefined) app.encryption = readable.encryption;
  const migrationsDir = join(dir, MIGRATIONS_DIR);
  if (existsSync(migrationsDir)) {
    const tooling = await migrationsTooling();
    const { tables, history, migrations } = await tooling.loadMigrationHistory({
      migrationsDir,
      currentTables: readable.tables as Parameters<
        Awaited<ReturnType<typeof migrationsTooling>>["loadMigrationHistory"]
      >[0]["currentTables"],
      loadModule,
    });
    app.tables = tables;
    if (Object.keys(history).length > 0) app.history = history;
    if (migrations.length > 0) app.migrations = migrations;
  }
  if (readable.transforms && Object.keys(readable.transforms).length > 0)
    app.transforms = readable.transforms;
  return { dir, schemaFile, permissionsFile, app, permissions, warnings };
}

/**
 * The line of `table`'s rule `rule` in the permissions source, for errors
 * that name a JSON path (`tables.chats.update.new…`): found as the table key
 * and then the rule key after it.
 */
export function locateRule(source: string, table: string, rule: string): number | undefined {
  const tableMatch = new RegExp(`(^|[\\s{,])["']?${escape(table)}["']?\\s*:`, "m").exec(source);
  if (!tableMatch) return undefined;
  const from = tableMatch.index + tableMatch[0].length;
  const keys =
    rule === "update.old"
      ? ["updateIf", "update"]
      : rule === "update.new"
        ? ["updateTo", "update"]
        : [rule];
  let index = from;
  for (const key of keys) {
    const match = new RegExp(`(^|[\\s{,])${escape(key)}\\s*:`, "m").exec(source.slice(from));
    if (match) {
      index = from + match.index + match[0].length;
      return lineOf(source, index);
    }
  }
  return lineOf(source, from);
}

function escape(text: string): string {
  return text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function lineOf(source: string, index: number): number {
  return source.slice(0, index).split("\n").length;
}

/**
 * The table and rule a writer refusal names: as a readable path
 * (`tables.chats.update.new…`) or an engine one
 * (`$.chats.policies.update.with_check…`).
 */
export function ruleOfError(message: string): { table: string; rule: string } | undefined {
  const readable =
    /jazz\.permissions\.json:\s*tables\.([A-Za-z_][A-Za-z0-9_]*)\.(read|insert|delete|update)(\.(old|new))?/.exec(
      message,
    );
  if (readable)
    return { table: readable[1]!, rule: readable[3] ? `update${readable[3]}` : readable[2]! };
  const engine =
    /\$\.([A-Za-z_][A-Za-z0-9_]*)\.policies\.(select|insert|update|delete)(\.(using|with_check))?/.exec(
      message,
    );
  if (!engine) return undefined;
  const operation = engine[2] === "select" ? "read" : engine[2]!;
  if (operation === "update" && engine[4])
    return { table: engine[1]!, rule: engine[4] === "using" ? "update.old" : "update.new" };
  return { table: engine[1]!, rule: operation };
}

/** A writer refusal, with the TS line of the rule it names when there is one. */
export async function withTsLocation(error: unknown, app: TsApp): Promise<Error> {
  const err = error instanceof Error ? error : new Error(String(error));
  const rule = ruleOfError(err.message);
  if (!rule || !app.permissionsFile) return err;
  const source = await readFile(app.permissionsFile, "utf8").catch(() => null);
  if (source === null) return err;
  const line = locateRule(source, rule.table, rule.rule);
  if (line === undefined) return err;
  const path = `tables.${rule.table}.${rule.rule}`;
  const located = new Error(
    `${err.message}\n  at ${shown(app.permissionsFile)}:${line} (${path})`,
    {
      cause: err,
    },
  );
  Object.assign(located, { code: (err as { code?: unknown }).code });
  return located;
}

export { PERMISSIONS_FILE };
