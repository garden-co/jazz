/**
 * The app's readable files, `jazz.app.json` and `jazz.permissions.json`
 * (SPEC 10.10). In stage 1 they are an IR the TS DSL compiles to: `validate`
 * and `migrations create` write them beside schema.ts through Jazz's Rust
 * writer (jazz-napi `writeAppArtifact`), `validate --check` refuses them when
 * stale, and `createJazz` refuses to start when committed ones no longer
 * match the app's TS. TS apps keep running from their TS; other languages
 * read the files. Node only.
 */
import { existsSync } from "node:fs";
import { readFile, writeFile } from "node:fs/promises";
import { dirname, join, relative } from "node:path";
import { runtimeSchemaJsonReplacer } from "../drivers/schema-wire.js";
import type { WasmSchema } from "../drivers/types.js";
import {
  mergePermissionsIntoWasmSchema,
  normalizePermissionsForWasm,
  type CompiledPermissionsMap,
} from "../schema-permissions.js";
import {
  recordedTsOnlyColumns,
  sliceTables,
  tsOnlyColumns,
  type SchemaDefinition,
} from "../typed-app.js";

export const APP_FILE = "jazz.app.json";
export const PERMISSIONS_FILE = "jazz.permissions.json";

/** Where the docs explain the files and their expression grammar. */
export const APP_FILES_DOCS = "https://jazz.tools/docs/reference/app-files";

/** An app's two files, as Jazz's Rust writer renders them. */
export interface AppFiles {
  /** `jazz.app.json`, which the app and its clients ship. */
  app: string;
  /** `jazz.permissions.json`, which only its backends read. */
  permissions: string;
}

/** What the writer takes: the app's TS compiled to the engine's forms. */
export interface AppFilesInput {
  /** The current schema's hash. */
  current: string;
  /** Every version the app ships, oldest first, the current one last. */
  schemas: WasmSchema[];
  migrations: Array<{ fromHash: string; toHash: string; forward: unknown[] }>;
  permissions: CompiledPermissionsMap;
  /** The current schema, for its relations and TS-only columns. */
  wasmSchema: WasmSchema;
  /** schema.ts's exports, for the slices it exports. */
  module?: Record<string, unknown>;
}

/** The writer's result: both files, or why the format can't express the app yet. */
export type BuiltAppFiles = { files: AppFiles } | { unsupported: string };

/**
 * A writer refusal that only says the format can't express something yet
 * (a catalogue-native enum, a relation the grammar can't spell). The app
 * itself is fine: a .62 server takes it, so tooling warns and goes on.
 */
export function isUnsupportedYet(message: string): boolean {
  return /not supported in jazz\.(?:app|permissions)\.json yet/.test(message);
}

/** Build both files with Jazz's Rust writer; throws for anything a deploy would refuse too. */
export async function buildAppFiles(input: AppFilesInput): Promise<BuiltAppFiles> {
  const engineInput = {
    current: input.current,
    // As a deployment sends them (`{ tables }`).
    schemas: input.schemas.map((schema) => ({
      tables: mergePermissionsIntoWasmSchema(schema, {}),
    })),
    migrations: input.migrations,
    permissions: normalizePermissionsForWasm(input.permissions),
    codegen: appCodegen(input.wasmSchema, input.module),
  };
  const { writeAppArtifact } = await import("jazz-napi");
  try {
    return { files: writeAppArtifact(JSON.stringify(engineInput, runtimeSchemaJsonReplacer)) };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (isUnsupportedYet(message)) return { unsupported: message };
    throw error;
  }
}

/**
 * What typed code in other languages needs beyond the schema: the DSL's
 * relations, the columns only TypeScript computes, and the slices schema.ts
 * exports, by export name.
 */
function appCodegen(wasmSchema: WasmSchema, module: Record<string, unknown> | undefined): unknown {
  const tsOnly = recordedTsOnlyColumns(wasmSchema) ?? definitionTsOnlyColumns(module) ?? {};
  const tables: Record<string, { relations?: unknown; ts_only?: unknown }> = {};
  for (const [name, table] of Object.entries(wasmSchema)) {
    const relations = Object.fromEntries(
      Object.entries(table.relations ?? {}).map(([relation, target]) => [
        relation,
        target.kind === "forward"
          ? { type: "Forward", table: target.table, column: target.column }
          : { type: "Reverse", table: target.table, relation: target.relation },
      ]),
    );
    const columns = tsOnly[name] ?? [];
    if (Object.keys(relations).length === 0 && columns.length === 0) continue;
    tables[name] = { relations, ts_only: columns };
  }
  const slices: Record<string, string[]> = {};
  for (const [name, value] of Object.entries(module ?? {})) {
    const sliced = sliceTables(value, wasmSchema);
    if (sliced) slices[name] = sliced;
  }
  return { tables, slices };
}

/** The TS-only columns of a schema.ts that exports its definition rather than an app. */
function definitionTsOnlyColumns(
  module: Record<string, unknown> | undefined,
): ReturnType<typeof tsOnlyColumns> | undefined {
  const definition = module?.schema ?? module?.schemaDef;
  if (typeof definition !== "object" || definition === null || "wasmSchema" in definition) {
    return undefined;
  }
  try {
    return tsOnlyColumns(definition as SchemaDefinition);
  } catch {
    // Not a definition: the schema came from elsewhere, with nothing TS-only.
    return undefined;
  }
}

/** Each of `files` with the path it is written to, in `dir` (beside schema.ts). */
export function appFilePaths(dir: string, files: AppFiles): Array<[path: string, text: string]> {
  return [
    [join(dir, APP_FILE), files.app],
    [join(dir, PERMISSIONS_FILE), files.permissions],
  ];
}

/** Write both files beside schema.ts, leaving each alone when unchanged so a watcher sees no edit. */
export async function writeAppFiles(schemaFile: string, files: AppFiles): Promise<void> {
  for (const [path, text] of appFilePaths(dirname(schemaFile), files)) {
    if ((await readFile(path, "utf8").catch(() => null)) === text) continue;
    await writeFile(path, text);
  }
}

/** `validate --check`: the committed files must be what validate would write. */
export async function checkAppFiles(
  schemaDir: string,
  schemaFile: string,
  files: AppFiles,
): Promise<void> {
  for (const [path, text] of appFilePaths(dirname(schemaFile), files)) {
    const committed = await readFile(path, "utf8").catch(() => null);
    if (committed === text) continue;
    const shown = relative(schemaDir, path) || path;
    throw new Error(
      committed === null
        ? `${shown} is missing. Run \`jazz-tools validate\` to write it, and commit it.`
        : `${shown} is not what schema.ts, permissions.ts and the migrations make: run \`jazz-tools validate\` and commit the jazz.app.json and jazz.permissions.json it writes.`,
    );
  }
}

/** The warning for an app whose files the format can't express yet. */
export function unsupportedWarning(message: string): string {
  return (
    `jazz.app.json and jazz.permissions.json were not written: ${message} ` +
    `The app runs from its TS as before; other languages can't load it until the format supports this. See ${APP_FILES_DOCS}.`
  );
}

/** Where a project keeps schema.ts, as the CLI looks for it: the root, ./src, ./src/lib. */
const SCHEMA_ROOTS = [".", "src", join("src", "lib")];

/** The committed files of the project in `cwd`, if it has both. */
export async function committedAppFiles(cwd: string): Promise<(AppFiles & { dir: string }) | null> {
  for (const root of SCHEMA_ROOTS) {
    const dir = join(cwd, root);
    const app = join(dir, APP_FILE);
    const permissions = join(dir, PERMISSIONS_FILE);
    if (!existsSync(app) && !existsSync(permissions)) continue;
    if (!existsSync(app) || !existsSync(permissions)) return null;
    return {
      dir,
      app: await readFile(app, "utf8"),
      permissions: await readFile(permissions, "utf8"),
    };
  }
  return null;
}

/**
 * `createJazz`'s check of the running app against the project's committed
 * files: refused when they describe another schema (schema.ts changed) or
 * other permissions (permissions.ts changed), naming which, as `validate
 * --check` would. Files absent, or an app the format can't express yet,
 * start as before; so does an app on `{ migrations: "none" }`, which the
 * caller skips. History is left to `validate --check`: the running app
 * knows its versions by hash only, not by the snapshot names the file uses.
 */
export async function checkRunningApp(
  input: AppFilesInput,
  cwd: string = process.cwd(),
  warn: (message: string) => void = console.warn,
): Promise<void> {
  const committed = await committedAppFiles(cwd);
  const built = await buildAppFiles(input);
  if ("unsupported" in built) {
    warn(`jazz: ${unsupportedWarning(built.unsupported)}`);
    return;
  }
  if (!committed) return;
  const { loadAppArtifact } = await import("jazz-napi");
  const read = (files: AppFiles) =>
    loadAppArtifact(files.app, files.permissions) as { current: string; bundle: string };
  let theirs;
  try {
    theirs = read(committed);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (isUnsupportedYet(message)) {
      warn(`jazz: ${unsupportedWarning(message)}`);
      return;
    }
    throw new Error(
      `${message}. Run \`npx jazz-tools validate\` to rewrite the files in ${relative(cwd, committed.dir) || "."}.`,
      { cause: error },
    );
  }
  const ours = read(built.files);
  const shown = (file: string) => relative(cwd, join(committed.dir, file)) || file;
  if (theirs.current !== ours.current) {
    throw new Error(
      `${shown(APP_FILE)} is out of date with schema.ts (or its migrations): run \`npx jazz-tools validate\` and commit the files it writes.`,
    );
  }
  if (theirs.bundle !== ours.bundle) {
    throw new Error(
      `${shown(PERMISSIONS_FILE)} is out of date with permissions.ts: run \`npx jazz-tools validate\` and commit the files it writes.`,
    );
  }
}
