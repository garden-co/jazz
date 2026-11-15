/**
 * Apps defined inline, for tests: a `jazz.app.json` (and its
 * `jazz.permissions.json`) written as an object in the test, compiled by
 * Jazz's Rust loader exactly as `jazz-tools generate` compiles the files
 * into `jazz.generated.ts`. Node only: it loads jazz-napi synchronously, so
 * a test can define its app at the top level.
 *
 * ```ts
 * const app = testApp({
 *   tables: { todos: { columns: { title: "string", done: "boolean" } } },
 * });
 * const permissions = { tables: { todos: { read: "true", insert: "true" } } };
 * ```
 *
 * Rows are loosely typed (`TTypes` defaults to {@link LooseAppTypes}); a test
 * about types uses a committed app with real generated bindings instead.
 */
import { createRequire } from "node:module";
import { resolveSchemaSource, type SchemaSourceInput } from "../schema-source.js";
import { bindApp, boundApp, type App, type AppTypes, type BindAppOptions } from "../typed-app.js";
import type { CompiledPermissionsMap } from "../schema-permissions.js";

/** A `jazz.app.json`, as an object; `format` defaults to 1. */
export interface TestAppFile {
  readonly format?: number;
  readonly tables: Readonly<Record<string, unknown>>;
  readonly [key: string]: unknown;
}

/** A `jazz.permissions.json`, as an object; `format` defaults to 1. */
export interface TestPermissionsFile {
  readonly format?: number;
  readonly tables: Readonly<Record<string, unknown>>;
  readonly defs?: Readonly<Record<string, unknown>>;
  readonly [key: string]: unknown;
}

/** One table of {@link LooseAppTypes}: any column, any relation. */
export interface LooseTableTypes {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  row: { readonly id: string; [column: string]: any };
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  insert: { [column: string]: any };
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  // `any`, not a union of kinds, so every column takes every where operator.
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  columns: { [column: string]: any };
  relations: { [relation: string]: { many: string; nullable?: boolean } };
}

/** The types of an app a test defines inline: every table, column and relation allowed. */
export interface LooseAppTypes extends AppTypes {
  [table: string]: LooseTableTypes;
}

interface Napi {
  compileApp(appJson: string): string;
  loadBackendApp(schema: string, permissions: string): unknown;
}

let napi: Napi | undefined;

function loadNapi(): Napi {
  napi ??= createRequire(import.meta.url)("jazz-napi") as Napi;
  return napi;
}

function fileText(file: TestAppFile | TestPermissionsFile | string): string {
  return typeof file === "string" ? file : JSON.stringify({ format: 1, ...file });
}

/** What the bindings of `app` would pass to `bindApp`. */
export function compileTestApp(app: TestAppFile | string): BindAppOptions<AppTypes> {
  return JSON.parse(loadNapi().compileApp(fileText(app))) as BindAppOptions<AppTypes>;
}

/**
 * An app from an inline `jazz.app.json`, bound as its generated bindings
 * would bind it. `implementations` gives the computed fields and transforms
 * the file declares (what `jazz.computed.ts` exports).
 */
export function testApp<TTypes extends AppTypes = LooseAppTypes>(
  app: TestAppFile | string,
  implementations: {
    computed?: Record<string, Record<string, (row: never) => unknown>>;
    transforms?: Record<string, { from(value: never): unknown; to(value: never): unknown }>;
  } = {},
): App<TTypes> {
  return bindApp({ ...compileTestApp(app), ...implementations } as never) as App<TTypes>;
}

/**
 * The engine's permissions for an inline `jazz.permissions.json`, checked
 * against `app` as a backend checks them, for a test that opens a context
 * or server directly. `createJazz`, `startTestJazz` and
 * `createPolicyTestApp` take the file itself.
 */
export function testPermissions(
  app: SchemaSourceInput,
  permissions: TestPermissionsFile | string,
): CompiledPermissionsMap {
  return loadTestPermissions(app, permissions).permissions;
}

/** {@link testPermissions}, with the bundle and the loader's warnings. */
export function loadTestPermissions(
  app: SchemaSourceInput,
  permissions: TestPermissionsFile | string,
): { permissions: CompiledPermissionsMap; bundle: string; warnings: string[] } {
  const bound = boundOf(app);
  return loadNapi().loadBackendApp(
    JSON.stringify({ schema: bound.schema, relations: bound.relations }),
    fileText(permissions),
  ) as { permissions: CompiledPermissionsMap; bundle: string; warnings: string[] };
}

function boundOf(app: SchemaSourceInput) {
  const bound = boundApp(resolveSchemaSource(app));
  if (!bound) throw new Error("testPermissions: `app` must come from testApp or jazz.generated.ts");
  return bound;
}
