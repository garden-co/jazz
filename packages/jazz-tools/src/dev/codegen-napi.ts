/**
 * Jazz's Rust code generator and app-file loader, through jazz-napi: the one
 * implementation every language's tooling shares (crates/jazz-codegen). The
 * TS side never parses `jazz.app.json` or `jazz.permissions.json` itself.
 * Node only.
 */
import type { CompiledPermissionsMap } from "../schema-permissions.js";
import { resolveSchemaSource, type SchemaSourceInput } from "../schema-source.js";
import { boundApp } from "../typed-app.js";

/** What `loadBackendApp` makes of the bindings' schema and `jazz.permissions.json`. */
export interface LoadedBackendApp {
  /** The current schema's hash. */
  current: string;
  /** The permissions bundle a backend pins its sessions to. */
  bundle: string;
  /** The engine's policies per table. */
  permissions: CompiledPermissionsMap;
  /** What a backend publishes at startup (a `DeploymentRequest`). */
  deployment: unknown;
  warnings: string[];
}

/** The two refusals of the bindings check, told apart by `error.code`. */
export const BINDINGS_STALE = "JAZZ_BINDINGS_STALE";
export const SCHEMA_CHANGED = "JAZZ_SCHEMA_CHANGED";

interface CodegenNapi {
  generate(lang: string, appJson: string): string;
  schemaSourceHash(appJson: string): string;
  migrationsCreate(
    appJson: string,
    bindings: string | null,
    name: string,
  ): { app: string; changed: boolean; todos: string[] };
  validateBindings?(appJson: string, bindings: string | null): void;
  checkBindings?(appJson: string, bindings: string | null): void;
  loadAppArtifact(app: string, permissions: string): unknown;
  loadBackendApp(schema: string, permissions: string): unknown;
  compileApp(appJson: string): string;
}

let napi: Promise<CodegenNapi> | undefined;

/**
 * Hand over the jazz-napi a backend imports statically. createJazz and the
 * backend sessions call this with their own `import * as … from "jazz-napi"`,
 * a specifier bundlers and file tracers (Next.js, Vercel's nft) follow, so a
 * deployed backend ships the addon. The fallback below is for tooling: a
 * specifier they can't follow, so a browser bundle that reaches this module
 * through the isomorphic catalogue never pulls the native addon in.
 */
export function provideCodegenNapi(module: unknown): void {
  provided ??= checkedCodegen(module);
}

/** The tooling fallback's import, replaceable so a test can make it fail as a deployed bundle's would. */
let importFallback = (specifier: string): Promise<unknown> =>
  import(/* @vite-ignore */ /* webpackIgnore: true */ specifier);

/**
 * Tests only: make the fallback import fail (or answer) as `load` does, as it
 * fails in a Next.js/Vercel bundle where the bare specifier doesn't resolve.
 * Returns a restore function.
 */
export function replaceCodegenFallbackForTests(
  load: (specifier: string) => Promise<unknown>,
): () => void {
  const previous = importFallback;
  importFallback = load;
  napi = undefined;
  return () => {
    importFallback = previous;
    napi = undefined;
  };
}

/** The backend's statically imported jazz-napi; wins over the fallback, whatever ran first. */
let provided: CodegenNapi | undefined;

function checkedCodegen(module: unknown): CodegenNapi {
  const loaded = module as Partial<CodegenNapi>;
  if (typeof loaded.generate !== "function" || typeof loaded.loadBackendApp !== "function") {
    throw new Error(
      "This jazz-napi build has no code generator: install jazz-napi at the same version as jazz-tools.",
    );
  }
  return loaded as CodegenNapi;
}

async function codegen(): Promise<CodegenNapi> {
  if (provided) return provided;
  // A specifier bundlers can't follow: this module is Node only, and a browser
  // bundle that reaches it (through the isomorphic catalogue) must not pull in
  // the native addon.
  const specifier = "jazz-napi";
  napi ??= importFallback(specifier).then(checkedCodegen);
  return napi;
}

/** `jazz.generated.ts` for `appJson`. */
export async function generateTs(appJson: string): Promise<string> {
  return (await codegen()).generate("ts", appJson);
}

/** `src/jazz_app.rs` (the `jazz-app` crate's bindings) for `appJson`. */
export async function generateRust(appJson: string): Promise<string> {
  return (await codegen()).generate("rust", appJson);
}

/** `"sha256:<hex>"` of `appJson`, with CRLF read as LF: what the bindings' `source` holds. */
export async function schemaSourceHash(appJson: string): Promise<string> {
  return (await codegen()).schemaSourceHash(appJson);
}

/**
 * Refuse bindings that no longer match `appJson` (code {@link BINDINGS_STALE})
 * and an edited schema without a migration from the bindings' version
 * (code {@link SCHEMA_CHANGED}).
 */
export async function validateBindings(appJson: string, bindings: string | null): Promise<void> {
  const loaded = await codegen();
  const check = loaded.validateBindings ?? loaded.checkBindings;
  if (!check) throw new Error("This jazz-napi build can't check bindings: update jazz-napi.");
  check.call(loaded, appJson, bindings);
}

/** `jazz.app.json` with the bindings' version moved into history and a migration stub to it. */
export async function migrationsCreate(
  appJson: string,
  bindings: string | null,
  name: string,
): Promise<{ app: string; changed: boolean; todos: string[] }> {
  return (await codegen()).migrationsCreate(appJson, bindings, name);
}

/** Read and check both files as a backend does; throws what a backend would refuse. */
export async function loadAppFiles(app: string, permissions: string): Promise<unknown> {
  return (await codegen()).loadAppArtifact(app, permissions);
}

/** Check `jazz.permissions.json` against the bindings' compiled schema, as a deployed backend does. */
export async function loadBackendApp(
  bound: { schema: unknown; relations?: unknown },
  permissions: string,
): Promise<LoadedBackendApp> {
  const json = JSON.stringify({ schema: bound.schema, relations: bound.relations });
  return (await codegen()).loadBackendApp(json, permissions) as LoadedBackendApp;
}

/** The options `jazz.generated.ts` would pass to `bindApp`, for tests that define apps inline. */
export async function compileApp(appJson: string): Promise<Record<string, unknown>> {
  return JSON.parse((await codegen()).compileApp(appJson)) as Record<string, unknown>;
}

/** A `jazz.permissions.json`, as imported or written inline. */
export type PermissionsFile = { readonly format: number } & Readonly<Record<string, unknown>>;

/** Whether `permissions` is a `jazz.permissions.json` rather than the engine's policies. */
export function isPermissionsFile(permissions: unknown): permissions is PermissionsFile {
  return (
    typeof permissions === "object" &&
    permissions !== null &&
    typeof (permissions as { format?: unknown }).format === "number"
  );
}

/** Check `permissions` against `app`'s bindings with the Rust loader, as a deployed backend does. */
export async function loadBackendAppFor(
  app: SchemaSourceInput,
  permissions: PermissionsFile,
): Promise<LoadedBackendApp> {
  const bound = boundApp(resolveSchemaSource(app));
  if (!bound) {
    throw new Error(
      "the app must come from jazz.generated.ts (or testApp) to check jazz.permissions.json against it",
    );
  }
  return loadBackendApp(bound, JSON.stringify(permissions));
}

/**
 * The engine's policies for `permissions`: a `jazz.permissions.json` is
 * checked and compiled against the app's bindings by the Rust loader; engine
 * policies (what a previous load returned) pass through.
 */
export async function enginePermissions(
  app: SchemaSourceInput,
  permissions: PermissionsFile | CompiledPermissionsMap,
): Promise<CompiledPermissionsMap> {
  if (!isPermissionsFile(permissions)) return permissions;
  return (await loadBackendAppFor(app, permissions)).permissions;
}
