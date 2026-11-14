import { serializeForwardLenses } from "../dev/catalogue.js";
import { runtimeSchemaJsonReplacer } from "../drivers/schema-wire.js";
import type { WasmSchema } from "../drivers/types.js";
import type { AppArtifact, AppPermissions } from "../runtime/app-artifact.js";
import { computeSchemaHash } from "../schema-hash.js";
import {
  mergePermissionsIntoWasmSchema,
  normalizePermissionsForWasm,
  type CompiledPermissionsMap,
} from "../schema-permissions.js";
import { defineApp, type App, type Schema, type SchemaDefinition } from "../typed-app.js";

/** A schema version: an app, or the definition one is made from. */
type SchemaVersion = { wasmSchema: WasmSchema } | SchemaDefinition;

function wasmSchemaOf(version: SchemaVersion): WasmSchema {
  if ("wasmSchema" in version && typeof version.wasmSchema === "object") {
    return version.wasmSchema as WasmSchema;
  }
  return defineApp(version as SchemaDefinition, { migrations: "none" }).wasmSchema;
}

/** The versions after the first, each with the migration into it. */
type Steps = Array<{
  to: SchemaVersion;
  migration: { forward: Parameters<typeof serializeForwardLenses>[0] };
}>;

/**
 * The `jazz.app.json` that `jazz-tools validate` would write for an app that
 * started at `app` and then went along `steps`: for tests that define their
 * schemas in code rather than in a schema directory. Written by Jazz's Rust
 * writer (through jazz-napi), so Node only.
 *
 * ```ts
 * const v2 = s.defineApp(after, {
 *   artifact: await appArtifact(before, { steps: [{ to: after, migration }] }),
 * });
 * ```
 *
 * With no steps it is the single version an app starts with. A backend
 * serving the app also takes its `jazz.permissions.json`: see
 * {@link appPermissions}.
 */
export async function appArtifact(
  app: SchemaVersion,
  options: { steps?: Steps } = {},
): Promise<AppArtifact> {
  return JSON.parse((await writeFiles(app, options.steps ?? [], {})).app) as AppArtifact;
}

/**
 * The `jazz.permissions.json` that `jazz-tools validate` would write for an
 * app whose current version is `app` and whose permissions.ts makes
 * `permissions`: what a backend serving it (`createJazz`, `startTestJazz`)
 * takes as `permissions`.
 *
 * ```ts
 * const app = s.defineApp(definition, { artifact: await appArtifact(definition) });
 * const permissions = await appPermissions(
 *   app,
 *   s.definePermissions(app, ({ policy }) => { ... }),
 * );
 * createJazz({ app, permissions, store: sqlite({ path }) });
 * ```
 */
export async function appPermissions(
  app: SchemaVersion,
  permissions: CompiledPermissionsMap,
): Promise<AppPermissions> {
  return JSON.parse((await writeFiles(app, [], permissions)).permissions) as AppPermissions;
}

/** Both files for an app that started at `app` and went along `steps`. */
async function writeFiles(
  app: SchemaVersion,
  steps: Steps,
  permissions: CompiledPermissionsMap,
): Promise<{ app: string; permissions: string }> {
  const versions = [app, ...steps.map((step) => step.to)].map((version) =>
    mergePermissionsIntoWasmSchema(wasmSchemaOf(version), {}),
  );
  const hashes = await Promise.all(versions.map((version) => computeSchemaHash(version)));
  const input = {
    current: hashes[hashes.length - 1]!,
    schemas: versions.map((tables) => ({ tables })),
    migrations: steps.map((step, i) => ({
      fromHash: hashes[i]!,
      toHash: hashes[i + 1]!,
      forward: serializeForwardLenses(step.migration.forward),
    })),
    permissions: normalizePermissionsForWasm(permissions),
  };
  const { writeAppArtifact } = await import("jazz-napi");
  return writeAppArtifact(JSON.stringify(input, runtimeSchemaJsonReplacer));
}

/**
 * `s.defineApp(definition, { artifact })` with the single version an app
 * starts with, and the `jazz.permissions.json` the permissions
 * `definePermissions` makes for it, for tests that open an app defined in
 * code on a store that keeps its data (`sqlite()`, `vercel()`, `remote()`),
 * where `createJazz` refuses `{ migrations: "none" }`. Pass the returned
 * `permissions` to the backend.
 */
export async function defineShippedApp<const TSchema extends SchemaDefinition>(
  definition: TSchema,
  definePermissions?: (app: App<Schema<TSchema>>) => CompiledPermissionsMap,
): Promise<{ app: App<Schema<TSchema>>; permissions: AppPermissions }> {
  const app = defineApp(definition as never, {
    artifact: await appArtifact(definition),
  }) as unknown as App<Schema<TSchema>>;
  return { app, permissions: await appPermissions(app, definePermissions?.(app) ?? {}) };
}
