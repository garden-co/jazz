import { serializeForwardLenses } from "../dev/catalogue.js";
import { runtimeSchemaJsonReplacer } from "../drivers/schema-wire.js";
import type { WasmSchema } from "../drivers/types.js";
import type { AppArtifact } from "../runtime/app-artifact.js";
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
 * started at `app` and then went along `steps`, with `permissions` for its
 * last version: for tests that define their schemas in code rather than in a
 * schema directory. Written by Jazz's Rust writer (through jazz-napi), so
 * Node only.
 *
 * ```ts
 * const draft = s.defineApp(after, { migrations: "none" });
 * const permissions = s.definePermissions(draft, ({ policy }) => { ... });
 * const v2 = s.defineApp(after, {
 *   artifact: await appArtifact(before, { steps: [{ to: after, migration }], permissions }),
 * });
 * ```
 *
 * With no steps it is the single version an app starts with. A backend
 * (`createJazz`, `startTestJazz`) refuses the app unless it is given the same
 * permissions; a client never looks at them.
 */
export async function appArtifact(
  app: SchemaVersion,
  options: { steps?: Steps; permissions?: CompiledPermissionsMap } = {},
): Promise<AppArtifact> {
  const steps = options.steps ?? [];
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
    permissions: normalizePermissionsForWasm(options.permissions ?? {}),
  };
  const { writeAppArtifact } = await import("jazz-napi");
  return JSON.parse(
    writeAppArtifact(JSON.stringify(input, runtimeSchemaJsonReplacer)),
  ) as AppArtifact;
}

/**
 * `s.defineApp(definition, { artifact })` with the single version an app
 * starts with and the permissions `definePermissions` makes for it, for tests
 * that open an app defined in code on a store that keeps its data
 * (`sqlite()`, `vercel()`, `remote()`), where `createJazz` refuses
 * `{ migrations: "none" }`. Pass the returned `permissions` to the backend.
 */
export async function defineShippedApp<
  const TSchema extends SchemaDefinition,
  TPermissions extends CompiledPermissionsMap = CompiledPermissionsMap,
>(
  definition: TSchema,
  definePermissions?: (app: App<Schema<TSchema>>) => TPermissions,
): Promise<{ app: App<Schema<TSchema>>; permissions: TPermissions }> {
  const draft = defineApp(definition as never, { migrations: "none" }) as unknown as App<
    Schema<TSchema>
  >;
  const permissions = (definePermissions?.(draft) ?? {}) as TPermissions;
  const app = defineApp(definition as never, {
    artifact: await appArtifact(definition, { permissions }),
  }) as unknown as App<Schema<TSchema>>;
  return { app, permissions };
}
