import { computeSchemaHash, serializeForwardLenses } from "../dev/catalogue.js";
import type { WasmSchema } from "../drivers/types.js";
import type { ClientMigrations } from "../runtime/client-migrations.js";
import { defineApp, type SchemaDefinition } from "../typed-app.js";

/** A schema version: an app, or the definition one is made from. */
type SchemaVersion = { wasmSchema: WasmSchema } | SchemaDefinition;

function wasmSchemaOf(version: SchemaVersion): WasmSchema {
  if ("wasmSchema" in version && typeof version.wasmSchema === "object") {
    return version.wasmSchema as WasmSchema;
  }
  return defineApp(version as SchemaDefinition, { migrations: "none" }).wasmSchema;
}

/**
 * The migrations module `jazz-tools` would generate for an app that started
 * at `app` and then went along `steps`, for tests that define their schemas
 * in code rather than in a schema directory:
 *
 * ```ts
 * const v2 = s.defineApp(after, {
 *   migrations: await appMigrations(before, { to: after, migration }),
 * });
 * ```
 *
 * With no steps it is the single version an app starts with.
 */
export async function appMigrations(
  app: SchemaVersion,
  ...steps: Array<{
    to: SchemaVersion;
    migration: { forward: Parameters<typeof serializeForwardLenses>[0] };
  }>
): Promise<ClientMigrations> {
  const versions = [app, ...steps.map((step) => step.to)].map(wasmSchemaOf);
  const hashes = await Promise.all(versions.map((version) => computeSchemaHash(version)));
  return {
    schemas: versions.map((version, i) => ({ hash: hashes[i]!, schema: { tables: version } })),
    migrations: steps.map((step, i) => ({
      fromHash: hashes[i]!,
      toHash: hashes[i + 1]!,
      forward: serializeForwardLenses(step.migration.forward),
    })),
  };
}
