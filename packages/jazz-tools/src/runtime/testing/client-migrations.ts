import { computeSchemaHash, serializeForwardLenses } from "../../dev/catalogue.js";
import type { ClientMigrations } from "../client-migrations.js";

/**
 * What `jazz-tools deploy` writes to migrations/bundle.json for an app that
 * went from `from` to `to` along `migration`: clients ship it, so they store
 * each version as the server does (SPEC 10.7).
 */
export async function bundleFor(
  from: { wasmSchema: Parameters<typeof computeSchemaHash>[0] },
  to: { wasmSchema: Parameters<typeof computeSchemaHash>[0] },
  migration: { forward: Parameters<typeof serializeForwardLenses>[0] },
): Promise<ClientMigrations> {
  const [fromHash, toHash] = await Promise.all([
    computeSchemaHash(from.wasmSchema),
    computeSchemaHash(to.wasmSchema),
  ]);
  return {
    schemas: [
      { hash: fromHash, schema: { tables: from.wasmSchema } },
      { hash: toHash, schema: { tables: to.wasmSchema } },
    ],
    migrations: [{ fromHash, toHash, forward: serializeForwardLenses(migration.forward) }],
  };
}
