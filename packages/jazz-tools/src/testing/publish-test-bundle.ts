import { deployArtifacts } from "../dev/catalogue.js";
import { fetchMigrationGraph } from "../dev/migration-graph.js";
import type { WasmSchema } from "../drivers/types.js";
import { computeSchemaHash } from "../schema-hash.js";
import {
  mergePermissionsIntoWasmSchema,
  type CompiledPermissionsMap,
} from "../schema-permissions.js";

/**
 * Publish `app` with `permissions` as a bundle, as createJazz does on boot,
 * and answer its id. A backend link pinned to it may act for users; an
 * unpinned one may not.
 */
export async function publishTestBundle(
  server: { appId: string; serverUrl: string; adminSecret: string },
  app: { wasmSchema: WasmSchema },
  permissions: CompiledPermissionsMap,
): Promise<string> {
  const structural = mergePermissionsIntoWasmSchema(app.wasmSchema, {});
  const targetSchemaHash = await computeSchemaHash(structural);
  const { bundle } = await deployArtifacts(server, await fetchMigrationGraph(server), {
    targetSchemaHash,
    schemas: [{ hash: targetSchemaHash, schema: structural }],
    migrations: [],
    permissions,
    activate: false,
  });
  if (!bundle) throw new Error("the core did not publish a permissions bundle");
  return bundle;
}
