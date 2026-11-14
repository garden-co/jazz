import type { WasmSchema } from "./drivers/types.js";
import { runtimeSchemaJsonReplacer } from "./drivers/schema-wire.js";

/**
 * The structural hash of `schema`, as every store and `jazz.app.json`
 * compute it: Jazz's Rust hash, through jazz-napi, so there is one
 * implementation. Node only; a browser never hashes a schema.
 */
export async function computeSchemaHash(schema: WasmSchema): Promise<string> {
  const { schemaHash } = await import("jazz-napi");
  return schemaHash(JSON.stringify(schema, runtimeSchemaJsonReplacer));
}
