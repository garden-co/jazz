import type { WasmSchema } from "./drivers/types.js";
import { structuralSchemaHash } from "./dev/schema-utils.js";

export async function computeSchemaHash(schema: WasmSchema): Promise<string> {
  return structuralSchemaHash(schema);
}
