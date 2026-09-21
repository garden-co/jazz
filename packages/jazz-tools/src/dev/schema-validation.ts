import type { WasmSchema } from "../drivers/types.js";
import {
  mergePermissionsIntoWasmSchema,
  type CompiledPermissionsMap,
} from "../schema-permissions.js";
import { encodeSchema } from "../runtime/native-runtime/schema-codec.js";

/** Run the server's schema and policy compiler without opening a database. */
export async function validateSchemaAndPermissions(
  schema: WasmSchema,
  permissions: CompiledPermissionsMap,
): Promise<void> {
  const combined = mergePermissionsIntoWasmSchema(schema, permissions);
  const { validateSchema } = await import("jazz-napi");
  validateSchema(encodeSchema(combined));
}
