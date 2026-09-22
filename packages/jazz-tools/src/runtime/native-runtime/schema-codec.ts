import type { WasmSchema } from "../../drivers/types.js";
import { serializeSchemaSource } from "../../drivers/schema-wire.js";

const textEncoder = new TextEncoder();

/**
 * Encode the developer-authored schema AST for the native runtime.
 *
 * Rust owns schema validation and PolicyExpr lowering. Keeping this boundary
 * source-level prevents the TypeScript runtime from duplicating the engine's
 * policy compiler or persisting its internal Query representation.
 */
export function encodeSchema(schema: WasmSchema): Uint8Array {
  return textEncoder.encode(serializeSchemaSource(schema));
}
