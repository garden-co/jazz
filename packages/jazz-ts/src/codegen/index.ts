/**
 * Codegen module for generating TypeScript interfaces from schema DSL.
 */

import type { Schema } from "../schema.js";
import { schemaToWasm } from "./schema-reader.js";
import { generateTypes } from "./type-generator.js";

export { schemaToWasm } from "./schema-reader.js";
export { generateTypes, tableNameToInterface } from "./type-generator.js";
export { analyzeRelations, type Relation } from "./relation-analyzer.js";

/**
 * Generate TypeScript client code from a schema.
 *
 * Returns a string containing:
 * - Base interfaces (with id field)
 * - Init interfaces (for inserts, without id)
 * - Exported wasmSchema constant
 */
export function generateClient(schema: Schema): string {
  const wasmSchema = schemaToWasm(schema);
  return generateTypes(wasmSchema);
}
