import { resolveSchemaSource, type SchemaSourceInput } from "../../schema-source.js";
import type { CompiledPermissionsMap } from "../../schema-permissions.js";

/**
 * Every operation allowed: the engine's form of
 * `{ "read": "true", "insert": "true", "update": "true", "delete": "true" }`.
 */
// The engine writes the absent side as `null`, which the TS type leaves out.
const grants = {
  select: { using: { type: "True" }, with_check: null },
  insert: { using: null, with_check: { type: "True" } },
  update: { using: { type: "True" }, with_check: { type: "True" } },
  delete: { using: { type: "True" }, with_check: null },
} as unknown as CompiledPermissionsMap[string];

/** Internal fixture. Allows every operation on every table. */
export function allowAll(schema: SchemaSourceInput): CompiledPermissionsMap {
  return Object.fromEntries(Object.keys(resolveSchemaSource(schema)).map((name) => [name, grants]));
}
