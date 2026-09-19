import { resolveSchemaSource, type SchemaSourceInput } from "../../schema-source.js";
import { schema as s } from "../../schema-namespace.js";
import type { CompiledPermissionsMap } from "../../schema-permissions.js";

const grants = s.definePermissions(s.defineApp({ fixture: s.table({}, {}) }), ({ policy }) => {
  policy.fixture.allowRead.always();
  policy.fixture.allowInsert.always();
  policy.fixture.allowUpdate.always();
  policy.fixture.allowDelete.always();
}).fixture;

/** Internal fixture. Allows every operation on every table. */
export function allowAll(schema: SchemaSourceInput): CompiledPermissionsMap {
  return Object.fromEntries(Object.keys(resolveSchemaSource(schema)).map((name) => [name, grants]));
}
