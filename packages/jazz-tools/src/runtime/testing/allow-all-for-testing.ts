import type { WasmSchema } from "../../drivers/types.js";
import { schema as s } from "../../schema-namespace.js";
import { mergePermissionsIntoWasmSchema } from "../../schema-permissions.js";

const grants = s.definePermissions(s.defineApp({ fixture: s.table({}) }), ({ policy }) => {
  policy.fixture.allowRead.always();
  policy.fixture.allowInsert.always();
  policy.fixture.allowUpdate.always();
  policy.fixture.allowDelete.always();
}).fixture;

/** Internal fixture opt-in. Replaces existing policies on every table. */
export function allowAllForTesting(schema: WasmSchema): WasmSchema {
  return mergePermissionsIntoWasmSchema(
    schema,
    Object.fromEntries(Object.keys(schema).map((name) => [name, grants])),
  );
}
