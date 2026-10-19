import { schemaColumns as s } from "../dsl.js";
import { DefinedTable } from "../table-definition.js";
import { rel } from "../relationships.js";

export const spaceAdministrationPrefix = "__e2ee_v1_space_administration_";
export const spaceAdministrationTable = (scope: string) => `${spaceAdministrationPrefix}${scope}`;

const definitions = new Map<string, ReturnType<typeof definition>>();

/** V1 immutable, typed binding from a signed root to its application scope. */
export function spaceAdministrationSchema(scope: string) {
  let table = definitions.get(scope);
  if (!table) definitions.set(scope, (table = definition(scope)));
  return table;
}
function definition(scope: string) {
  return new DefinedTable(
    { rootId: s.uuid(), identifier: s.uuid(), creatorAccountId: s.uuid() },
    {
      root: rel("__e2ee_spaces", "rootId"),
      scope: rel(scope, "identifier"),
      account: rel("__e2ee_account_identities", "creatorAccountId"),
    },
  );
}
