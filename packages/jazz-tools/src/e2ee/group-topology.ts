import { definePermissions, type CompiledPermissions } from "../permissions/index.js";
import type { GroupTables } from "./groups.js";
import { applyGroupTopologyPermissions, groupTopologyTables } from "./group-policy.js";

/** Package-owned topology reads; every other operation retains its application policy. */
export function withGroupTopologyPermissions(
  tables: GroupTables,
  administration: CompiledPermissions,
): CompiledPermissions {
  const reads = definePermissions(tables, applyGroupTopologyPermissions);
  const result = { ...administration };
  for (const table of groupTopologyTables)
    result[table] = { ...administration[table], select: reads[table]!.select };
  return result;
}
