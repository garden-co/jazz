import type { PolicyContext } from "../permissions/index.js";
import type { GroupTables } from "./groups.js";

export const groupTopologyTables = [
  "__e2ee_groups",
  "__e2ee_group_membership",
  "__e2ee_group_successors",
] as const;

/** Topology is public to authenticated accounts; administration remains explicit. */
export function applyGroupTopologyPermissions({
  policy,
  session,
}: PolicyContext<GroupTables>): void {
  const authenticated = session.where({ authMode: { in: ["local-first", "external"] } });
  policy.__e2ee_groups.allowRead.where(authenticated);
  policy.__e2ee_group_membership.allowRead.where(authenticated);
  policy.__e2ee_group_successors.allowRead.where(authenticated);
}
