export const currentGroupMembersTable = "__e2ee_v1_group_members";
export const currentSpaceMembersPrefix = "__e2ee_v1_space_members_";
export const permissionSpacesPrefix = "__e2ee_v1_permission_spaces_";
export const permissionSpaceColumn = "__e2ee_permission_space";
export function currentSpaceMembersTable<const Scope extends string>(scope: Scope) {
  return `${currentSpaceMembersPrefix}${scope}` as const;
}
export function permissionSpacesTable<const Scope extends string>(scope: Scope) {
  return `${permissionSpacesPrefix}${scope}` as const;
}
