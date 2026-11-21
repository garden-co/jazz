/**
 * A membership's role and the bookkeeping the rules check (see
 * instant.schema.ts): who is an admin or editor, and its unique key.
 */
export function membershipFields(organizationId: string, userId: string, role: string) {
  return {
    userId,
    role,
    adminUserId: role === "admin" ? userId : null,
    editorUserId: role === "admin" || role === "editor" ? userId : null,
    memberKey: `${organizationId}/${userId}`,
  };
}
