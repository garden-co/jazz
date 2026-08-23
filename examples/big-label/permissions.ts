import { definePermissions } from "jazz-tools/permissions";
import { app } from "./schema.js";

/** Every tenant-owned row is visible only through an organization membership. */
export default definePermissions(app, ({ policy, session, allowedTo }) => {
  policy.people.allowRead.where({});
  policy.people.allowInsert.where({ userId: session.user_id });
  policy.people.allowUpdate.where({ userId: session.user_id });

  // `userId` is deliberately denormalized here: the policy can use an indexed
  // membership lookup without exposing another tenant's person record.
  const member = (organizationId: unknown) =>
    policy.memberships.exists.where({
      organizationId: organizationId as never,
      userId: session.user_id,
    });
  policy.organizations.allowRead.where((row) => member(row.id));
  policy.teams.allowRead.where((row) => member(row.organizationId));
  policy.memberships.allowRead.where((row) => member(row.organizationId));
  policy.artists.allowRead.where((row) => member(row.organizationId));
  policy.releases.allowRead.where((row) => member(row.organizationId));
  policy.teamAssignments.allowRead.where(allowedTo.read("teamId"));
  policy.releaseAssignments.allowRead.where(allowedTo.read("releaseId"));
});
