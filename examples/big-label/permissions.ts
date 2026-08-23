import { definePermissions } from "jazz-tools/permissions";
import { app } from "./schema.js";

/** Tenant admission has one authority: a server-issued bootstrap-admin claim. */
export default definePermissions(app, ({ policy, session, allowedTo }) => {
  const bootstrapAdmin = session.where({ "claims.biglabel_admin": true });
  // userId is deliberately denormalized so this is an indexed membership lookup.
  const member = (organizationId: unknown) =>
    policy.memberships.exists.where({
      organizationId: organizationId as never,
      userId: session.user_id,
    });
  const admin = (organizationId: unknown) =>
    policy.memberships.exists.where({
      organizationId: organizationId as never,
      userId: session.user_id,
      role: "admin",
    });

  policy.people.allowRead.where({});
  policy.people.allowInsert.where({ userId: session.user_id });
  policy.people.allowUpdate
    .whereOld({ userId: session.user_id })
    .whereNew({ userId: session.user_id });
  policy.people.allowDelete.where({ userId: session.user_id });

  policy.organizations.allowRead.where((row) => member(row.id));
  policy.organizations.allowInsert.where(bootstrapAdmin);
  policy.organizations.allowUpdate
    .whereOld((row) => admin(row.id))
    .whereNew((row) => admin(row.id));
  policy.organizations.allowDelete.where((row) => admin(row.id));

  policy.memberships.allowRead.where((row) => member(row.organizationId));
  policy.memberships.allowInsert.where((row) => admin(row.organizationId));
  policy.memberships.allowUpdate
    .whereOld((row) => admin(row.organizationId))
    .whereNew((row) => admin(row.organizationId));
  policy.memberships.allowDelete.where((row) => admin(row.organizationId));

  policy.teams.allowRead.where((row) => member(row.organizationId));
  policy.teams.allowInsert.where((row) => admin(row.organizationId));
  policy.teams.allowUpdate
    .whereOld((row) => admin(row.organizationId))
    .whereNew((row) => admin(row.organizationId));
  policy.teams.allowDelete.where((row) => admin(row.organizationId));

  policy.artists.allowRead.where((row) => member(row.organizationId));
  policy.artists.allowInsert.where((row) => admin(row.organizationId));
  policy.artists.allowUpdate
    .whereOld((row) => admin(row.organizationId))
    .whereNew((row) => admin(row.organizationId));
  policy.artists.allowDelete.where((row) => admin(row.organizationId));

  policy.releases.allowRead.where((row) => member(row.organizationId));
  policy.releases.allowInsert.where((row) => admin(row.organizationId));
  policy.releases.allowUpdate
    .whereOld((row) => admin(row.organizationId))
    .whereNew((row) => admin(row.organizationId));
  policy.releases.allowDelete.where((row) => admin(row.organizationId));

  policy.teamAssignments.allowRead.where(allowedTo.read("teamId"));
  policy.teamAssignments.allowInsert.where(allowedTo.insert("teamId"));
  policy.teamAssignments.allowUpdate
    .whereOld(allowedTo.update("teamId"))
    .whereNew(allowedTo.update("teamId"));
  policy.teamAssignments.allowDelete.where(allowedTo.delete("teamId"));
  policy.releaseAssignments.allowRead.where(allowedTo.read("releaseId"));
  policy.releaseAssignments.allowInsert.where(allowedTo.insert("releaseId"));
  policy.releaseAssignments.allowUpdate
    .whereOld(allowedTo.update("releaseId"))
    .whereNew(allowedTo.update("releaseId"));
  policy.releaseAssignments.allowDelete.where(allowedTo.delete("releaseId"));
});
