import { definePermissions } from "jazz-tools/permissions";
import { app } from "./schema.js";

/** Tenant admission has one authority: a server-issued bootstrap-admin claim. */
export default definePermissions(app, ({ policy, session, allowedTo, allOf }) => {
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
  const personMatchesMembership = (row: { personId: unknown; userId: unknown }) =>
    policy.people.exists.where({ id: row.personId as never, userId: row.userId as never });
  const artistBelongsToRelease = (row: { artistId: unknown; organizationId: unknown }) =>
    policy.artists.exists.where({
      id: row.artistId as never,
      organizationId: row.organizationId as never,
    });
  const teamMatchesAssignment = (row: { teamId: unknown; organizationId: unknown }) =>
    policy.teams.exists.where({
      id: row.teamId as never,
      organizationId: row.organizationId as never,
    });
  const membershipMatchesAssignment = (row: { membershipId: unknown; organizationId: unknown }) =>
    policy.memberships.exists.where({
      id: row.membershipId as never,
      organizationId: row.organizationId as never,
    });
  const releaseMatchesAssignment = (row: { releaseId: unknown; organizationId: unknown }) =>
    policy.releases.exists.where({
      id: row.releaseId as never,
      organizationId: row.organizationId as never,
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
  policy.memberships.allowInsert.where((row) =>
    // The proposed row must not be able to make itself satisfy `admin(...)`.
    // First admins come only from the trusted bootstrap route; existing admins
    // can invite non-admin members, and may promote them later through update.
    allOf([admin(row.organizationId), personMatchesMembership(row), { role: { ne: "admin" } }]),
  );
  policy.memberships.allowUpdate
    .whereOld((row) => admin(row.organizationId))
    .whereNew((row) => allOf([admin(row.organizationId), personMatchesMembership(row)]));
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
  policy.releases.allowInsert.where((row) =>
    allOf([admin(row.organizationId), artistBelongsToRelease(row)]),
  );
  policy.releases.allowUpdate
    .whereOld((row) => admin(row.organizationId))
    .whereNew((row) => allOf([admin(row.organizationId), artistBelongsToRelease(row)]));
  policy.releases.allowDelete.where((row) => admin(row.organizationId));

  policy.teamAssignments.allowRead.where(allowedTo.read("teamId"));
  policy.teamAssignments.allowInsert.where((row) =>
    allOf([
      allowedTo.insert("teamId"),
      teamMatchesAssignment(row),
      membershipMatchesAssignment(row),
    ]),
  );
  policy.teamAssignments.allowUpdate
    .whereOld(allowedTo.update("teamId"))
    .whereNew((row) =>
      allOf([
        allowedTo.update("teamId"),
        teamMatchesAssignment(row),
        membershipMatchesAssignment(row),
      ]),
    );
  policy.teamAssignments.allowDelete.where(allowedTo.delete("teamId"));
  policy.releaseAssignments.allowRead.where(allowedTo.read("releaseId"));
  policy.releaseAssignments.allowInsert.where((row) =>
    allOf([
      allowedTo.insert("releaseId"),
      releaseMatchesAssignment(row),
      membershipMatchesAssignment(row),
    ]),
  );
  policy.releaseAssignments.allowUpdate
    .whereOld(allowedTo.update("releaseId"))
    .whereNew((row) =>
      allOf([
        allowedTo.update("releaseId"),
        releaseMatchesAssignment(row),
        membershipMatchesAssignment(row),
      ]),
    );
  policy.releaseAssignments.allowDelete.where(allowedTo.delete("releaseId"));
});
