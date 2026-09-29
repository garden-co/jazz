import { definePermissions } from "jazz-tools/permissions";
import { permissions as betterAuthPermissions } from "./schema-better-auth/schema";
import { app } from "./schema.js";
import { catalogueEditors } from "./src/roles.js";

/**
 * Tenant admission has one authority: the app-owned backend bootstrap route.
 * Roles (see src/roles.ts): admins run the organization, editors maintain the
 * catalogue, viewers read it.
 */
const tenantPermissions = definePermissions(app, ({ policy, session, allowedTo, allOf, anyOf }) => {
  // userId is deliberately denormalized so this is an indexed membership lookup.
  const member = (organizationId: unknown) =>
    policy.memberships.exists.where({
      organizationId: organizationId as never,
      userId: session.user.account,
    });
  const admin = (organizationId: unknown) =>
    policy.memberships.exists.where({
      organizationId: organizationId as never,
      userId: session.user.account,
      role: "admin",
    });
  const editor = (organizationId: unknown) =>
    anyOf(
      catalogueEditors.map((role) =>
        policy.memberships.exists.where({
          organizationId: organizationId as never,
          userId: session.user.account,
          role,
        }),
      ),
    );
  const personMatchesMembership = (row: { personId: unknown; userId: unknown }) =>
    policy.people.exists.where({ id: row.personId as never, userId: row.userId as never });
  const artistBelongsToRelease = (row: { artistId: unknown; organizationId: unknown }) =>
    policy.artists.exists.where({
      id: row.artistId as never,
      organizationId: row.organizationId as never,
    });
  const catalogueBelongsToRelease = (row: { catalogueId: unknown; organizationId: unknown }) =>
    anyOf([
      { catalogueId: { isNull: true } },
      policy.catalogues.exists.where({
        id: row.catalogueId as never,
        organizationId: row.organizationId as never,
      }),
    ]);
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
  // Profiles are created only by the trusted bootstrap transaction. This
  // prevents a client-created duplicate from splitting a user's membership
  // identity before their personal tenant is established.
  policy.people.allowInsert.never();
  policy.people.allowUpdate
    .whereOld({ userId: session.user.account })
    .whereNew({ userId: session.user.account });
  policy.people.allowDelete.never();

  policy.organizations.allowRead.where((row) => member(row.id));
  policy.organizations.allowInsert.never();
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
  policy.artists.allowInsert.where((row) => editor(row.organizationId));
  policy.artists.allowUpdate
    .whereOld((row) => editor(row.organizationId))
    .whereNew((row) => editor(row.organizationId));
  policy.artists.allowDelete.where((row) => admin(row.organizationId));

  policy.catalogues.allowRead.where((row) => member(row.organizationId));
  policy.catalogues.allowInsert.where((row) => admin(row.organizationId));
  policy.catalogues.allowUpdate
    .whereOld((row) => admin(row.organizationId))
    .whereNew((row) => admin(row.organizationId));
  policy.catalogues.allowDelete.where((row) => admin(row.organizationId));

  // Every relation a release points at must live in the release's tenant.
  const releaseRelationsInTenant = (row: {
    artistId: unknown;
    catalogueId: unknown;
    organizationId: unknown;
  }) => allOf([artistBelongsToRelease(row), catalogueBelongsToRelease(row)]);
  policy.releases.allowRead.where((row) => member(row.organizationId));
  policy.releases.allowInsert.where((row) =>
    allOf([editor(row.organizationId), releaseRelationsInTenant(row)]),
  );
  policy.releases.allowUpdate
    .whereOld((row) => editor(row.organizationId))
    .whereNew((row) => allOf([editor(row.organizationId), releaseRelationsInTenant(row)]));
  policy.releases.allowDelete.where((row) => admin(row.organizationId));

  policy.teamAssignments.allowRead.where(allowedTo.read("team"));
  policy.teamAssignments.allowInsert.where((row) =>
    allOf([allowedTo.insert("team"), teamMatchesAssignment(row), membershipMatchesAssignment(row)]),
  );
  policy.teamAssignments.allowUpdate
    .whereOld(allowedTo.update("team"))
    .whereNew((row) =>
      allOf([
        allowedTo.update("team"),
        teamMatchesAssignment(row),
        membershipMatchesAssignment(row),
      ]),
    );
  policy.teamAssignments.allowDelete.where(allowedTo.delete("team"));
  // Per-person release roles (such as "owner") grant accountability, so only
  // admins hand them out, even though editors may edit the release itself.
  policy.releaseAssignments.allowRead.where(allowedTo.read("release"));
  policy.releaseAssignments.allowInsert.where((row) =>
    allOf([
      admin(row.organizationId),
      releaseMatchesAssignment(row),
      membershipMatchesAssignment(row),
    ]),
  );
  policy.releaseAssignments.allowUpdate
    .whereOld((row) => admin(row.organizationId))
    .whereNew((row) =>
      allOf([
        admin(row.organizationId),
        releaseMatchesAssignment(row),
        membershipMatchesAssignment(row),
      ]),
    );
  policy.releaseAssignments.allowDelete.where((row) => admin(row.organizationId));

  // Editors staff releases with teams; the team must belong to the same tenant.
  policy.releaseTeams.allowRead.where((row) => member(row.organizationId));
  policy.releaseTeams.allowInsert.where((row) =>
    allOf([editor(row.organizationId), releaseMatchesAssignment(row), teamMatchesAssignment(row)]),
  );
  policy.releaseTeams.allowUpdate.never();
  policy.releaseTeams.allowDelete.where((row) => editor(row.organizationId));
});

export default {
  ...betterAuthPermissions,
  ...tenantPermissions,
};
