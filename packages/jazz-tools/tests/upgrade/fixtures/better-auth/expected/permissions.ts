import { schema as s } from "jazz-tools";
import { schema } from "./schema";
import { permissions as betterAuthPermissions } from "./schema-better-auth/schema";

const { definePermissions, exists, allowedTo, session, anyOf, allOf } = schema;

const isMembership = (organizationId: s.Uuid) =>
  exists.memberships({ organizationId, userId: session.user });

const isMembershipWithRole = (organizationId: s.Uuid, role: s.Value<"string">) =>
  exists.memberships({ organizationId, userId: session.user, role });

const isMembershipAdminOrEditor = (organizationId: s.Uuid) =>
  anyOf(
    isMembershipWithRole(organizationId, "admin"),
    isMembershipWithRole(organizationId, "editor"),
  );

const personWithUserId = (membership: s.Row<typeof schema, "memberships">) =>
  membership.person.where({ userId: membership.userId });

const mine = { userId: session.user };

const isMembershipAdminAndMore = (releaseAssignment: s.Row<typeof schema, "releaseAssignments">) =>
  allOf(
    isMembershipWithRole(releaseAssignment.organizationId, "admin"),
    releaseAssignment.release.where({ organizationId: releaseAssignment.organizationId }),
    releaseAssignment.membership.where({ organizationId: releaseAssignment.organizationId }),
  );

const isMembershipAdminOrEditorAndMore = (release: s.Row<typeof schema, "releases">) =>
  allOf(
    isMembershipAdminOrEditor(release.organizationId),
    allOf(
      release.artist.where({ organizationId: release.organizationId }),
      anyOf(
        { catalogueId: null },
        release.catalogue.where({ organizationId: release.organizationId }),
      ),
    ),
  );

const teamWithOrganizationId = (teamAssignment: s.Row<typeof schema, "teamAssignments">) =>
  teamAssignment.team.where({ organizationId: teamAssignment.organizationId });

const membershipWithOrganizationId = (teamAssignment: s.Row<typeof schema, "teamAssignments">) =>
  teamAssignment.membership.where({ organizationId: teamAssignment.organizationId });

export const permissions = definePermissions({
  ...betterAuthPermissions,
  artists: {
    read: (artist) => isMembership(artist.organizationId),
    insert: (artist) => isMembershipAdminOrEditor(artist.organizationId),
    update: (artist) => isMembershipAdminOrEditor(artist.organizationId),
    delete: (artist) => isMembershipWithRole(artist.organizationId, "admin"),
  },
  catalogues: {
    read: (catalogue) => isMembership(catalogue.organizationId),
    insert: (catalogue) => isMembershipWithRole(catalogue.organizationId, "admin"),
    update: (catalogue) => isMembershipWithRole(catalogue.organizationId, "admin"),
    delete: (catalogue) => isMembershipWithRole(catalogue.organizationId, "admin"),
  },
  memberships: {
    read: (membership) => isMembership(membership.organizationId),
    insert: (membership) =>
      allOf(
        isMembershipWithRole(membership.organizationId, "admin"),
        personWithUserId(membership),
        anyOf({ role: "editor" }, { role: "viewer" }),
      ),
    updateIf: (membership) => isMembershipWithRole(membership.organizationId, "admin"),
    updateTo: (membership) =>
      allOf(
        isMembershipWithRole(membership.organizationId, "admin"),
        personWithUserId(membership),
        anyOf({ role: "admin" }, { role: "editor" }, { role: "viewer" }),
      ),
    delete: (membership) => isMembershipWithRole(membership.organizationId, "admin"),
  },
  organizations: {
    read: (organization) => isMembership(organization.id),
    insert: false,
    update: (organization) => isMembershipWithRole(organization.id, "admin"),
    delete: (organization) => isMembershipWithRole(organization.id, "admin"),
  },
  people: {
    read: (person) => anyOf(mine, allowedTo.read(person.membershipsViaPerson)),
    insert: false,
    update: mine,
    delete: false,
  },
  personEmails: {
    read: false,
    insert: false,
    update: false,
    delete: false,
  },
  releaseAssignments: {
    read: (releaseAssignment) => allowedTo.read(releaseAssignment.release),
    insert: (releaseAssignment) => isMembershipAdminAndMore(releaseAssignment),
    updateIf: (releaseAssignment) =>
      isMembershipWithRole(releaseAssignment.organizationId, "admin"),
    updateTo: (releaseAssignment) => isMembershipAdminAndMore(releaseAssignment),
    delete: (releaseAssignment) => isMembershipWithRole(releaseAssignment.organizationId, "admin"),
  },
  releaseTeams: {
    read: (releaseTeam) => isMembership(releaseTeam.organizationId),
    insert: (releaseTeam) =>
      allOf(
        isMembershipAdminOrEditor(releaseTeam.organizationId),
        releaseTeam.release.where({ organizationId: releaseTeam.organizationId }),
        releaseTeam.team.where({ organizationId: releaseTeam.organizationId }),
      ),
    update: false,
    delete: (releaseTeam) => isMembershipAdminOrEditor(releaseTeam.organizationId),
  },
  releases: {
    read: (release) => isMembership(release.organizationId),
    insert: (release) => isMembershipAdminOrEditorAndMore(release),
    updateIf: (release) => isMembershipAdminOrEditor(release.organizationId),
    updateTo: (release) => isMembershipAdminOrEditorAndMore(release),
    delete: (release) => isMembershipWithRole(release.organizationId, "admin"),
  },
  teamAssignments: {
    read: (teamAssignment) => allowedTo.read(teamAssignment.team),
    insert: (teamAssignment) =>
      allOf(
        allowedTo.insert(teamAssignment.team),
        teamWithOrganizationId(teamAssignment),
        membershipWithOrganizationId(teamAssignment),
      ),
    updateIf: (teamAssignment) => allowedTo.update(teamAssignment.team),
    updateTo: (teamAssignment) =>
      allOf(
        allowedTo.update(teamAssignment.team),
        teamWithOrganizationId(teamAssignment),
        membershipWithOrganizationId(teamAssignment),
      ),
    delete: (teamAssignment) => allowedTo.delete(teamAssignment.team),
  },
  teams: {
    read: (team) => isMembership(team.organizationId),
    insert: (team) => isMembershipWithRole(team.organizationId, "admin"),
    update: (team) => isMembershipWithRole(team.organizationId, "admin"),
    delete: (team) => isMembershipWithRole(team.organizationId, "admin"),
  },
});
