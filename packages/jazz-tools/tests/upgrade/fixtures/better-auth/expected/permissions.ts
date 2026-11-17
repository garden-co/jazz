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

const personMatches = (personId: s.Uuid, userId: s.Uuid) => exists.people({ id: personId, userId });

const mine = { userId: session.user };

const releaseMatches = (releaseId: s.Uuid, organizationId: s.Uuid) =>
  exists.releases({ id: releaseId, organizationId });

const membershipMatches = (membershipId: s.Uuid, organizationId: s.Uuid) =>
  exists.memberships({ id: membershipId, organizationId });

const rule1 = (organizationId: s.Uuid, releaseId: s.Uuid, membershipId: s.Uuid) =>
  allOf(
    isMembershipWithRole(organizationId, "admin"),
    releaseMatches(releaseId, organizationId),
    membershipMatches(membershipId, organizationId),
  );

const teamMatches = (teamId: s.Uuid, organizationId: s.Uuid) =>
  exists.teams({ id: teamId, organizationId });

const rule2 = (organizationId: s.Uuid, artistId: s.Uuid, catalogueId: s.Uuid) =>
  allOf(
    isMembershipAdminOrEditor(organizationId),
    allOf(
      exists.artists({ id: artistId, organizationId }),
      anyOf({ catalogueId: null }, exists.catalogues({ id: catalogueId, organizationId })),
    ),
  );

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
        personMatches(membership.personId, membership.userId),
        anyOf({ role: "editor" }, { role: "viewer" }),
      ),
    updateIf: (membership) => isMembershipWithRole(membership.organizationId, "admin"),
    updateTo: (membership) =>
      allOf(
        isMembershipWithRole(membership.organizationId, "admin"),
        personMatches(membership.personId, membership.userId),
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
    insert: (releaseAssignment) =>
      rule1(
        releaseAssignment.organizationId,
        releaseAssignment.releaseId,
        releaseAssignment.membershipId,
      ),
    updateIf: (releaseAssignment) =>
      isMembershipWithRole(releaseAssignment.organizationId, "admin"),
    updateTo: (releaseAssignment) =>
      rule1(
        releaseAssignment.organizationId,
        releaseAssignment.releaseId,
        releaseAssignment.membershipId,
      ),
    delete: (releaseAssignment) => isMembershipWithRole(releaseAssignment.organizationId, "admin"),
  },
  releaseTeams: {
    read: (releaseTeam) => isMembership(releaseTeam.organizationId),
    insert: (releaseTeam) =>
      allOf(
        isMembershipAdminOrEditor(releaseTeam.organizationId),
        releaseMatches(releaseTeam.releaseId, releaseTeam.organizationId),
        teamMatches(releaseTeam.teamId, releaseTeam.organizationId),
      ),
    update: false,
    delete: (releaseTeam) => isMembershipAdminOrEditor(releaseTeam.organizationId),
  },
  releases: {
    read: (release) => isMembership(release.organizationId),
    insert: (release) => rule2(release.organizationId, release.artistId, release.catalogueId),
    updateIf: (release) => isMembershipAdminOrEditor(release.organizationId),
    updateTo: (release) => rule2(release.organizationId, release.artistId, release.catalogueId),
    delete: (release) => isMembershipWithRole(release.organizationId, "admin"),
  },
  teamAssignments: {
    read: (teamAssignment) => allowedTo.read(teamAssignment.team),
    insert: (teamAssignment) =>
      allOf(
        allowedTo.insert(teamAssignment.team),
        teamMatches(teamAssignment.teamId, teamAssignment.organizationId),
        membershipMatches(teamAssignment.membershipId, teamAssignment.organizationId),
      ),
    updateIf: (teamAssignment) => allowedTo.update(teamAssignment.team),
    updateTo: (teamAssignment) =>
      allOf(
        allowedTo.update(teamAssignment.team),
        teamMatches(teamAssignment.teamId, teamAssignment.organizationId),
        membershipMatches(teamAssignment.membershipId, teamAssignment.organizationId),
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
