import { schema as s } from "jazz-tools";
import { schema } from "./schema";
import { permissions as betterAuthPermissions } from "./schema-better-auth/schema";

const { definePermissions, exists, allowedTo, session, anyOf, allOf } = schema;

const isMembership = (organizationId: s.Uuid) =>
  exists.memberships({ organizationId, userId: session.user });

const isMembershipAdmin = (organizationId: s.Uuid) =>
  exists.memberships({ organizationId, userId: session.user, role: "admin" });

const isMembershipEditor = (organizationId: s.Uuid) =>
  exists.memberships({ organizationId, userId: session.user, role: "editor" });

const mine = { userId: session.user };

export const permissions = definePermissions({
  ...betterAuthPermissions,
  artists: {
    read: (artist) => isMembership(artist.organizationId),
    insert: (artist) =>
      anyOf(isMembershipAdmin(artist.organizationId), isMembershipEditor(artist.organizationId)),
    update: (artist) =>
      anyOf(isMembershipAdmin(artist.organizationId), isMembershipEditor(artist.organizationId)),
    delete: (artist) => isMembershipAdmin(artist.organizationId),
  },
  catalogues: {
    read: (catalogue) => isMembership(catalogue.organizationId),
    insert: (catalogue) => isMembershipAdmin(catalogue.organizationId),
    update: (catalogue) => isMembershipAdmin(catalogue.organizationId),
    delete: (catalogue) => isMembershipAdmin(catalogue.organizationId),
  },
  memberships: {
    read: (membership) => isMembership(membership.organizationId),
    insert: (membership) =>
      allOf(
        isMembershipAdmin(membership.organizationId),
        membership.person.where({ userId: membership.userId }),
        anyOf({ role: "editor" }, { role: "viewer" }),
      ),
    updateIf: (membership) => isMembershipAdmin(membership.organizationId),
    updateTo: (membership) =>
      allOf(
        isMembershipAdmin(membership.organizationId),
        membership.person.where({ userId: membership.userId }),
        anyOf({ role: "admin" }, { role: "editor" }, { role: "viewer" }),
      ),
    delete: (membership) => isMembershipAdmin(membership.organizationId),
  },
  organizations: {
    read: (organization) => isMembership(organization.id),
    insert: false,
    update: (organization) => isMembershipAdmin(organization.id),
    delete: (organization) => isMembershipAdmin(organization.id),
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
      allOf(
        isMembershipAdmin(releaseAssignment.organizationId),
        releaseAssignment.release.where({ organizationId: releaseAssignment.organizationId }),
        releaseAssignment.membership.where({ organizationId: releaseAssignment.organizationId }),
      ),
    updateIf: (releaseAssignment) => isMembershipAdmin(releaseAssignment.organizationId),
    updateTo: (releaseAssignment) =>
      allOf(
        isMembershipAdmin(releaseAssignment.organizationId),
        releaseAssignment.release.where({ organizationId: releaseAssignment.organizationId }),
        releaseAssignment.membership.where({ organizationId: releaseAssignment.organizationId }),
      ),
    delete: (releaseAssignment) => isMembershipAdmin(releaseAssignment.organizationId),
  },
  releaseTeams: {
    read: (releaseTeam) => isMembership(releaseTeam.organizationId),
    insert: (releaseTeam) =>
      allOf(
        anyOf(
          isMembershipAdmin(releaseTeam.organizationId),
          isMembershipEditor(releaseTeam.organizationId),
        ),
        releaseTeam.release.where({ organizationId: releaseTeam.organizationId }),
        releaseTeam.team.where({ organizationId: releaseTeam.organizationId }),
      ),
    update: false,
    delete: (releaseTeam) =>
      anyOf(
        isMembershipAdmin(releaseTeam.organizationId),
        isMembershipEditor(releaseTeam.organizationId),
      ),
  },
  releases: {
    read: (releas) => isMembership(releas.organizationId),
    insert: (releas) =>
      allOf(
        anyOf(isMembershipAdmin(releas.organizationId), isMembershipEditor(releas.organizationId)),
        allOf(
          releas.artist.where({ organizationId: releas.organizationId }),
          anyOf(
            { catalogueId: null },
            releas.catalogue.where({ organizationId: releas.organizationId }),
          ),
        ),
      ),
    updateIf: (releas) =>
      anyOf(isMembershipAdmin(releas.organizationId), isMembershipEditor(releas.organizationId)),
    updateTo: (releas) =>
      allOf(
        anyOf(isMembershipAdmin(releas.organizationId), isMembershipEditor(releas.organizationId)),
        allOf(
          releas.artist.where({ organizationId: releas.organizationId }),
          anyOf(
            { catalogueId: null },
            releas.catalogue.where({ organizationId: releas.organizationId }),
          ),
        ),
      ),
    delete: (releas) => isMembershipAdmin(releas.organizationId),
  },
  teamAssignments: {
    read: (teamAssignment) => allowedTo.read(teamAssignment.team),
    insert: (teamAssignment) =>
      allOf(
        allowedTo.insert(teamAssignment.team),
        teamAssignment.team.where({ organizationId: teamAssignment.organizationId }),
        teamAssignment.membership.where({ organizationId: teamAssignment.organizationId }),
      ),
    updateIf: (teamAssignment) => allowedTo.update(teamAssignment.team),
    updateTo: (teamAssignment) =>
      allOf(
        allowedTo.update(teamAssignment.team),
        teamAssignment.team.where({ organizationId: teamAssignment.organizationId }),
        teamAssignment.membership.where({ organizationId: teamAssignment.organizationId }),
      ),
    delete: (teamAssignment) => allowedTo.delete(teamAssignment.team),
  },
  teams: {
    read: (team) => isMembership(team.organizationId),
    insert: (team) => isMembershipAdmin(team.organizationId),
    update: (team) => isMembershipAdmin(team.organizationId),
    delete: (team) => isMembershipAdmin(team.organizationId),
  },
});
