import { schema as s } from "jazz-tools";
import { schema } from "./schema";
import { permissions as betterAuthPermissions } from "./schema-better-auth/schema";
import { catalogueEditors, invitableRoles, roles } from "./src/roles";

const { definePermissions, exists, allowedTo, session, anyOf, allOf } = schema;

/**
 * Tenant admission has one authority: the app-owned backend bootstrap route.
 * Roles (see src/roles.ts): admins run the organization, editors maintain the
 * catalogue, viewers read it.
 */

// userId is deliberately denormalized so this is an indexed membership lookup.
const isOrgMember = (organizationId: s.Uuid) =>
  exists.memberships({ organizationId, userId: session.user });

const hasOrgRole = (organizationId: s.Uuid, role: string) =>
  exists.memberships({ organizationId, userId: session.user, role });

const isOrgAdmin = (organizationId: s.Uuid) => hasOrgRole(organizationId, "admin");

const canEditCatalogue = (organizationId: s.Uuid) =>
  anyOf(...catalogueEditors.map((role) => hasOrgRole(organizationId, role)));

// Only the roles the app knows; a new member can't start as an admin.
const roleIsOneOf = (allowed: readonly string[]) => anyOf(...allowed.map((role) => ({ role })));

const mine = { userId: session.user };

// Every relation a release points at must live in the release's tenant.
const releaseRelationsInTenant = (release: s.Row<typeof schema, "releases">) =>
  allOf(
    release.artist.where({ organizationId: release.organizationId }),
    anyOf(
      { catalogueId: null },
      release.catalogue.where({ organizationId: release.organizationId }),
    ),
  );

export const permissions = definePermissions({
  ...betterAuthPermissions,
  artists: {
    read: (artist) => isOrgMember(artist.organizationId),
    insert: (artist) => canEditCatalogue(artist.organizationId),
    update: (artist) => canEditCatalogue(artist.organizationId),
    delete: (artist) => isOrgAdmin(artist.organizationId),
  },
  catalogues: {
    read: (catalogue) => isOrgMember(catalogue.organizationId),
    insert: (catalogue) => isOrgAdmin(catalogue.organizationId),
    update: (catalogue) => isOrgAdmin(catalogue.organizationId),
    delete: (catalogue) => isOrgAdmin(catalogue.organizationId),
  },
  memberships: {
    read: (membership) => isOrgMember(membership.organizationId),
    // The proposed row must not be able to make itself satisfy `isOrgAdmin(...)`.
    // First admins come only from the trusted bootstrap route; existing admins
    // can invite non-admin members, and may promote them later through update.
    insert: (membership) =>
      allOf(
        isOrgAdmin(membership.organizationId),
        membership.person.where({ userId: membership.userId }),
        roleIsOneOf(invitableRoles),
      ),
    updateIf: (membership) => isOrgAdmin(membership.organizationId),
    updateTo: (membership) =>
      allOf(
        isOrgAdmin(membership.organizationId),
        membership.person.where({ userId: membership.userId }),
        roleIsOneOf(roles),
      ),
    delete: (membership) => isOrgAdmin(membership.organizationId),
  },
  organizations: {
    read: (organization) => isOrgMember(organization.id),
    insert: false,
    update: (organization) => isOrgAdmin(organization.id),
    delete: (organization) => isOrgAdmin(organization.id),
  },
  people: {
    // You see your own profile and the people who share a label with you.
    read: (person) => anyOf(mine, allowedTo.read(person.membershipsViaPerson)),
    // Profiles are created only by the trusted bootstrap transaction. This
    // prevents a client-created duplicate from splitting a user's membership
    // identity before their personal tenant is established.
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
  // Per-person release roles (such as "owner") grant accountability, so only
  // admins hand them out, even though editors may edit the release itself.
  releaseAssignments: {
    read: (releaseAssignment) => allowedTo.read(releaseAssignment.release),
    insert: (releaseAssignment) =>
      allOf(
        isOrgAdmin(releaseAssignment.organizationId),
        releaseAssignment.release.where({ organizationId: releaseAssignment.organizationId }),
        releaseAssignment.membership.where({ organizationId: releaseAssignment.organizationId }),
      ),
    updateIf: (releaseAssignment) => isOrgAdmin(releaseAssignment.organizationId),
    updateTo: (releaseAssignment) =>
      allOf(
        isOrgAdmin(releaseAssignment.organizationId),
        releaseAssignment.release.where({ organizationId: releaseAssignment.organizationId }),
        releaseAssignment.membership.where({ organizationId: releaseAssignment.organizationId }),
      ),
    delete: (releaseAssignment) => isOrgAdmin(releaseAssignment.organizationId),
  },
  // Editors staff releases with teams; the team must belong to the same tenant.
  releaseTeams: {
    read: (releaseTeam) => isOrgMember(releaseTeam.organizationId),
    insert: (releaseTeam) =>
      allOf(
        canEditCatalogue(releaseTeam.organizationId),
        releaseTeam.release.where({ organizationId: releaseTeam.organizationId }),
        releaseTeam.team.where({ organizationId: releaseTeam.organizationId }),
      ),
    update: false,
    delete: (releaseTeam) => canEditCatalogue(releaseTeam.organizationId),
  },
  releases: {
    read: (release) => isOrgMember(release.organizationId),
    insert: (release) =>
      allOf(canEditCatalogue(release.organizationId), releaseRelationsInTenant(release)),
    updateIf: (release) => canEditCatalogue(release.organizationId),
    updateTo: (release) =>
      allOf(canEditCatalogue(release.organizationId), releaseRelationsInTenant(release)),
    delete: (release) => isOrgAdmin(release.organizationId),
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
    read: (team) => isOrgMember(team.organizationId),
    insert: (team) => isOrgAdmin(team.organizationId),
    update: (team) => isOrgAdmin(team.organizationId),
    delete: (team) => isOrgAdmin(team.organizationId),
  },
});
