// examples/big-label's schema and permissions in the TS DSL (acceptance fixture).
import { schema as s } from "../../index.js";
import * as betterAuth from "./big-label-better-auth.js";

const roles = ["admin", "editor", "viewer"] as const;
const catalogueEditors = ["admin", "editor"] as const;
const invitableRoles = ["editor", "viewer"] as const;

export const schema = s.defineSchema({
  ...betterAuth.tables,
  organizations: s.table(
    { name: s.string(), slug: s.string() },
    {
      teamsViaOrganization: s.reverse("teams", "organization"),
      membershipsViaOrganization: s.reverse("memberships", "organization"),
      teamAssignmentsViaOrganization: s.reverse("teamAssignments", "organization"),
      artistsViaOrganization: s.reverse("artists", "organization"),
      releasesViaOrganization: s.reverse("releases", "organization"),
      releaseAssignmentsViaOrganization: s.reverse("releaseAssignments", "organization"),
      cataloguesViaOrganization: s.reverse("catalogues", "organization"),
      releaseTeamsViaOrganization: s.reverse("releaseTeams", "organization"),
    },
  ),
  people: s.table(
    { userId: s.uuid(), name: s.string() },
    {
      membershipsViaPerson: s.reverse("memberships", "person"),
      personEmailsViaPerson: s.reverse("personEmails", "person"),
    },
  ),
  // Sign-in emails, written only by the trusted server (bootstrap) and read
  // only by it (adding a member by email). Clients never see this table.
  personEmails: s.table(
    { personId: s.uuid(), email: s.string() },
    { person: s.rel("people", "personId") },
  ),
  teams: s.table(
    { organizationId: s.uuid(), name: s.string() },
    {
      organization: s.rel("organizations", "organizationId"),
      teamAssignmentsViaTeam: s.reverse("teamAssignments", "team"),
      releaseTeamsViaTeam: s.reverse("releaseTeams", "team"),
    },
  ),
  memberships: s.table(
    {
      organizationId: s.uuid(),
      personId: s.uuid(),
      userId: s.uuid(),
      role: s.string(),
    },
    {
      organization: s.rel("organizations", "organizationId"),
      person: s.rel("people", "personId"),
      teamAssignmentsViaMembership: s.reverse("teamAssignments", "membership"),
      releaseAssignmentsViaMembership: s.reverse("releaseAssignments", "membership"),
    },
  ),
  teamAssignments: s.table(
    {
      organizationId: s.uuid(),
      teamId: s.uuid(),
      membershipId: s.uuid(),
      role: s.string(),
    },
    {
      organization: s.rel("organizations", "organizationId"),
      team: s.rel("teams", "teamId"),
      membership: s.rel("memberships", "membershipId"),
    },
  ),
  artists: s.table(
    {
      organizationId: s.uuid(),
      name: s.string(),
      genre: s.string(),
      status: s.string(),
      // Lower-cased name and genre. `contains` is case-sensitive, so search
      // matches against this denormalized key.
      searchKey: s.string(),
    },
    {
      organization: s.rel("organizations", "organizationId"),
      releasesViaArtist: s.reverse("releases", "artist"),
    },
  ),
  catalogues: s.table(
    { organizationId: s.uuid(), name: s.string(), code: s.string() },
    {
      organization: s.rel("organizations", "organizationId"),
      releasesViaCatalogue: s.reverse("releases", "catalogue"),
    },
  ),
  releases: s.table(
    {
      organizationId: s.uuid(),
      artistId: s.uuid(),
      catalogueId: s.uuid().optional(),
      catalogNumber: s.string(),
      // The catalogue number's trailing digits, so "NFC-1000" sorts after
      // "NFC-999" when suggesting the next number.
      catalogSequence: s.int().optional(),
      title: s.string(),
      format: s.string(),
      releaseDate: s.timestamp(),
      status: s.string(),
      // Lower-cased title and catalogue number, for case-insensitive search.
      searchKey: s.string(),
    },
    {
      organization: s.rel("organizations", "organizationId"),
      artist: s.rel("artists", "artistId"),
      catalogue: s.rel("catalogues", "catalogueId"),
      releaseAssignmentsViaRelease: s.reverse("releaseAssignments", "release"),
      releaseTeamsViaRelease: s.reverse("releaseTeams", "release"),
    },
  ),
  // Which teams work on a release. Both sides must belong to the same tenant.
  releaseTeams: s.table(
    { organizationId: s.uuid(), releaseId: s.uuid(), teamId: s.uuid() },
    {
      organization: s.rel("organizations", "organizationId"),
      release: s.rel("releases", "releaseId"),
      team: s.rel("teams", "teamId"),
    },
  ),
  releaseAssignments: s.table(
    {
      organizationId: s.uuid(),
      releaseId: s.uuid(),
      membershipId: s.uuid(),
      role: s.string(),
    },
    {
      organization: s.rel("organizations", "organizationId"),
      release: s.rel("releases", "releaseId"),
      membership: s.rel("memberships", "membershipId"),
    },
  ),
});

const { definePermissions, exists, allowedTo, session, anyOf, allOf } = schema;

const member = (organizationId: s.Uuid) =>
  exists.memberships({ organizationId, userId: session.user });
const admin = (organizationId: s.Uuid) =>
  exists.memberships({ organizationId, userId: session.user, role: "admin" });
const editor = (organizationId: s.Uuid) =>
  exists.memberships({ organizationId, userId: session.user, role: catalogueEditors });
const inTenant = (org: s.Uuid, ...rels: s.Rel<{ organizationId: s.Uuid }>[]) =>
  allOf(...rels.map((rel) => rel.where({ organizationId: org })));
const releaseRefsInTenant = (r: s.Row<typeof schema, "releases">) =>
  allOf(
    inTenant(r.organizationId, r.artist),
    anyOf({ catalogueId: null }, inTenant(r.organizationId, r.catalogue)),
  );
const personMatches = (r: s.Row<typeof schema, "memberships">) =>
  r.person.where({ userId: r.userId });

export const permissions = definePermissions({
  ...betterAuth.permissions,
  personEmails: {},
  people: {
    read: (r) => anyOf({ userId: session.user }, allowedTo.read(r.membershipsViaPerson)),
    update: { userId: session.user },
  },
  organizations: {
    read: (r) => member(r.id),
    update: (r) => admin(r.id),
    delete: (r) => admin(r.id),
  },
  teams: {
    read: (r) => member(r.organizationId),
    insert: (r) => admin(r.organizationId),
    update: (r) => admin(r.organizationId),
    delete: (r) => admin(r.organizationId),
  },
  catalogues: {
    read: (r) => member(r.organizationId),
    insert: (r) => admin(r.organizationId),
    update: (r) => admin(r.organizationId),
    delete: (r) => admin(r.organizationId),
  },
  artists: {
    read: (r) => member(r.organizationId),
    insert: (r) => editor(r.organizationId),
    update: (r) => editor(r.organizationId),
    delete: (r) => admin(r.organizationId),
  },
  memberships: {
    read: (r) => member(r.organizationId),
    insert: (r) => allOf(admin(r.organizationId), personMatches(r), { role: invitableRoles }),
    updateIf: (r) => admin(r.organizationId),
    updateTo: (r) => allOf(admin(r.organizationId), personMatches(r), { role: roles }),
    delete: (r) => admin(r.organizationId),
  },
  releases: {
    read: (r) => member(r.organizationId),
    insert: (r) => allOf(editor(r.organizationId), releaseRefsInTenant(r)),
    updateIf: (r) => editor(r.organizationId),
    updateTo: (r) => allOf(editor(r.organizationId), releaseRefsInTenant(r)),
    delete: (r) => admin(r.organizationId),
  },
  teamAssignments: {
    read: (r) => allowedTo.read(r.team),
    insert: (r) => allOf(allowedTo.insert(r.team), inTenant(r.organizationId, r.team, r.membership)),
    updateIf: (r) => allowedTo.update(r.team),
    updateTo: (r) =>
      allOf(allowedTo.update(r.team), inTenant(r.organizationId, r.team, r.membership)),
    delete: (r) => allowedTo.delete(r.team),
  },
  releaseAssignments: {
    read: (r) => allowedTo.read(r.release),
    insert: (r) =>
      allOf(admin(r.organizationId), inTenant(r.organizationId, r.release, r.membership)),
    updateIf: (r) => admin(r.organizationId),
    updateTo: (r) =>
      allOf(admin(r.organizationId), inTenant(r.organizationId, r.release, r.membership)),
    delete: (r) => admin(r.organizationId),
  },
  releaseTeams: {
    read: (r) => member(r.organizationId),
    insert: (r) => allOf(editor(r.organizationId), inTenant(r.organizationId, r.release, r.team)),
    delete: (r) => editor(r.organizationId),
  },
});
