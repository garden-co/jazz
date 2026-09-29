import { schema as s } from "jazz-tools";
import { schema as betterAuthSchema } from "./schema-better-auth/schema";

const schema = {
  ...betterAuthSchema,
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
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
