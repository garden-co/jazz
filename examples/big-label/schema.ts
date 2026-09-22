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
    },
  ),
  people: s.table(
    { userId: s.uuid(), name: s.string() },
    { membershipsViaPerson: s.reverse("memberships", "person") },
  ),
  teams: s.table(
    { organizationId: s.uuid(), name: s.string() },
    {
      organization: s.rel("organizations", "organizationId"),
      teamAssignmentsViaTeam: s.reverse("teamAssignments", "team"),
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
    },
    {
      organization: s.rel("organizations", "organizationId"),
      releasesViaArtist: s.reverse("releases", "artist"),
    },
  ),
  releases: s.table(
    {
      organizationId: s.uuid(),
      artistId: s.uuid(),
      title: s.string(),
      releaseDate: s.timestamp(),
      status: s.string(),
    },
    {
      organization: s.rel("organizations", "organizationId"),
      artist: s.rel("artists", "artistId"),
      releaseAssignmentsViaRelease: s.reverse("releaseAssignments", "release"),
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
