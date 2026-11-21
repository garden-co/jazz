import type {
  Artist,
  Catalogue,
  Membership,
  Organization,
  Person,
  Release,
  ReleaseTeam,
  Team,
  TeamAssignment,
} from "../schema";

/**
 * The live queries the UI subscribes to, by name. The server builds the SQL
 * for each (src/lib/live-queries.server.ts), scoped to the signed-in user, and
 * seals it; the browser only names the query and its arguments.
 */
export type QueryDef<Row, Result> = { name: string; args: Record<string, unknown>; one: boolean; _?: [Row, Result] };
const many =
  <Row, Args extends Record<string, unknown> = Record<string, never>>(name: string) =>
  (args?: Args): QueryDef<Row, Row[]> => ({ name, args: args ?? {}, one: false });
const one =
  <Row, Args extends Record<string, unknown> = Record<string, never>>(name: string) =>
  (args?: Args): QueryDef<Row, Row | undefined> => ({ name, args: args ?? {}, one: true });

type InLabel = { organizationId: string };
type Ref = { id: string; name: string };
export type Page = { search?: string; status?: string | null; sort: string; direction: "asc" | "desc"; limit: number; offset: number };

export type ReleaseWithArtist = Release & { artist: Ref | null };
export type ReleaseWithRefs = Release & { artist: Ref | null; catalogue: Ref | null };
export type MemberWithPerson = Membership & { person: Pick<Person, "id" | "name"> | null };
/** Bounded counts (at most `countLimit` rows each), as the Jazz app reads them. */
export type LabelCounts = { artists: number; releases: number; scheduled: number; members: number; teams: number };

export const queries = {
  memberships: {
    /** Every label the signed-in account belongs to, with the label's name. */
    mine: many<Membership & { organization: Pick<Organization, "id" | "name"> | null }>("memberships.mine"),
    byLabel: many<MemberWithPerson, InLabel>("memberships.byLabel"),
  },
  labels: { counts: one<LabelCounts, InLabel>("labels.counts") },
  artists: {
    page: many<Artist, InLabel & Page>("artists.page"),
    /** The release form's artist picker, by name. */
    options: many<Artist, InLabel>("artists.options"),
    one: one<Artist, InLabel & { id: string }>("artists.one"),
  },
  releases: {
    latest: many<ReleaseWithArtist, InLabel>("releases.latest"),
    /** One page of a label's (or one catalogue's) releases: search, status, sort. */
    page: many<ReleaseWithArtist, InLabel & Page & { catalogueId?: string }>("releases.page"),
    byArtist: many<Release & { catalogue: Ref | null }, InLabel & { artistId: string }>("releases.byArtist"),
    one: one<ReleaseWithRefs, InLabel & { id: string }>("releases.one"),
  },
  catalogues: {
    all: many<Catalogue, InLabel>("catalogues.all"),
    one: one<Catalogue, InLabel & { id: string }>("catalogues.one"),
  },
  teams: {
    all: many<Team, InLabel>("teams.all"),
    one: one<Team, InLabel & { id: string }>("teams.one"),
  },
  teamAssignments: {
    byLabel: many<Pick<TeamAssignment, "id" | "teamId">, InLabel>("teamAssignments.byLabel"),
    byTeam: many<TeamAssignment & { membership: { person: { name: string } | null } | null }, InLabel & { teamId: string }>(
      "teamAssignments.byTeam",
    ),
  },
  releaseTeams: {
    byLabel: many<Pick<ReleaseTeam, "id" | "teamId">, InLabel>("releaseTeams.byLabel"),
    byRelease: many<ReleaseTeam & { team: Ref | null }, InLabel & { releaseId: string }>("releaseTeams.byRelease"),
    byTeam: many<ReleaseTeam & { release: Release | null }, InLabel & { teamId: string }>("releaseTeams.byTeam"),
  },
};
