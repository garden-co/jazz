import "server-only";
import { rawSql, type RawSqlParameter } from "@neon/realtime/server";
import { z } from "zod";

/**
 * Who can read what in BigLabel (writes are checked in mutators.server.ts),
 * the README's rules:
 *
 * - A label and everything in it (members, teams, artists, catalogues,
 *   releases, assignments) is visible to the label's members.
 * - People are visible only through the memberships of labels you belong to,
 *   so you see your co-members and nobody else.
 * - Sign-in emails (person_emails) are never read by a live query.
 *
 * Realtime authorizes the query, not the connection: each query is built for
 * the signed-in user ($1) and sealed. The membership check is part of the
 * live query, so it stays live: a removed member stops seeing the label.
 */
const isMember = (organizationId: string) =>
  `EXISTS (SELECT 1 FROM memberships me WHERE me.organization_id = ${organizationId} AND me.user_id = $1)`;

/** Upper bound for the overview's counts, as the Jazz app's bounded reads. */
const COUNT_LIMIT = 5000;
/** Upper bound for the Teams page's per-team counts, as in the Jazz app. */
const TEAM_COUNT_LIMIT = 2000;

const RELEASE = (r: string) =>
  `${r}.id, ${r}.organization_id AS "organizationId", ${r}.artist_id AS "artistId", ${r}.catalogue_id AS "catalogueId",
   ${r}.catalog_number AS "catalogNumber", ${r}.catalog_sequence AS "catalogSequence", ${r}.title, ${r}.format,
   ${r}.release_date AS "releaseDate", ${r}.status, ${r}.search_key AS "searchKey"`;
const RELEASE_JSON = (r: string) =>
  `jsonb_build_object('id', ${r}.id, 'organizationId', ${r}.organization_id, 'artistId', ${r}.artist_id,
     'catalogueId', ${r}.catalogue_id, 'catalogNumber', ${r}.catalog_number, 'catalogSequence', ${r}.catalog_sequence,
     'title', ${r}.title, 'format', ${r}.format, 'releaseDate', ${r}.release_date, 'status', ${r}.status,
     'searchKey', ${r}.search_key)`;
const ARTIST = `a.id, a.organization_id AS "organizationId", a.name, a.genre, a.status, a.search_key AS "searchKey"`;
const REF = (t: string) => `CASE WHEN ${t}.id IS NULL THEN NULL ELSE jsonb_build_object('id', ${t}.id, 'name', ${t}.name) END`;
const MEMBERSHIP = (m: string) =>
  `${m}.id, ${m}.organization_id AS "organizationId", ${m}.person_id AS "personId", ${m}.user_id AS "userId", ${m}.role`;

const inLabel = z.object({ organizationId: z.string() });
const byId = inLabel.extend({ id: z.string() });
const page = <S extends Record<string, string>>(sorts: S) =>
  inLabel.extend({
    search: z.string().optional(),
    status: z.string().nullable().optional(),
    sort: z.enum(Object.keys(sorts) as [keyof S & string, ...(keyof S & string)[]]),
    direction: z.enum(["asc", "desc"]),
    limit: z.number().int().min(1).max(101),
    offset: z.number().int().min(0),
  });
const releaseSorts = { title: "r.title", catalogNumber: "r.catalog_number", releaseDate: "r.release_date", status: "r.status" };
const artistSorts = { name: "a.name", genre: "a.genre", status: "a.status" };

/**
 * A page of rows: optional search (`contains` on the lower-cased search key,
 * as `strpos`, which matches literally) and status filters, then
 * `ORDER BY <sort>, id LIMIT n OFFSET m`, the Jazz app's query shape.
 */
function pageClauses(
  t: string,
  args: { search?: string; status?: string | null; sort: string; direction: "asc" | "desc"; limit: number; offset: number },
  sorts: Record<string, string>,
  params: RawSqlParameter[],
) {
  const where: string[] = [];
  const search = args.search?.trim().toLowerCase();
  if (search) where.push(`strpos(${t}.search_key, $${params.push(search)}) > 0`);
  if (args.status) where.push(`${t}.status = $${params.push(args.status)}`);
  const order = `ORDER BY ${sorts[args.sort]} ${args.direction === "desc" ? "DESC" : "ASC"}, ${t}.id ASC
    LIMIT $${params.push(args.limit)} OFFSET $${params.push(args.offset)}`;
  return { where: where.map((clause) => ` AND ${clause}`).join(""), order };
}

export const liveQueries: Record<string, (me: string, args: unknown) => ReturnType<typeof rawSql>> = {
  "memberships.mine": (me) =>
    rawSql(
      `SELECT ${MEMBERSHIP("m")}, jsonb_build_object('id', o.id, 'name', o.name) AS organization
       FROM memberships m JOIN organizations o ON o.id = m.organization_id
       WHERE m.user_id = $1 ORDER BY m.id LIMIT 100`,
      [me],
    ),

  "memberships.byLabel": (me, args) => {
    const { organizationId } = inLabel.parse(args);
    return rawSql(
      `SELECT ${MEMBERSHIP("m")}, jsonb_build_object('id', p.id, 'name', p.name) AS person
       FROM memberships m JOIN people p ON p.id = m.person_id
       WHERE m.organization_id = $2 AND ${isMember("$2")} ORDER BY m.id LIMIT 500`,
      [me, organizationId],
    );
  },

  /** The overview's counts: bounded reads (at most 5,000 rows each), counted on the server. */
  "labels.counts": (me, args) => {
    const { organizationId } = inLabel.parse(args);
    const bounded = (table: string, column = "1") =>
      `(SELECT ${column} AS v FROM ${table} WHERE organization_id = $2 LIMIT ${COUNT_LIMIT})`;
    return rawSql(
      `SELECT
         (SELECT count(*)::int FROM ${bounded("artists")} x) AS artists,
         (SELECT count(*)::int FROM ${bounded("releases", "status")} x) AS releases,
         (SELECT (count(*) FILTER (WHERE v = 'scheduled'))::int FROM ${bounded("releases", "status")} x) AS scheduled,
         (SELECT count(*)::int FROM ${bounded("memberships")} x) AS members,
         (SELECT count(*)::int FROM ${bounded("teams")} x) AS teams
       WHERE ${isMember("$2")}`,
      [me, organizationId],
    );
  },

  "artists.page": (me, args) => {
    const parsed = page(artistSorts).parse(args);
    const params: RawSqlParameter[] = [me, parsed.organizationId];
    const { where, order } = pageClauses("a", parsed, artistSorts, params);
    return rawSql(`SELECT ${ARTIST} FROM artists a WHERE a.organization_id = $2 AND ${isMember("$2")}${where} ${order}`, params);
  },

  "artists.options": (me, args) => {
    const { organizationId } = inLabel.parse(args);
    return rawSql(
      `SELECT ${ARTIST} FROM artists a WHERE a.organization_id = $2 AND ${isMember("$2")} ORDER BY a.name, a.id LIMIT 500`,
      [me, organizationId],
    );
  },

  "artists.one": (me, args) => {
    const { organizationId, id } = byId.parse(args);
    return rawSql(`SELECT ${ARTIST} FROM artists a WHERE a.id = $3 AND a.organization_id = $2 AND ${isMember("$2")}`, [
      me,
      organizationId,
      id,
    ]);
  },

  /** The benchmark's label_load: the label's releases, newest first. */
  "releases.latest": (me, args) => {
    const { organizationId } = inLabel.parse(args);
    return rawSql(
      `SELECT ${RELEASE("r")}, ${REF("a")} AS artist
       FROM releases r LEFT JOIN artists a ON a.organization_id = r.organization_id AND a.id = r.artist_id
       WHERE r.organization_id = $2 AND ${isMember("$2")} ORDER BY r.release_date DESC, r.id ASC LIMIT 8`,
      [me, organizationId],
    );
  },

  "releases.page": (me, args) => {
    const parsed = page(releaseSorts).extend({ catalogueId: z.string().optional() }).parse(args);
    const params: RawSqlParameter[] = [me, parsed.organizationId];
    const catalogue = parsed.catalogueId ? ` AND r.catalogue_id = $${params.push(parsed.catalogueId)}` : "";
    const { where, order } = pageClauses("r", parsed, releaseSorts, params);
    return rawSql(
      `SELECT ${RELEASE("r")}, ${REF("a")} AS artist
       FROM releases r LEFT JOIN artists a ON a.organization_id = r.organization_id AND a.id = r.artist_id
       WHERE r.organization_id = $2 AND ${isMember("$2")}${catalogue}${where} ${order}`,
      params,
    );
  },

  /** The benchmark's artist_load: one artist's releases, newest first. */
  "releases.byArtist": (me, args) => {
    const { organizationId, artistId } = inLabel.extend({ artistId: z.string() }).parse(args);
    return rawSql(
      `SELECT ${RELEASE("r")}, ${REF("c")} AS catalogue
       FROM releases r LEFT JOIN catalogues c ON c.organization_id = r.organization_id AND c.id = r.catalogue_id
       WHERE r.organization_id = $2 AND r.artist_id = $3 AND ${isMember("$2")}
       ORDER BY r.release_date DESC, r.id ASC LIMIT 100`,
      [me, organizationId, artistId],
    );
  },

  "releases.one": (me, args) => {
    const { organizationId, id } = byId.parse(args);
    return rawSql(
      `SELECT ${RELEASE("r")}, ${REF("a")} AS artist, ${REF("c")} AS catalogue
       FROM releases r
       LEFT JOIN artists a ON a.organization_id = r.organization_id AND a.id = r.artist_id
       LEFT JOIN catalogues c ON c.organization_id = r.organization_id AND c.id = r.catalogue_id
       WHERE r.id = $3 AND r.organization_id = $2 AND ${isMember("$2")}`,
      [me, organizationId, id],
    );
  },

  "catalogues.all": (me, args) => {
    const { organizationId } = inLabel.parse(args);
    return rawSql(
      `SELECT c.id, c.organization_id AS "organizationId", c.name, c.code FROM catalogues c
       WHERE c.organization_id = $2 AND ${isMember("$2")} ORDER BY c.name, c.id`,
      [me, organizationId],
    );
  },

  "catalogues.one": (me, args) => {
    const { organizationId, id } = byId.parse(args);
    return rawSql(
      `SELECT c.id, c.organization_id AS "organizationId", c.name, c.code FROM catalogues c
       WHERE c.id = $3 AND c.organization_id = $2 AND ${isMember("$2")}`,
      [me, organizationId, id],
    );
  },

  "teams.all": (me, args) => {
    const { organizationId } = inLabel.parse(args);
    return rawSql(
      `SELECT t.id, t.organization_id AS "organizationId", t.name FROM teams t
       WHERE t.organization_id = $2 AND ${isMember("$2")} ORDER BY t.name, t.id`,
      [me, organizationId],
    );
  },

  "teams.one": (me, args) => {
    const { organizationId, id } = byId.parse(args);
    return rawSql(
      `SELECT t.id, t.organization_id AS "organizationId", t.name FROM teams t
       WHERE t.id = $3 AND t.organization_id = $2 AND ${isMember("$2")}`,
      [me, organizationId, id],
    );
  },

  "teamAssignments.byLabel": (me, args) => {
    const { organizationId } = inLabel.parse(args);
    return rawSql(
      `SELECT ta.id, ta.team_id AS "teamId" FROM team_assignments ta
       WHERE ta.organization_id = $2 AND ${isMember("$2")} ORDER BY ta.id LIMIT ${TEAM_COUNT_LIMIT}`,
      [me, organizationId],
    );
  },

  "teamAssignments.byTeam": (me, args) => {
    const { organizationId, teamId } = inLabel.extend({ teamId: z.string() }).parse(args);
    return rawSql(
      `SELECT ta.id, ta.organization_id AS "organizationId", ta.team_id AS "teamId", ta.membership_id AS "membershipId", ta.role,
         jsonb_build_object('person', CASE WHEN p.id IS NULL THEN NULL ELSE jsonb_build_object('name', p.name) END) AS membership
       FROM team_assignments ta
       JOIN memberships m ON m.organization_id = ta.organization_id AND m.id = ta.membership_id
       LEFT JOIN people p ON p.id = m.person_id
       WHERE ta.organization_id = $2 AND ta.team_id = $3 AND ${isMember("$2")} ORDER BY ta.id`,
      [me, organizationId, teamId],
    );
  },

  "releaseTeams.byLabel": (me, args) => {
    const { organizationId } = inLabel.parse(args);
    return rawSql(
      `SELECT rt.id, rt.team_id AS "teamId" FROM release_teams rt
       WHERE rt.organization_id = $2 AND ${isMember("$2")} ORDER BY rt.id LIMIT ${TEAM_COUNT_LIMIT}`,
      [me, organizationId],
    );
  },

  "releaseTeams.byRelease": (me, args) => {
    const { organizationId, releaseId } = inLabel.extend({ releaseId: z.string() }).parse(args);
    return rawSql(
      `SELECT rt.id, rt.organization_id AS "organizationId", rt.release_id AS "releaseId", rt.team_id AS "teamId", ${REF("t")} AS team
       FROM release_teams rt LEFT JOIN teams t ON t.organization_id = rt.organization_id AND t.id = rt.team_id
       WHERE rt.organization_id = $2 AND rt.release_id = $3 AND ${isMember("$2")} ORDER BY rt.id`,
      [me, organizationId, releaseId],
    );
  },

  "releaseTeams.byTeam": (me, args) => {
    const { organizationId, teamId } = inLabel.extend({ teamId: z.string() }).parse(args);
    return rawSql(
      `SELECT rt.id, rt.organization_id AS "organizationId", rt.release_id AS "releaseId", rt.team_id AS "teamId",
         ${RELEASE_JSON("r")} AS release
       FROM release_teams rt JOIN releases r ON r.organization_id = rt.organization_id AND r.id = rt.release_id
       WHERE rt.organization_id = $2 AND rt.team_id = $3 AND ${isMember("$2")} ORDER BY rt.id LIMIT 200`,
      [me, organizationId, teamId],
    );
  },
};
