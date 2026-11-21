import { defineQueries, defineQuery, escapeLike } from "@rocicorp/zero";
import { z } from "zod";
import { zql, type AuthData } from "./schema";

/**
 * Who can read what in BigLabel (writes are checked in mutators.ts):
 *
 * - A label and everything in it (memberships, teams, artists, catalogues,
 *   releases and their assignments) is visible to the label's members only;
 *   foreign labels read as empty.
 * - People are visible to the members of their labels, through the
 *   memberships that name them; sign-in emails are never synced.
 *
 * zero-cache resolves each query on the server with the caller's context, so
 * these filters are enforced there.
 */
const me = (ctx: AuthData | undefined) => ctx?.userID ?? "";
// Applied to a row's `tenantMembers` (its label's memberships): the caller is one.
const caller =
  (ctx: AuthData | undefined) =>
  <Q extends { where(column: "userId", value: string): Q }>(members: Q) =>
    members.where("userId", me(ctx));

const label = z.object({ organizationId: z.string() });
const one = label.extend({ id: z.string() });
/** Upper bound for the overview's and teams page's counts (no aggregate query). */
export const COUNT_LIMIT = 5000;

/**
 * One page of a searchable, sortable list: `limit` rows (a page plus one, so
 * the pager knows whether there is a next page), after the `start` row.
 * ZQL pages with a cursor, not an offset (see NOTES.md).
 */
const list = <S extends readonly [string, ...string[]]>(sorts: S) =>
  label.extend({
    search: z.string(),
    status: z.string().nullable(),
    sort: z.enum(sorts),
    direction: z.enum(["asc", "desc"]),
    start: z.object({ id: z.string(), value: z.union([z.string(), z.number(), z.null()]) }).nullable(),
    limit: z.number().int().max(101),
  });
const releaseSorts = ["title", "catalogNumber", "releaseDate", "status"] as const;
const artistSorts = ["name", "genre", "status"] as const;
const like = (search: string) => `%${escapeLike(search.trim().toLowerCase())}%`;

export const queries = defineQueries({
  memberships: {
    /** Every label this account belongs to: an indexed lookup on the denormalized userId. */
    mine: defineQuery(({ ctx }) =>
      zql.memberships.where("userId", me(ctx)).related("organization").limit(100),
    ),
    ofLabel: defineQuery(label.extend({ limit: z.number().int().max(COUNT_LIMIT) }), ({ ctx, args }) =>
      zql.memberships
        .where("organizationId", args.organizationId)
        .whereExists("tenantMembers", caller(ctx))
        .related("person")
        .limit(args.limit),
    ),
  },
  teams: {
    ofLabel: defineQuery(label, ({ ctx, args }) =>
      zql.teams
        .where("organizationId", args.organizationId)
        .whereExists("tenantMembers", caller(ctx))
        .orderBy("name", "asc")
        .limit(COUNT_LIMIT),
    ),
    one: defineQuery(one, ({ ctx, args }) =>
      zql.teams
        .where("organizationId", args.organizationId)
        .where("id", args.id)
        .whereExists("tenantMembers", caller(ctx))
        .one(),
    ),
  },
  teamAssignments: {
    ofLabel: defineQuery(label.extend({ limit: z.number().int().max(COUNT_LIMIT) }), ({ ctx, args }) =>
      zql.teamAssignments
        .where("organizationId", args.organizationId)
        .whereExists("tenantMembers", caller(ctx))
        .limit(args.limit),
    ),
    ofTeam: defineQuery(label.extend({ teamId: z.string() }), ({ ctx, args }) =>
      zql.teamAssignments
        .where("organizationId", args.organizationId)
        .where("teamId", args.teamId)
        .whereExists("tenantMembers", caller(ctx))
        .related("membership", (m) => m.related("person")),
    ),
  },
  artists: {
    page: defineQuery(list(artistSorts), ({ ctx, args }) => {
      let q = zql.artists.where("organizationId", args.organizationId).whereExists("tenantMembers", caller(ctx));
      if (args.search.trim()) q = q.where("searchKey", "LIKE", like(args.search));
      if (args.status) q = q.where("status", args.status);
      q = q.orderBy(args.sort, args.direction).orderBy("id", "asc");
      if (args.start) q = q.start({ id: args.start.id, [args.sort]: args.start.value } as never);
      return q.limit(args.limit);
    }),
    /** For pickers and counts: by name, bounded. */
    ofLabel: defineQuery(label.extend({ limit: z.number().int().max(COUNT_LIMIT) }), ({ ctx, args }) =>
      zql.artists
        .where("organizationId", args.organizationId)
        .whereExists("tenantMembers", caller(ctx))
        .orderBy("name", "asc")
        .limit(args.limit),
    ),
    one: defineQuery(one, ({ ctx, args }) =>
      zql.artists
        .where("organizationId", args.organizationId)
        .where("id", args.id)
        .whereExists("tenantMembers", caller(ctx))
        .one(),
    ),
  },
  catalogues: {
    ofLabel: defineQuery(label, ({ ctx, args }) =>
      zql.catalogues
        .where("organizationId", args.organizationId)
        .whereExists("tenantMembers", caller(ctx))
        .orderBy("name", "asc"),
    ),
    one: defineQuery(one, ({ ctx, args }) =>
      zql.catalogues
        .where("organizationId", args.organizationId)
        .where("id", args.id)
        .whereExists("tenantMembers", caller(ctx))
        .one(),
    ),
  },
  releases: {
    /** The Releases table (and a catalogue's page with `catalogueId`), with artists. */
    page: defineQuery(list(releaseSorts).extend({ catalogueId: z.string().optional() }), ({ ctx, args }) => {
      let q = zql.releases.where("organizationId", args.organizationId).whereExists("tenantMembers", caller(ctx));
      if (args.catalogueId) q = q.where("catalogueId", args.catalogueId);
      if (args.search.trim()) q = q.where("searchKey", "LIKE", like(args.search));
      if (args.status) q = q.where("status", args.status);
      q = q.orderBy(args.sort, args.direction).orderBy("id", "asc");
      if (args.start) q = q.start({ id: args.start.id, [args.sort]: args.start.value } as never);
      return q.related("artist").limit(args.limit);
    }),
    /** The benchmark's label_load: the label's releases, newest first. */
    latest: defineQuery(label, ({ ctx, args }) =>
      zql.releases
        .where("organizationId", args.organizationId)
        .whereExists("tenantMembers", caller(ctx))
        .orderBy("releaseDate", "desc")
        .orderBy("id", "asc")
        .related("artist")
        .limit(8),
    ),
    /** Bounded read for the overview's counts. */
    ofLabel: defineQuery(label, ({ ctx, args }) =>
      zql.releases
        .where("organizationId", args.organizationId)
        .whereExists("tenantMembers", caller(ctx))
        .limit(COUNT_LIMIT),
    ),
    /** The benchmark's artist_load: one artist's releases, newest first. */
    ofArtist: defineQuery(label.extend({ artistId: z.string() }), ({ ctx, args }) =>
      zql.releases
        .where("organizationId", args.organizationId)
        .where("artistId", args.artistId)
        .whereExists("tenantMembers", caller(ctx))
        .orderBy("releaseDate", "desc")
        .orderBy("id", "asc")
        .related("catalogue")
        .limit(100),
    ),
    one: defineQuery(one, ({ ctx, args }) =>
      zql.releases
        .where("organizationId", args.organizationId)
        .where("id", args.id)
        .whereExists("tenantMembers", caller(ctx))
        .related("artist")
        .related("catalogue")
        .one(),
    ),
    /** Who holds a catalogue number (the release form's instant check). */
    withNumber: defineQuery(label.extend({ catalogNumber: z.string() }), ({ ctx, args }) =>
      zql.releases
        .where("organizationId", args.organizationId)
        .where("catalogNumber", args.catalogNumber)
        .whereExists("tenantMembers", caller(ctx)),
    ),
    /** The highest-numbered release in a catalogue, by sequence and by text. */
    lastInCatalogue: defineQuery(
      label.extend({ catalogueId: z.string(), by: z.enum(["catalogSequence", "catalogNumber"]) }),
      ({ ctx, args }) => {
        let q = zql.releases
          .where("organizationId", args.organizationId)
          .where("catalogueId", args.catalogueId)
          .whereExists("tenantMembers", caller(ctx));
        if (args.by === "catalogSequence") q = q.where("catalogSequence", ">=", 0);
        return q.orderBy(args.by, "desc").limit(1);
      },
    ),
  },
  releaseTeams: {
    ofLabel: defineQuery(label.extend({ limit: z.number().int().max(COUNT_LIMIT) }), ({ ctx, args }) =>
      zql.releaseTeams
        .where("organizationId", args.organizationId)
        .whereExists("tenantMembers", caller(ctx))
        .limit(args.limit),
    ),
    ofRelease: defineQuery(label.extend({ releaseId: z.string() }), ({ ctx, args }) =>
      zql.releaseTeams
        .where("organizationId", args.organizationId)
        .where("releaseId", args.releaseId)
        .whereExists("tenantMembers", caller(ctx))
        .related("team"),
    ),
    ofTeam: defineQuery(label.extend({ teamId: z.string() }), ({ ctx, args }) =>
      zql.releaseTeams
        .where("organizationId", args.organizationId)
        .where("teamId", args.teamId)
        .whereExists("tenantMembers", caller(ctx))
        .related("release")
        .limit(200),
    ),
  },
});
