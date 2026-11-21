import { ConvexError, v } from "convex/values";
import type { Doc } from "./_generated/dataModel";
import { mutation, query, type QueryCtx } from "./_generated/server";
import { idOf, requireAdmin, requireEditor, requireInLabel, requireMember } from "./lib/access";
import { pageOf } from "./lib/paging";
import { formatCatalogNumber } from "../src/fixtures";

const sortKey = v.union(v.literal("title"), v.literal("catalogNumber"), v.literal("releaseDate"), v.literal("status"));
const direction = v.union(v.literal("asc"), v.literal("desc"));

async function withArtist(ctx: QueryCtx, release: Doc<"releases">) {
  return { ...release, artist: await ctx.db.get(release.artistId) };
}

/**
 * A page of a label's releases (or one catalogue's), sorted by a column,
 * optionally filtered by a lower-cased search term and a status.
 */
export const list = query({
  args: {
    organizationId: v.id("organizations"),
    catalogueId: v.optional(v.id("catalogues")),
    search: v.optional(v.string()),
    status: v.optional(v.string()),
    sortKey,
    direction,
    offset: v.number(),
    limit: v.number(),
  },
  handler: async (ctx, { organizationId, catalogueId, search, status, sortKey, direction, offset, limit }) => {
    await requireMember(ctx, organizationId);
    const ordered = catalogueId
      ? ctx.db
          .query("releases")
          .withIndex(
            sortKey === "releaseDate"
              ? "by_org_catalogue_releaseDate"
              : sortKey === "title"
                ? "by_org_catalogue_title"
                : sortKey === "catalogNumber"
                  ? "by_org_catalogue_catalogNumber"
                  : "by_org_catalogue_status",
            (q) => q.eq("organizationId", organizationId).eq("catalogueId", catalogueId),
          )
      : ctx.db
          .query("releases")
          .withIndex(`by_org_${sortKey}`, (q) => q.eq("organizationId", organizationId));
    const rows = await pageOf(
      ordered.order(direction),
      { offset, limit },
      (row) => (!search || row.searchKey.includes(search)) && (!status || row.status === status),
    );
    return Promise.all(rows.map((row) => withArtist(ctx, row)));
  },
});

/** The label's newest releases (the Overview's "Latest releases"). */
export const latest = query({
  args: { organizationId: v.id("organizations"), limit: v.number() },
  handler: async (ctx, { organizationId, limit }) => {
    await requireMember(ctx, organizationId);
    const rows = await ctx.db
      .query("releases")
      .withIndex("by_org_releaseDate", (q) => q.eq("organizationId", organizationId))
      .order("desc")
      .take(limit);
    return Promise.all(rows.map((row) => withArtist(ctx, row)));
  },
});

/** One artist's releases, newest first, with their catalogues. */
export const byArtist = query({
  args: { organizationId: v.id("organizations"), artistId: v.string() },
  handler: async (ctx, { organizationId, artistId }) => {
    await requireMember(ctx, organizationId);
    const id = idOf(ctx, "artists", artistId);
    if (!id) return [];
    const rows = await ctx.db
      .query("releases")
      .withIndex("by_org_artist_releaseDate", (q) => q.eq("organizationId", organizationId).eq("artistId", id))
      .order("desc")
      .take(100);
    return Promise.all(
      rows.map(async (row) => ({ ...row, catalogue: row.catalogueId ? await ctx.db.get(row.catalogueId) : null })),
    );
  },
});

/** One release with its artist and catalogue; null when it isn't this label's. */
export const get = query({
  args: { organizationId: v.id("organizations"), id: v.string() },
  handler: async (ctx, { organizationId, id }) => {
    await requireMember(ctx, organizationId);
    const releaseId = idOf(ctx, "releases", id);
    const release = releaseId ? await ctx.db.get(releaseId) : null;
    if (!release || release.organizationId !== organizationId) return null;
    return {
      ...release,
      artist: await ctx.db.get(release.artistId),
      catalogue: release.catalogueId ? await ctx.db.get(release.catalogueId) : null,
    };
  },
});

/** The trailing digits of a catalogue number: 13 for "NFC-013". */
function catalogSequence(catalogNumber: string) {
  const digits = catalogNumber.match(/(\d+)$/)?.[1];
  return digits === undefined ? undefined : Number(digits);
}

/** The number after the highest one used in a catalogue, e.g. NFC-013. */
export const nextCatalogNumber = query({
  args: { organizationId: v.id("organizations"), catalogueId: v.id("catalogues") },
  handler: async (ctx, { organizationId, catalogueId }) => {
    await requireMember(ctx, organizationId);
    const catalogue = await requireInLabel(ctx, catalogueId, organizationId);
    const highest = await ctx.db
      .query("releases")
      .withIndex("by_org_catalogue_sequence", (q) =>
        q.eq("organizationId", organizationId).eq("catalogueId", catalogueId).gte("catalogSequence", 0),
      )
      .order("desc")
      .first();
    return formatCatalogNumber(catalogue.code, (highest?.catalogSequence ?? 0) + 1);
  },
});

/**
 * Inserts or updates a release (the Jazz app's saveRelease). Catalogue
 * numbers are unique per label: the mutation reads the number's current
 * holders, and Convex's serializable transactions retry it if a concurrent
 * write changed that read. Editors only; the artist and catalogue must be
 * this label's.
 */
export const save = mutation({
  args: {
    organizationId: v.id("organizations"),
    releaseId: v.optional(v.id("releases")),
    title: v.string(),
    artistId: v.id("artists"),
    catalogueId: v.union(v.id("catalogues"), v.null()),
    catalogNumber: v.string(),
    format: v.string(),
    releaseDate: v.number(),
    status: v.string(),
    searchKey: v.string(),
  },
  handler: async (ctx, { organizationId, releaseId, catalogueId, ...fields }) => {
    await requireEditor(ctx, organizationId);
    await requireInLabel(ctx, fields.artistId, organizationId);
    if (catalogueId) await requireInLabel(ctx, catalogueId, organizationId);
    if (releaseId) await requireInLabel(ctx, releaseId, organizationId);
    const holders = await ctx.db
      .query("releases")
      .withIndex("by_org_catalogNumber", (q) =>
        q.eq("organizationId", organizationId).eq("catalogNumber", fields.catalogNumber),
      )
      .collect();
    if (holders.some((row) => row._id !== releaseId)) {
      throw new ConvexError({ code: "catalog_number_taken", catalogNumber: fields.catalogNumber });
    }
    const row = {
      ...fields,
      catalogueId: catalogueId ?? undefined,
      catalogSequence: catalogSequence(fields.catalogNumber),
    };
    if (releaseId) {
      await ctx.db.patch(releaseId, row);
      return releaseId;
    }
    return ctx.db.insert("releases", { ...row, organizationId });
  },
});

/** Admins delete a release with its team assignments. */
export const remove = mutation({
  args: { organizationId: v.id("organizations"), releaseId: v.id("releases") },
  handler: async (ctx, { organizationId, releaseId }) => {
    await requireAdmin(ctx, organizationId);
    await requireInLabel(ctx, releaseId, organizationId);
    const teams = await ctx.db
      .query("releaseTeams")
      .withIndex("by_org_release", (q) => q.eq("organizationId", organizationId).eq("releaseId", releaseId))
      .collect();
    for (const row of teams) await ctx.db.delete(row._id);
    await ctx.db.delete(releaseId);
  },
});

