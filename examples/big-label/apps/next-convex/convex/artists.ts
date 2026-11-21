import { ConvexError, v } from "convex/values";
import { mutation, query } from "./_generated/server";
import { idOf, requireAdmin, requireEditor, requireInLabel, requireMember } from "./lib/access";
import { pageOf } from "./lib/paging";

/** A page of a label's artists, sorted by a column, with search and a status filter. */
export const list = query({
  args: {
    organizationId: v.id("organizations"),
    search: v.optional(v.string()),
    status: v.optional(v.string()),
    sortKey: v.union(v.literal("name"), v.literal("genre"), v.literal("status")),
    direction: v.union(v.literal("asc"), v.literal("desc")),
    offset: v.number(),
    limit: v.number(),
  },
  handler: async (ctx, { organizationId, search, status, sortKey, direction, offset, limit }) => {
    await requireMember(ctx, organizationId);
    return pageOf(
      ctx.db
        .query("artists")
        .withIndex(`by_org_${sortKey}`, (q) => q.eq("organizationId", organizationId))
        .order(direction),
      { offset, limit },
      (row) => (!search || row.searchKey.includes(search)) && (!status || row.status === status),
    );
  },
});

/** Artists by name, for the release form's picker. */
export const options = query({
  args: { organizationId: v.id("organizations") },
  handler: async (ctx, { organizationId }) => {
    await requireMember(ctx, organizationId);
    return ctx.db
      .query("artists")
      .withIndex("by_org_name", (q) => q.eq("organizationId", organizationId))
      .take(500);
  },
});

export const get = query({
  args: { organizationId: v.id("organizations"), id: v.string() },
  handler: async (ctx, { organizationId, id }) => {
    await requireMember(ctx, organizationId);
    const artistId = idOf(ctx, "artists", id);
    const artist = artistId ? await ctx.db.get(artistId) : null;
    return artist?.organizationId === organizationId ? artist : null;
  },
});

const fields = { name: v.string(), genre: v.string(), status: v.string(), searchKey: v.string() };

/** Editors add and edit artists. */
export const create = mutation({
  args: { organizationId: v.id("organizations"), ...fields },
  handler: async (ctx, { organizationId, ...artist }) => {
    await requireEditor(ctx, organizationId);
    return ctx.db.insert("artists", { organizationId, ...artist });
  },
});

export const update = mutation({
  args: { organizationId: v.id("organizations"), artistId: v.id("artists"), ...fields },
  handler: async (ctx, { organizationId, artistId, ...artist }) => {
    await requireEditor(ctx, organizationId);
    await requireInLabel(ctx, artistId, organizationId);
    await ctx.db.patch(artistId, artist);
  },
});

/** Admins delete an artist that has no releases (read in the same transaction). */
export const remove = mutation({
  args: { organizationId: v.id("organizations"), artistId: v.id("artists") },
  handler: async (ctx, { organizationId, artistId }) => {
    await requireAdmin(ctx, organizationId);
    await requireInLabel(ctx, artistId, organizationId);
    const release = await ctx.db
      .query("releases")
      .withIndex("by_org_artist_releaseDate", (q) => q.eq("organizationId", organizationId).eq("artistId", artistId))
      .first();
    if (release) throw new ConvexError("Delete this artist's releases first");
    await ctx.db.delete(artistId);
  },
});
