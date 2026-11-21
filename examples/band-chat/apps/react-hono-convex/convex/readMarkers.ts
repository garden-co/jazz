import { v } from "convex/values";
import { mutation, query } from "./_generated/server";
import { requireAuthor, requireMember } from "./lib/access";

/** Your private read markers (one per room). */
export const mine = query({
  args: {},
  handler: async (ctx) => {
    const reader = await requireAuthor(ctx);
    return ctx.db
      .query("readMarkers")
      .withIndex("by_reader_room", (q) => q.eq("reader", reader))
      .collect();
  },
});

/** Moves your marker in a room you belong to. */
export const mark = mutation({
  args: { roomId: v.id("rooms"), lastReadAt: v.number() },
  handler: async (ctx, { roomId, lastReadAt }) => {
    const reader = await requireAuthor(ctx);
    await requireMember(ctx, roomId, reader);
    const marker = await ctx.db
      .query("readMarkers")
      .withIndex("by_reader_room", (q) => q.eq("reader", reader).eq("roomId", roomId))
      .first();
    if (marker) await ctx.db.patch(marker._id, { lastReadAt });
    else await ctx.db.insert("readMarkers", { roomId, reader, lastReadAt });
  },
});
