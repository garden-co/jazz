import { v } from "convex/values";
import { mutation, query } from "./_generated/server";
import { requireAuthor, requireCreator, requireProfile } from "./lib/access";

/** The rooms you belong to or created. */
export const mine = query({
  args: {},
  handler: async (ctx) => {
    const author = await requireAuthor(ctx);
    const memberships = await ctx.db
      .query("roomMembers")
      .withIndex("by_author", (q) => q.eq("memberAuthor", author))
      .collect();
    const created = await ctx.db
      .query("rooms")
      .withIndex("by_creator", (q) => q.eq("creator", author))
      .collect();
    const ids = new Set([...memberships.map((m) => m.roomId), ...created.map((r) => r._id)]);
    const rooms = await Promise.all([...ids].map((id) => ctx.db.get(id)));
    return rooms.filter((room) => room !== null);
  },
});

/** The room and its creator's membership, in one transaction. */
export const create = mutation({
  args: { name: v.string() },
  handler: async (ctx, { name }) => {
    const author = await requireAuthor(ctx);
    const profile = await requireProfile(ctx, author);
    const roomId = await ctx.db.insert("rooms", { name, creator: author });
    await ctx.db.insert("roomMembers", {
      roomId,
      memberAuthor: author,
      memberProfileId: profile._id,
    });
    return roomId;
  },
});

/** Only the creator renames a room. */
export const rename = mutation({
  args: { roomId: v.id("rooms"), name: v.string() },
  handler: async (ctx, { roomId, name }) => {
    await requireCreator(ctx, roomId, await requireAuthor(ctx));
    await ctx.db.patch(roomId, { name });
  },
});
