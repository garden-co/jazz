import { v } from "convex/values";
import { mutation, query } from "./_generated/server";
import { denied, requireAuthor, requireMember, requireMessageInMyRoom } from "./lib/access";

/** A room's reactions, for its members. */
export const byRoom = query({
  args: { roomId: v.id("rooms") },
  handler: async (ctx, { roomId }) => {
    await requireMember(ctx, roomId, await requireAuthor(ctx));
    return ctx.db
      .query("reactions")
      .withIndex("by_room", (q) => q.eq("roomId", roomId))
      .collect();
  },
});

/** Members react as themselves; the room comes from the message, not the caller. */
export const add = mutation({
  args: { messageId: v.id("messages"), emoji: v.string() },
  handler: async (ctx, { messageId, emoji }) => {
    const author = await requireAuthor(ctx);
    const message = await requireMessageInMyRoom(ctx, messageId, author);
    await ctx.db.insert("reactions", { roomId: message.roomId, messageId, author, emoji });
  },
});

export const remove = mutation({
  args: { reactionId: v.id("reactions") },
  handler: async (ctx, { reactionId }) => {
    const author = await requireAuthor(ctx);
    const reaction = await ctx.db.get(reactionId);
    if (!reaction || reaction.author !== author) throw denied();
    await requireMember(ctx, reaction.roomId, author);
    await ctx.db.delete(reactionId);
  },
});
