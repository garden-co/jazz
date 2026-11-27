import { v } from "convex/values";
import { mutation, query } from "./_generated/server";
import { denied, requireAuthor, requireCreator, requireProfile } from "./lib/access";

/** The requests to join a room, for its creator. */
export const byRoom = query({
  args: { roomId: v.id("rooms") },
  handler: async (ctx, { roomId }) => {
    await requireCreator(ctx, roomId, await requireAuthor(ctx));
    return ctx.db
      .query("joinRequests")
      .withIndex("by_room_requester", (q) => q.eq("roomId", roomId))
      .collect();
  },
});

/** Your own requests to join a room (from a room link). */
export const mine = query({
  args: { roomId: v.string() },
  handler: async (ctx, args) => {
    const author = await requireAuthor(ctx);
    const roomId = ctx.db.normalizeId("rooms", args.roomId);
    if (!roomId) return [];
    return ctx.db
      .query("joinRequests")
      .withIndex("by_room_requester", (q) => q.eq("roomId", roomId).eq("requester", author))
      .collect();
  },
});

/** Anyone holding a room link may ask to join, with their own profile. */
export const create = mutation({
  args: { roomId: v.string() },
  handler: async (ctx, args) => {
    const author = await requireAuthor(ctx);
    const profile = await requireProfile(ctx, author);
    const roomId = ctx.db.normalizeId("rooms", args.roomId);
    if (!roomId) throw denied();
    return ctx.db.insert("joinRequests", { roomId, requester: author, profileId: profile._id });
  },
});

/** The requester cancels, or the creator declines. */
export const remove = mutation({
  args: { requestId: v.id("joinRequests") },
  handler: async (ctx, { requestId }) => {
    const author = await requireAuthor(ctx);
    const request = await ctx.db.get(requestId);
    if (!request) return;
    if (request.requester !== author) await requireCreator(ctx, request.roomId, author);
    await ctx.db.delete(requestId);
  },
});
