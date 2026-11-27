import { v } from "convex/values";
import { mutation, query } from "./_generated/server";
import { denied, membership, requireAuthor, requireCreator, requireReadableRoom } from "./lib/access";

/** Your memberships, behind the room list. */
export const mine = query({
  args: {},
  handler: async (ctx) => {
    const author = await requireAuthor(ctx);
    return ctx.db
      .query("roomMembers")
      .withIndex("by_author", (q) => q.eq("memberAuthor", author))
      .collect();
  },
});

/** A room's members, for anyone who can read the room. */
export const byRoom = query({
  args: { roomId: v.id("rooms") },
  handler: async (ctx, { roomId }) => {
    await requireReadableRoom(ctx, roomId, await requireAuthor(ctx));
    return ctx.db
      .query("roomMembers")
      .withIndex("by_room_author", (q) => q.eq("roomId", roomId))
      .collect();
  },
});

/**
 * Only the room creator admits members: the creator themselves, or someone
 * who asked to join, always with that account's own profile. Admission
 * clears their requests in the same transaction.
 */
export const admit = mutation({
  args: { roomId: v.id("rooms"), memberAuthor: v.id("users"), memberProfileId: v.id("profiles") },
  handler: async (ctx, { roomId, memberAuthor, memberProfileId }) => {
    const author = await requireAuthor(ctx);
    await requireCreator(ctx, roomId, author);
    const profile = await ctx.db.get(memberProfileId);
    if (profile?.author !== memberAuthor) throw denied();
    const requests = await ctx.db
      .query("joinRequests")
      .withIndex("by_room_requester", (q) => q.eq("roomId", roomId).eq("requester", memberAuthor))
      .collect();
    if (memberAuthor !== author && requests.length === 0) throw denied();
    for (const request of requests) await ctx.db.delete(request._id);
    if (await membership(ctx, roomId, memberAuthor)) return;
    await ctx.db.insert("roomMembers", { roomId, memberAuthor, memberProfileId });
  },
});

/**
 * The creator removes members; a member may also leave on their own, which
 * drops their private read markers in the same transaction.
 */
export const remove = mutation({
  args: { memberId: v.id("roomMembers") },
  handler: async (ctx, { memberId }) => {
    const author = await requireAuthor(ctx);
    const member = await ctx.db.get(memberId);
    if (!member) return;
    if (member.memberAuthor === author) {
      const markers = await ctx.db
        .query("readMarkers")
        .withIndex("by_reader_room", (q) => q.eq("reader", author).eq("roomId", member.roomId))
        .collect();
      for (const marker of markers) await ctx.db.delete(marker._id);
    } else {
      await requireCreator(ctx, member.roomId, author);
    }
    await ctx.db.delete(memberId);
  },
});
