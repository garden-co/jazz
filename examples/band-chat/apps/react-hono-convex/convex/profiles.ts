import { v } from "convex/values";
import { mutation, query } from "./_generated/server";
import type { Id } from "./_generated/dataModel";
import { denied, myProfile, requireAuthor } from "./lib/access";

/** Your profile; null before first-run setup. */
export const mine = query({
  args: {},
  handler: async (ctx) => myProfile(ctx, await requireAuthor(ctx)),
});

/**
 * Every profile this account may see: its own, its co-members' and those of
 * people asking to join its rooms. One subscription feeds every name/avatar.
 */
export const directory = query({
  args: {},
  handler: async (ctx) => {
    const author = await requireAuthor(ctx);
    const ids = new Set<Id<"profiles">>();
    const memberships = await ctx.db
      .query("roomMembers")
      .withIndex("by_author", (q) => q.eq("memberAuthor", author))
      .collect();
    for (const { roomId } of memberships) {
      const members = await ctx.db
        .query("roomMembers")
        .withIndex("by_room_author", (q) => q.eq("roomId", roomId))
        .collect();
      for (const member of members) ids.add(member.memberProfileId);
    }
    const created = await ctx.db
      .query("rooms")
      .withIndex("by_creator", (q) => q.eq("creator", author))
      .collect();
    for (const room of created) {
      const requests = await ctx.db
        .query("joinRequests")
        .withIndex("by_room_requester", (q) => q.eq("roomId", room._id))
        .collect();
      for (const request of requests) ids.add(request.profileId);
    }
    const profiles = await Promise.all([...ids].map((id) => ctx.db.get(id)));
    return profiles.filter((profile) => profile !== null);
  },
});

const fields = {
  displayName: v.string(),
  avatar: v.optional(v.bytes()),
  avatarType: v.optional(v.string()),
};

/** First run: every account chooses how it appears before joining rooms. */
export const create = mutation({
  args: fields,
  handler: async (ctx, args) => {
    const author = await requireAuthor(ctx);
    const existing = await myProfile(ctx, author);
    if (existing) return existing._id;
    return ctx.db.insert("profiles", { author, ...args });
  },
});

export const update = mutation({
  args: { profileId: v.id("profiles"), ...fields },
  handler: async (ctx, { profileId, ...args }) => {
    const author = await requireAuthor(ctx);
    const profile = await ctx.db.get(profileId);
    if (profile?.author !== author) throw denied();
    await ctx.db.replace(profileId, { author, ...args });
  },
});
