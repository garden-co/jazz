import { v } from "convex/values";
import { mutation, query } from "./_generated/server";
import { requireAccount, requireChief } from "./lib/access";
import { newInviteCode } from "./lib/stage";

/** Invite codes are bearer secrets: only the chief reads or rotates them. */
export const list = query({
  args: { showId: v.id("shows") },
  handler: async (ctx, { showId }) => {
    await requireChief(ctx, await requireAccount(ctx), showId);
    return ctx.db
      .query("showInvites")
      .withIndex("by_show_code", (q) => q.eq("showId", showId))
      .collect();
  },
});

/** Replaces the show's invite code; links built from the old code stop working. */
export const rotate = mutation({
  args: { showId: v.id("shows") },
  handler: async (ctx, { showId }) => {
    await requireChief(ctx, await requireAccount(ctx), showId);
    const old = await ctx.db
      .query("showInvites")
      .withIndex("by_show_code", (q) => q.eq("showId", showId))
      .collect();
    for (const invite of old) await ctx.db.delete(invite._id);
    await ctx.db.insert("showInvites", { showId, code: newInviteCode() });
  },
});
