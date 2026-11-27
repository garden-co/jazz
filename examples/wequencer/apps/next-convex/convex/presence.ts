import { v } from "convex/values";
import { mutation, query } from "./_generated/server";
import { requireAuthor, requireMember } from "./lib/access";

/** A session's presence rows with their profiles: how bandmates learn each other's names. */
export const bySession = query({
  args: { sessionId: v.id("sessions") },
  handler: async (ctx, { sessionId }) => {
    await requireMember(ctx, sessionId, await requireAuthor(ctx));
    const rows = await ctx.db
      .query("presence")
      .withIndex("by_session_profile", (q) => q.eq("session_id", sessionId))
      .collect();
    return Promise.all(rows.map(async (row) => ({ ...row, profile: await ctx.db.get(row.profile_id) })));
  },
});

/** Members publish their own presence (advisory: it never authorizes a write). */
export const heartbeat = mutation({
  args: { sessionId: v.id("sessions") },
  handler: async (ctx, { sessionId }) => {
    const author = await requireAuthor(ctx);
    await requireMember(ctx, sessionId, author);
    const profile = await ctx.db
      .query("profiles")
      .withIndex("by_author", (q) => q.eq("author", author))
      .first();
    if (!profile) return;
    const own = await ctx.db
      .query("presence")
      .withIndex("by_session_profile", (q) => q.eq("session_id", sessionId).eq("profile_id", profile._id))
      .first();
    const heartbeat_at = Date.now();
    if (own) await ctx.db.patch(own._id, { heartbeat_at });
    else await ctx.db.insert("presence", { session_id: sessionId, profile_id: profile._id, cursor_step: 0, heartbeat_at });
  },
});
