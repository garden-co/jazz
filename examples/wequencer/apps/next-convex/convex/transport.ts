import { v } from "convex/values";
import { mutation, query } from "./_generated/server";
import { requireAuthor, requireEditor, requireMember, requirePatternOf } from "./lib/access";

/** The newest transport observation of a session. */
export const latest = query({
  args: { sessionId: v.id("sessions") },
  handler: async (ctx, { sessionId }) => {
    await requireMember(ctx, sessionId, await requireAuthor(ctx));
    return ctx.db
      .query("transport_observations")
      .withIndex("by_session_observed", (q) => q.eq("session_id", sessionId))
      .order("desc")
      .first();
  },
});

/** Editors append an observation; its pattern must be this session's. */
export const observe = mutation({
  args: {
    sessionId: v.id("sessions"),
    playing: v.boolean(),
    bar: v.number(),
    observed_at: v.number(),
    tempo_bpm: v.number(),
    pattern_id: v.optional(v.id("patterns")),
  },
  handler: async (ctx, { sessionId, ...observation }) => {
    await requireEditor(ctx, sessionId, await requireAuthor(ctx));
    if (observation.pattern_id) await requirePatternOf(ctx, observation.pattern_id, sessionId);
    await ctx.db.insert("transport_observations", { session_id: sessionId, ...observation });
  },
});
