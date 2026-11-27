import { v } from "convex/values";
import { mutation, query } from "./_generated/server";
import { denied, requireAuthor, requireEditor, requireMember } from "./lib/access";

export const bySession = query({
  args: { sessionId: v.id("sessions") },
  handler: async (ctx, { sessionId }) => {
    await requireMember(ctx, sessionId, await requireAuthor(ctx));
    return ctx.db
      .query("patterns")
      .withIndex("by_session_position", (q) => q.eq("session_id", sessionId))
      .collect();
  },
});

export const add = mutation({
  args: { sessionId: v.id("sessions"), position: v.number(), length: v.number() },
  handler: async (ctx, { sessionId, position, length }) => {
    await requireEditor(ctx, sessionId, await requireAuthor(ctx));
    return ctx.db.insert("patterns", {
      session_id: sessionId,
      position,
      name: `Pattern ${position + 1}`,
      length,
    });
  },
});

export const setLength = mutation({
  args: { patternId: v.id("patterns"), length: v.number() },
  handler: async (ctx, { patternId, length }) => {
    const pattern = await ctx.db.get(patternId);
    if (!pattern) throw denied();
    await requireEditor(ctx, pattern.session_id, await requireAuthor(ctx));
    await ctx.db.patch(patternId, { length });
  },
});
