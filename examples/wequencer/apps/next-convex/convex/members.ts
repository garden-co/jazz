import { ConvexError, v } from "convex/values";
import { mutation, query } from "./_generated/server";
import { role } from "./schema";
import { readableSession, requireAuthor, requireCreator, denied } from "./lib/access";

/** Everyone who can read the session sees its members. */
export const bySession = query({
  args: { sessionId: v.id("sessions") },
  handler: async (ctx, { sessionId }) => {
    if (!(await readableSession(ctx, sessionId, await requireAuthor(ctx)))) throw denied();
    return ctx.db
      .query("session_members")
      .withIndex("by_session_author", (q) => q.eq("session_id", sessionId))
      .collect();
  },
});

/** Only the session's creator adds members, by account ID. */
export const add = mutation({
  args: { sessionId: v.id("sessions"), memberAuthor: v.string(), role },
  handler: async (ctx, { sessionId, memberAuthor, role }) => {
    await requireCreator(ctx, sessionId, await requireAuthor(ctx));
    const member = ctx.db.normalizeId("users", memberAuthor);
    if (!member || !(await ctx.db.get(member))) throw new ConvexError("unknown_account");
    await ctx.db.insert("session_members", { session_id: sessionId, member_author: member, role });
  },
});

/** Only the session's creator removes members. */
export const remove = mutation({
  args: { memberId: v.id("session_members") },
  handler: async (ctx, { memberId }) => {
    const author = await requireAuthor(ctx);
    const member = await ctx.db.get(memberId);
    if (!member) return;
    await requireCreator(ctx, member.session_id, author);
    await ctx.db.delete(memberId);
  },
});
