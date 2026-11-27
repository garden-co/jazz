import { v } from "convex/values";
import { query } from "./_generated/server";
import { requireAccount, requireCrew, requireTask } from "./lib/access";

/** Newest first, with the actor's profile: a show's activity, or a single task's. */
export const list = query({
  args: {
    of: v.union(
      v.object({ showId: v.id("shows") }),
      v.object({ taskId: v.id("tasks") }),
    ),
    limit: v.number(),
  },
  handler: async (ctx, { of, limit }) => {
    const account = await requireAccount(ctx);
    const entries =
      "showId" in of
        ? (await requireCrew(ctx, account, of.showId),
          await ctx.db
            .query("activity")
            .withIndex("by_show", (q) => q.eq("showId", of.showId))
            .order("desc")
            .take(limit))
        : (await requireTask(ctx, account, of.taskId),
          await ctx.db
            .query("activity")
            .withIndex("by_task", (q) => q.eq("taskId", of.taskId))
            .order("desc")
            .take(limit));
    return Promise.all(
      entries.map(async (entry) => ({ ...entry, actor: await ctx.db.get(entry.actorId) })),
    );
  },
});
