import { v } from "convex/values";
import { mutation, query } from "./_generated/server";
import { requireAccount, requireMe, requireTask } from "./lib/access";
import { logActivity } from "./lib/stage";

/** Comments follow their task, oldest first, with their authors. */
export const list = query({
  args: { taskId: v.id("tasks") },
  handler: async (ctx, { taskId }) => {
    await requireTask(ctx, await requireAccount(ctx), taskId);
    const comments = await ctx.db
      .query("comments")
      .withIndex("by_task", (q) => q.eq("taskId", taskId))
      .collect();
    return Promise.all(
      comments.map(async (comment) => ({ ...comment, author: await ctx.db.get(comment.authorId) })),
    );
  },
});

export const add = mutation({
  args: { taskId: v.id("tasks"), body: v.string() },
  handler: async (ctx, { taskId, body }) => {
    const me = await requireMe(ctx);
    const { task } = await requireTask(ctx, me.account, taskId);
    await ctx.db.insert("comments", { taskId, authorId: me.profile._id, body });
    await logActivity(ctx, me, task, "commented");
  },
});
