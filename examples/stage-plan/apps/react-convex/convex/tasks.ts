import { v } from "convex/values";
import { mutation, query } from "./_generated/server";
import { taskStatus } from "./schema";
import {
  canDeleteTask,
  denied,
  requireAccount,
  requireAssigneeIsCrew,
  requireCrew,
  requireMe,
  requireTask,
} from "./lib/access";
import { logActivity, STATUS_LABELS } from "./lib/stage";

/** A show's tasks in rank order. */
export const list = query({
  args: { showId: v.id("shows") },
  handler: async (ctx, { showId }) => {
    await requireCrew(ctx, await requireAccount(ctx), showId);
    return ctx.db
      .query("tasks")
      .withIndex("by_show_rank", (q) => q.eq("showId", showId))
      .collect();
  },
});

export const add = mutation({
  args: { showId: v.id("shows"), title: v.string(), status: taskStatus, rank: v.number() },
  handler: async (ctx, { showId, title, status, rank }) => {
    const me = await requireMe(ctx);
    await requireCrew(ctx, me.account, showId);
    const taskId = await ctx.db.insert("tasks", {
      showId,
      title,
      status,
      rank,
      createdBy: me.account,
      updatedAt: Date.now(),
    });
    await logActivity(ctx, me, { _id: taskId, showId }, "created", STATUS_LABELS[status]);
    return taskId;
  },
});

/** Moves a task to a column (and position); logs a move when the column changes. */
export const move = mutation({
  args: { taskId: v.id("tasks"), status: taskStatus, rank: v.number() },
  handler: async (ctx, { taskId, status, rank }) => {
    const me = await requireMe(ctx);
    const { task } = await requireTask(ctx, me.account, taskId);
    await ctx.db.patch(taskId, { status, rank, updatedAt: Date.now() });
    if (status !== task.status) await logActivity(ctx, me, task, "moved", STATUS_LABELS[status]);
  },
});

export const assign = mutation({
  args: { taskId: v.id("tasks"), assigneeId: v.union(v.id("crew"), v.null()) },
  handler: async (ctx, { taskId, assigneeId }) => {
    const me = await requireMe(ctx);
    const { task } = await requireTask(ctx, me.account, taskId);
    await requireAssigneeIsCrew(ctx, task.showId, assigneeId ?? undefined);
    await ctx.db.patch(taskId, { assigneeId: assigneeId ?? undefined, updatedAt: Date.now() });
    const assignee = assigneeId ? await ctx.db.get(assigneeId) : null;
    await logActivity(ctx, me, task, "assigned", assignee?.name ?? "nobody");
  },
});

export const rename = mutation({
  args: { taskId: v.id("tasks"), title: v.string() },
  handler: async (ctx, { taskId, title }) => {
    const me = await requireMe(ctx);
    const { task } = await requireTask(ctx, me.account, taskId);
    await ctx.db.patch(taskId, { title, updatedAt: Date.now() });
    await logActivity(ctx, me, task, "renamed", title);
  },
});

/** Saves the task's notes. The last save wins (Jazz merges concurrent edits). */
export const setNotes = mutation({
  args: { taskId: v.id("tasks"), notes: v.string() },
  handler: async (ctx, { taskId, notes }) => {
    await requireTask(ctx, await requireAccount(ctx), taskId);
    await ctx.db.patch(taskId, { notes: notes || undefined, updatedAt: Date.now() });
  },
});

export const canDelete = query({
  args: { taskId: v.id("tasks") },
  handler: async (ctx, { taskId }) => {
    const account = await requireAccount(ctx);
    const { task, show } = await requireTask(ctx, account, taskId);
    return canDeleteTask(account, task, show);
  },
});

/** The chief or the task's creator deletes it. */
export const remove = mutation({
  args: { taskId: v.id("tasks") },
  handler: async (ctx, { taskId }) => {
    const me = await requireMe(ctx);
    const { task, show } = await requireTask(ctx, me.account, taskId);
    if (!canDeleteTask(me.account, task, show)) throw denied();
    await logActivity(ctx, me, task, "deleted", task.title);
    await ctx.db.delete(taskId);
  },
});
