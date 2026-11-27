import { getAuthUserId } from "@convex-dev/auth/server";
import { ConvexError } from "convex/values";
import type { Doc, Id } from "../_generated/dataModel";
import type { QueryCtx } from "../_generated/server";

/**
 * Who can do what in StagePlan (the Jazz app's permissions.ts, as checks
 * every function runs before it reads or writes):
 *
 * - The crew chief (the account that created a show) manages the show, its
 *   invite link and its crew.
 * - Crew members (anyone with a showCrew row) see the show and can add, edit
 *   and move its tasks, comment and log activity.
 * - Everyone else sees nothing of the show: not the show, its tasks,
 *   comments, activity or crew list.
 * - Checklist items are private to their owner.
 */
export const denied = () => new ConvexError("permission_denied");

export async function requireAccount(ctx: QueryCtx) {
  const account = await getAuthUserId(ctx);
  if (!account) throw denied();
  return account;
}

/** The signed-in person's crew profile; null before it exists. */
export async function myProfile(ctx: QueryCtx, account: Id<"users">) {
  return ctx.db
    .query("crew")
    .withIndex("by_account", (q) => q.eq("account", account))
    .unique();
}

export async function requireMe(ctx: QueryCtx) {
  const account = await requireAccount(ctx);
  const profile = await myProfile(ctx, account);
  if (!profile) throw denied();
  return { account, profile };
}

/** The show, if `account` is on its crew (or is its chief); null otherwise. */
export async function showIfCrew(ctx: QueryCtx, account: Id<"users">, showId: Id<"shows">) {
  const show = await ctx.db.get(showId);
  if (!show) return null;
  if (show.chiefAccount === account) return show;
  const membership = await ctx.db
    .query("showCrew")
    .withIndex("by_show_account", (q) => q.eq("showId", showId).eq("account", account))
    .first();
  return membership ? show : null;
}

export async function requireCrew(ctx: QueryCtx, account: Id<"users">, showId: Id<"shows">) {
  const show = await showIfCrew(ctx, account, showId);
  if (!show) throw denied();
  return show;
}

export async function requireChief(ctx: QueryCtx, account: Id<"users">, showId: Id<"shows">) {
  const show = await ctx.db.get(showId);
  if (!show || show.chiefAccount !== account) throw denied();
  return show;
}

/** The task and its show, if `account` is on the show's crew. */
export async function requireTask(ctx: QueryCtx, account: Id<"users">, taskId: Id<"tasks">) {
  const task = await ctx.db.get(taskId);
  if (!task) throw denied();
  const show = await requireCrew(ctx, account, task.showId);
  return { task, show };
}

/** A task is assigned to nobody or to someone on the show's crew. */
export async function requireAssigneeIsCrew(
  ctx: QueryCtx,
  showId: Id<"shows">,
  assigneeId: Id<"crew"> | undefined,
) {
  if (!assigneeId) return;
  const member = await ctx.db
    .query("showCrew")
    .withIndex("by_show_crew", (q) => q.eq("showId", showId).eq("crewId", assigneeId))
    .first();
  if (!member) throw denied();
}

/** The chief or the task's creator may delete it. */
export function canDeleteTask(account: Id<"users">, task: Doc<"tasks">, show: Doc<"shows">) {
  return show.chiefAccount === account || task.createdBy === account;
}
