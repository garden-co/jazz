import { v } from "convex/values";
import { mutation, query } from "./_generated/server";
import { requireAccount, requireChief, requireMe, showIfCrew } from "./lib/access";
import { insertShow } from "./lib/stage";

const showInput = { name: v.string(), venue: v.string(), date: v.string(), doors: v.string() };

/** The shows you crew on, by date, with their tasks' statuses for the progress bar. */
export const list = query({
  args: {},
  handler: async (ctx) => {
    const account = await requireAccount(ctx);
    const memberships = await ctx.db
      .query("showCrew")
      .withIndex("by_account", (q) => q.eq("account", account))
      .collect();
    const showIds = [...new Set(memberships.map((member) => member.showId))];
    const shows = await Promise.all(
      showIds.map(async (showId) => {
        const show = await ctx.db.get(showId);
        if (!show) return null;
        const tasks = await ctx.db
          .query("tasks")
          .withIndex("by_show_rank", (q) => q.eq("showId", showId))
          .collect();
        return { ...show, tasks: tasks.map((task) => ({ status: task.status })) };
      }),
    );
    return shows.filter((show) => show !== null).sort((a, b) => a.date.localeCompare(b.date));
  },
});

/** One show, or null for outsiders: the same answer as for a show that doesn't exist. */
export const get = query({
  args: { showId: v.string() },
  handler: async (ctx, args) => {
    const account = await requireAccount(ctx);
    const showId = ctx.db.normalizeId("shows", args.showId);
    return showId ? showIfCrew(ctx, account, showId) : null;
  },
});

export const create = mutation({
  args: showInput,
  handler: async (ctx, input) => insertShow(ctx, await requireMe(ctx), input),
});

export const update = mutation({
  args: { showId: v.id("shows"), ...showInput },
  handler: async (ctx, { showId, ...input }) => {
    await requireChief(ctx, await requireAccount(ctx), showId);
    await ctx.db.patch(showId, input);
  },
});
