// Bench plumbing: writes the StagePlan fixture as the fixture's seed chief.
// Run with `npx convex run seed:run` (internal functions skip the checks).
import { v } from "convex/values";
import { internal } from "./_generated/api";
import type { TableNames } from "./_generated/dataModel";
import { internalAction, internalMutation } from "./_generated/server";
import { BIG_SHOW, stagePlanFixture } from "../../../../../dev/app-comparisons/fixtures/stage-plan.mjs";

/** The local deployment limits writes to 4 MiB/s: when a batch hits it, wait and retry. */
async function throttled<T>(write: () => Promise<T>): Promise<T> {
  for (let attempt = 0; ; attempt++) {
    try {
      return await write();
    } catch (error) {
      if (attempt >= 60 || !String(error).includes("Too many writes")) throw error;
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }
  }
}

const TABLES: TableNames[] = [
  "activity",
  "comments",
  "tasks",
  "showInvites",
  "showCrew",
  "shows",
  "checklistItems",
  "crew",
  "authRefreshTokens",
  "authSessions",
  "authAccounts",
  "authVerificationCodes",
  "authVerifiers",
  "authRateLimits",
  "users",
];

export const clear = internalMutation({
  args: { table: v.string() },
  handler: async (ctx, { table }) => {
    const docs = await ctx.db.query(table as TableNames).take(2000);
    for (const doc of docs) await ctx.db.delete(doc._id);
    return docs.length === 2000;
  },
});

export const createChief = internalMutation({
  args: { name: v.string() },
  handler: async (ctx, { name }) => {
    const account = await ctx.db.insert("users", { name });
    const crewId = await ctx.db.insert("crew", { account, name });
    return { account, crewId };
  },
});

export const insertShow = internalMutation({
  args: { chief: v.object({ account: v.id("users"), crewId: v.id("crew") }), show: v.any() },
  handler: async (ctx, { chief, show }) => {
    const showId = await ctx.db.insert("shows", {
      name: show.name,
      venue: show.venue,
      date: show.date,
      doors: show.doors,
      chiefAccount: chief.account,
    });
    await ctx.db.insert("showCrew", {
      showId,
      crewId: chief.crewId,
      account: chief.account,
      role: "chief",
    });
    await ctx.db.insert("showInvites", { showId, code: show.invite.code });
    for (const task of show.tasks) {
      const taskId = await ctx.db.insert("tasks", {
        showId,
        title: task.title,
        status: task.status,
        rank: task.rank,
        createdBy: chief.account,
        updatedAt: Date.now(),
      });
      for (const comment of task.comments) {
        await ctx.db.insert("comments", { taskId, authorId: chief.crewId, body: comment.body });
      }
    }
    return showId;
  },
});

/** Resets the deployment's data and writes the fixture; returns show-0's join ids. */
export const run = internalAction({
  args: {},
  handler: async (ctx) => {
    for (const table of TABLES) {
      while (await throttled(() => ctx.runMutation(internal.seed.clear, { table }))) {}
    }
    const fixture = stagePlanFixture();
    const chief = await throttled(() => ctx.runMutation(internal.seed.createChief, {
      name: fixture.users.chief.name,
    }));
    let big: { showId: string; code: string } | undefined;
    for (const show of fixture.shows) {
      const invite = fixture.invites.find((i: { showKey: string }) => i.showKey === show.key);
      const tasks = fixture.tasks
        .filter((t: { showKey: string }) => t.showKey === show.key)
        .map((t: { key: string }) => ({
          ...t,
          comments: fixture.comments.filter((c: { taskKey: string }) => c.taskKey === t.key),
        }));
      const showId = await throttled(() => ctx.runMutation(internal.seed.insertShow, {
        chief,
        show: { ...show, invite, tasks },
      }));
      if (show.key === BIG_SHOW) big = { showId, code: invite!.code };
    }
    return big!;
  },
});
