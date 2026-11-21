import { v } from "convex/values";
import { mutation, query } from "./_generated/server";
import { denied, myProfile, requireAccount, requireChief, requireCrew, requireMe } from "./lib/access";
import { insertShow, logActivity } from "./lib/stage";
import { DEMO_COMMENT, DEMO_SHOW, DEMO_TASKS } from "./lib/demo";

/** The signed-in person's crew profile (null until set up). */
export const me = query({
  args: {},
  handler: async (ctx) => {
    const account = await requireAccount(ctx);
    return myProfile(ctx, account);
  },
});

/**
 * Makes sure the account has a crew profile and gives a new account the demo
 * show, unless it arrived through an invite link. One transaction.
 */
export const setUp = mutation({
  args: { withDemo: v.boolean() },
  handler: async (ctx, { withDemo }) => {
    const account = await requireAccount(ctx);
    if (await myProfile(ctx, account)) return;
    const profileId = await ctx.db.insert("crew", {
      account,
      name: `Stagehand ${account.slice(-4).toUpperCase()}`,
    });
    if (!withDemo) return;
    const me = { account, profile: (await ctx.db.get(profileId))! };
    const showId = await insertShow(ctx, me, DEMO_SHOW);
    const now = Date.now();
    for (const [index, demo] of DEMO_TASKS.entries()) {
      const taskId = await ctx.db.insert("tasks", {
        showId,
        title: demo.title,
        status: demo.status,
        assigneeId: demo.mine ? profileId : undefined,
        notes: demo.notes,
        rank: index + 1,
        createdBy: account,
        updatedAt: now,
      });
      const task = { _id: taskId, showId };
      await logActivity(ctx, me, task, "created");
      if (demo.title === "Line check") {
        await ctx.db.insert("comments", { taskId, authorId: profileId, body: DEMO_COMMENT });
        await logActivity(ctx, me, task, "commented");
      }
    }
  },
});

/** Crew profiles: names are shared, only you edit yours. */
export const rename = mutation({
  args: { name: v.string() },
  handler: async (ctx, { name }) => {
    const { profile } = await requireMe(ctx);
    await ctx.db.patch(profile._id, { name });
  },
});

/** A show's crew, chief first, with each person's profile. */
export const list = query({
  args: { showId: v.id("shows") },
  handler: async (ctx, { showId }) => {
    const account = await requireAccount(ctx);
    await requireCrew(ctx, account, showId);
    const members = await ctx.db
      .query("showCrew")
      .withIndex("by_show_role", (q) => q.eq("showId", showId))
      .collect();
    return Promise.all(
      members.map(async (member) => ({ ...member, crew: await ctx.db.get(member.crewId) })),
    );
  },
});

/** Joins a show with an invite code, checked against the show's private invites. */
export const join = mutation({
  args: { showId: v.string(), code: v.string() },
  handler: async (ctx, args) => {
    const { account, profile } = await requireMe(ctx);
    const showId = ctx.db.normalizeId("shows", args.showId);
    if (!showId) throw denied();
    const existing = await ctx.db
      .query("showCrew")
      .withIndex("by_show_account", (q) => q.eq("showId", showId).eq("account", account))
      .first();
    if (existing) return;
    const invite = await ctx.db
      .query("showInvites")
      .withIndex("by_show_code", (q) => q.eq("showId", showId).eq("code", args.code))
      .first();
    if (!invite) throw denied();
    await ctx.db.insert("showCrew", { showId, crewId: profile._id, account, role: "crew" });
  },
});

/**
 * Takes someone off a show's crew: members leave on their own, the chief can
 * remove anyone. Their tasks go back to nobody, because tasks may only be
 * assigned to crew.
 */
export const remove = mutation({
  args: { memberId: v.id("showCrew") },
  handler: async (ctx, { memberId }) => {
    const account = await requireAccount(ctx);
    const member = await ctx.db.get(memberId);
    if (!member) return;
    if (member.account !== account) await requireChief(ctx, account, member.showId);
    const assigned = await ctx.db
      .query("tasks")
      .withIndex("by_show_assignee", (q) =>
        q.eq("showId", member.showId).eq("assigneeId", member.crewId),
      )
      .collect();
    for (const task of assigned) {
      await ctx.db.patch(task._id, { assigneeId: undefined, updatedAt: Date.now() });
    }
    await ctx.db.delete(memberId);
  },
});
