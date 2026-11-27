import { v } from "convex/values";
import { mutation, query } from "./_generated/server";
import { idOf, requireEditor, requireInLabel, requireMember } from "./lib/access";

/** The teams working on a release. */
export const byRelease = query({
  args: { organizationId: v.id("organizations"), releaseId: v.string() },
  handler: async (ctx, { organizationId, releaseId }) => {
    await requireMember(ctx, organizationId);
    const id = idOf(ctx, "releases", releaseId);
    if (!id) return [];
    const rows = await ctx.db
      .query("releaseTeams")
      .withIndex("by_org_release", (q) => q.eq("organizationId", organizationId).eq("releaseId", id))
      .collect();
    return Promise.all(rows.map(async (row) => ({ ...row, team: await ctx.db.get(row.teamId) })));
  },
});

/** Editors staff releases with teams; the release and the team must be this label's. */
export const assign = mutation({
  args: { organizationId: v.id("organizations"), releaseId: v.id("releases"), teamId: v.id("teams") },
  handler: async (ctx, { organizationId, releaseId, teamId }) => {
    await requireEditor(ctx, organizationId);
    await requireInLabel(ctx, releaseId, organizationId);
    await requireInLabel(ctx, teamId, organizationId);
    return ctx.db.insert("releaseTeams", { organizationId, releaseId, teamId });
  },
});

export const remove = mutation({
  args: { organizationId: v.id("organizations"), releaseTeamId: v.id("releaseTeams") },
  handler: async (ctx, { organizationId, releaseTeamId }) => {
    await requireEditor(ctx, organizationId);
    const row = await ctx.db.get(releaseTeamId);
    if (row?.organizationId === organizationId) await ctx.db.delete(releaseTeamId);
  },
});
