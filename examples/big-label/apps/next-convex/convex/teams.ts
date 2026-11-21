import { v } from "convex/values";
import { mutation, query } from "./_generated/server";
import { idOf, requireAdmin, requireInLabel, requireMember } from "./lib/access";

/** Upper bound for the per-team counts; there is no aggregate count query. */
const countLimit = 2000;

/** The label's teams by name, with bounded member and release counts. */
export const list = query({
  args: { organizationId: v.id("organizations") },
  handler: async (ctx, { organizationId }) => {
    await requireMember(ctx, organizationId);
    const [teams, members, releases] = await Promise.all([
      ctx.db.query("teams").withIndex("by_org_name", (q) => q.eq("organizationId", organizationId)).collect(),
      ctx.db.query("teamAssignments").withIndex("by_org_team", (q) => q.eq("organizationId", organizationId)).take(countLimit),
      ctx.db.query("releaseTeams").withIndex("by_org_team", (q) => q.eq("organizationId", organizationId)).take(countLimit),
    ]);
    const count = (rows: { teamId: string }[], teamId: string) => rows.filter((row) => row.teamId === teamId).length;
    return teams.map((team) => ({
      ...team,
      members: count(members, team._id),
      membersCapped: members.length >= countLimit,
      releases: count(releases, team._id),
      releasesCapped: releases.length >= countLimit,
    }));
  },
});

export const get = query({
  args: { organizationId: v.id("organizations"), id: v.string() },
  handler: async (ctx, { organizationId, id }) => {
    await requireMember(ctx, organizationId);
    const teamId = idOf(ctx, "teams", id);
    const team = teamId ? await ctx.db.get(teamId) : null;
    return team?.organizationId === organizationId ? team : null;
  },
});

/** A team's members with their people, and the releases it works on. */
export const details = query({
  args: { organizationId: v.id("organizations"), teamId: v.id("teams") },
  handler: async (ctx, { organizationId, teamId }) => {
    await requireMember(ctx, organizationId);
    const [assignments, releaseTeams] = await Promise.all([
      ctx.db
        .query("teamAssignments")
        .withIndex("by_org_team", (q) => q.eq("organizationId", organizationId).eq("teamId", teamId))
        .collect(),
      ctx.db
        .query("releaseTeams")
        .withIndex("by_org_team", (q) => q.eq("organizationId", organizationId).eq("teamId", teamId))
        .take(200),
    ]);
    return {
      assignments: await Promise.all(
        assignments.map(async (assignment) => {
          const membership = await ctx.db.get(assignment.membershipId);
          const person = membership ? await ctx.db.get(membership.personId) : null;
          return { ...assignment, membership: membership && { ...membership, person } };
        }),
      ),
      releases: (await Promise.all(releaseTeams.map((row) => ctx.db.get(row.releaseId)))).filter(
        (release) => release !== null,
      ),
    };
  },
});

/** Admins create, rename and delete teams, and staff them. */
export const create = mutation({
  args: { organizationId: v.id("organizations"), name: v.string() },
  handler: async (ctx, { organizationId, name }) => {
    await requireAdmin(ctx, organizationId);
    return ctx.db.insert("teams", { organizationId, name });
  },
});

export const rename = mutation({
  args: { organizationId: v.id("organizations"), teamId: v.id("teams"), name: v.string() },
  handler: async (ctx, { organizationId, teamId, name }) => {
    await requireAdmin(ctx, organizationId);
    await requireInLabel(ctx, teamId, organizationId);
    await ctx.db.patch(teamId, { name });
  },
});

/** Deletes a team with its memberships and release assignments. */
export const remove = mutation({
  args: { organizationId: v.id("organizations"), teamId: v.id("teams") },
  handler: async (ctx, { organizationId, teamId }) => {
    await requireAdmin(ctx, organizationId);
    await requireInLabel(ctx, teamId, organizationId);
    for (const table of ["teamAssignments", "releaseTeams"] as const) {
      const rows = await ctx.db
        .query(table)
        .withIndex("by_org_team", (q) => q.eq("organizationId", organizationId).eq("teamId", teamId))
        .collect();
      for (const row of rows) await ctx.db.delete(row._id);
    }
    await ctx.db.delete(teamId);
  },
});

/** Puts a member on a team; both must be this label's. */
export const addMember = mutation({
  args: {
    organizationId: v.id("organizations"),
    teamId: v.id("teams"),
    membershipId: v.id("memberships"),
    role: v.string(),
  },
  handler: async (ctx, { organizationId, teamId, membershipId, role }) => {
    await requireAdmin(ctx, organizationId);
    await requireInLabel(ctx, teamId, organizationId);
    await requireInLabel(ctx, membershipId, organizationId);
    return ctx.db.insert("teamAssignments", { organizationId, teamId, membershipId, role });
  },
});

export const removeMember = mutation({
  args: { organizationId: v.id("organizations"), assignmentId: v.id("teamAssignments") },
  handler: async (ctx, { organizationId, assignmentId }) => {
    await requireAdmin(ctx, organizationId);
    const assignment = await ctx.db.get(assignmentId);
    if (assignment?.organizationId === organizationId) await ctx.db.delete(assignmentId);
  },
});
