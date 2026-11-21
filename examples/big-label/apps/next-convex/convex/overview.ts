import { v } from "convex/values";
import { query } from "./_generated/server";
import { requireMember } from "./lib/access";

/** Upper bound for the overview's counts; there is no aggregate count query. */
const countLimit = 5000;

/** The Overview's big numbers, from bounded reads. */
export const counts = query({
  args: { organizationId: v.id("organizations") },
  handler: async (ctx, { organizationId }) => {
    await requireMember(ctx, organizationId);
    const [artists, releases, members, teams] = await Promise.all([
      ctx.db.query("artists").withIndex("by_org_name", (q) => q.eq("organizationId", organizationId)).take(countLimit),
      ctx.db.query("releases").withIndex("by_org_releaseDate", (q) => q.eq("organizationId", organizationId)).take(countLimit),
      ctx.db.query("memberships").withIndex("by_org_user", (q) => q.eq("organizationId", organizationId)).take(countLimit),
      ctx.db.query("teams").withIndex("by_org_name", (q) => q.eq("organizationId", organizationId)).take(countLimit),
    ]);
    const counted = (rows: unknown[]) => ({ count: rows.length, isCapped: rows.length >= countLimit });
    return {
      artists: counted(artists),
      releases: counted(releases),
      scheduled: releases.filter((release) => release.status === "scheduled").length,
      members: counted(members),
      teams: counted(teams),
    };
  },
});
