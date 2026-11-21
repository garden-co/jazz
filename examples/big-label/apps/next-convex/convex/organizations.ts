import { v } from "convex/values";
import { mutation, query } from "./_generated/server";
import { requireAdmin, requireUser } from "./lib/access";

/** Every label the caller belongs to, with their membership: the label switcher. */
export const mine = query({
  args: {},
  handler: async (ctx) => {
    const userId = await requireUser(ctx);
    const memberships = await ctx.db
      .query("memberships")
      .withIndex("by_user", (q) => q.eq("userId", userId))
      .take(100);
    return Promise.all(
      memberships.map(async (membership) => ({
        ...membership,
        organization: await ctx.db.get(membership.organizationId),
      })),
    );
  },
});

/** Admins rename the label. */
export const rename = mutation({
  args: { organizationId: v.id("organizations"), name: v.string() },
  handler: async (ctx, { organizationId, name }) => {
    await requireAdmin(ctx, organizationId);
    await ctx.db.patch(organizationId, { name });
  },
});
