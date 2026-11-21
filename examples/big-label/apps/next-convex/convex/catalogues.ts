import { v } from "convex/values";
import { mutation, query } from "./_generated/server";
import { idOf, requireAdmin, requireInLabel, requireMember } from "./lib/access";

export const list = query({
  args: { organizationId: v.id("organizations") },
  handler: async (ctx, { organizationId }) => {
    await requireMember(ctx, organizationId);
    return ctx.db
      .query("catalogues")
      .withIndex("by_org_name", (q) => q.eq("organizationId", organizationId))
      .collect();
  },
});

export const get = query({
  args: { organizationId: v.id("organizations"), id: v.string() },
  handler: async (ctx, { organizationId, id }) => {
    await requireMember(ctx, organizationId);
    const catalogueId = idOf(ctx, "catalogues", id);
    const catalogue = catalogueId ? await ctx.db.get(catalogueId) : null;
    return catalogue?.organizationId === organizationId ? catalogue : null;
  },
});

const fields = { name: v.string(), code: v.string() };

/** Admins add and edit catalogues. */
export const create = mutation({
  args: { organizationId: v.id("organizations"), ...fields },
  handler: async (ctx, { organizationId, ...catalogue }) => {
    await requireAdmin(ctx, organizationId);
    return ctx.db.insert("catalogues", { organizationId, ...catalogue });
  },
});

export const update = mutation({
  args: { organizationId: v.id("organizations"), catalogueId: v.id("catalogues"), ...fields },
  handler: async (ctx, { organizationId, catalogueId, ...catalogue }) => {
    await requireAdmin(ctx, organizationId);
    await requireInLabel(ctx, catalogueId, organizationId);
    await ctx.db.patch(catalogueId, catalogue);
  },
});
