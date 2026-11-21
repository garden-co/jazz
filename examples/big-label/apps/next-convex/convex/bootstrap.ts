import { mutation } from "./_generated/server";
import { requireUser } from "./lib/access";
import { normalizeEmail } from "../src/lib/emails";

/**
 * First-tenant bootstrap (the Jazz app's POST /api/bootstrap). For the
 * signed-in user only, it gets or creates their one profile, their
 * `personal-<userId>` label and their admin membership, and records their
 * sign-in email so admins can add them by email. One transaction: a
 * concurrent call retries against the committed rows. Duplicates or a
 * mismatched personal membership are an error, never "first row wins".
 */
export const ensurePersonalOrganization = mutation({
  args: {},
  handler: async (ctx) => {
    const userId = await requireUser(ctx);
    const user = (await ctx.db.get(userId))!;
    const name = user.name || user.email || userId;
    const slug = `personal-${userId}`;
    const people = await ctx.db.query("people").withIndex("by_user", (q) => q.eq("userId", userId)).collect();
    const organizations = await ctx.db.query("organizations").withIndex("by_slug", (q) => q.eq("slug", slug)).collect();
    if (people.length > 1 || organizations.length > 1) throw new Error("Bootstrap refused ambiguous rows");
    const personId = people[0]?._id ?? (await ctx.db.insert("people", { userId, name }));
    const organizationId =
      organizations[0]?._id ?? (await ctx.db.insert("organizations", { name: `${name}'s label`, slug }));
    const memberships = await ctx.db
      .query("memberships")
      .withIndex("by_org_user", (q) => q.eq("organizationId", organizationId).eq("userId", userId))
      .collect();
    if (memberships.length > 1) throw new Error("Bootstrap refused ambiguous memberships");
    const membership = memberships[0];
    if (membership && (membership.personId !== personId || membership.role !== "admin"))
      throw new Error("Bootstrap found an invalid personal organization membership");
    if (!membership) await ctx.db.insert("memberships", { organizationId, personId, userId, role: "admin" });
    if (user.email) {
      const email = normalizeEmail(user.email);
      const existing = await ctx.db.query("personEmails").withIndex("by_person", (q) => q.eq("personId", personId)).first();
      if (!existing) await ctx.db.insert("personEmails", { personId, email });
      else if (existing.email !== email) await ctx.db.patch(existing._id, { email });
    }
    return organizationId;
  },
});
