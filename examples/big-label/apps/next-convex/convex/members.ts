import { v } from "convex/values";
import { mutation, query } from "./_generated/server";
import { requireAdmin, requireInLabel, requireMember } from "./lib/access";
import { invitableRoles, isRole } from "../src/roles";
import { normalizeEmail } from "../src/lib/emails";

/** A label's members with their people: the only way to see someone else's profile. */
export const list = query({
  args: { organizationId: v.id("organizations") },
  handler: async (ctx, { organizationId }) => {
    await requireMember(ctx, organizationId);
    const memberships = await ctx.db
      .query("memberships")
      .withIndex("by_org_user", (q) => q.eq("organizationId", organizationId))
      .take(500);
    return Promise.all(
      memberships.map(async (membership) => ({ ...membership, person: await ctx.db.get(membership.personId) })),
    );
  },
});

/**
 * Admins add someone who has signed in to BigLabel, by their sign-in email
 * (the Jazz app's POST /api/members). Non-admins get "forbidden" before any
 * lookup, so nobody can probe emails or membership. New members can't be
 * admins. The mutation reads the person's membership first, so a double
 * submit adds them once.
 */
export const addByEmail = mutation({
  args: { organizationId: v.id("organizations"), email: v.string(), role: v.string() },
  handler: async (ctx, { organizationId, email, role }) => {
    try {
      await requireAdmin(ctx, organizationId);
    } catch {
      return { status: "forbidden" as const };
    }
    if (!(invitableRoles as readonly string[]).includes(role)) return { status: "forbidden" as const };
    const emails = await ctx.db
      .query("personEmails")
      .withIndex("by_email", (q) => q.eq("email", normalizeEmail(email)))
      .take(2);
    const person = emails.length === 1 ? await ctx.db.get(emails[0]!.personId) : null;
    if (!person) return { status: "not-found" as const };
    const existing = await ctx.db
      .query("memberships")
      .withIndex("by_org_person", (q) => q.eq("organizationId", organizationId).eq("personId", person._id))
      .first();
    if (existing) return { status: "already-member" as const, name: person.name };
    await ctx.db.insert("memberships", { organizationId, personId: person._id, userId: person.userId, role });
    return { status: "added" as const, name: person.name };
  },
});

/** Admins change roles, to any of the app's roles. */
export const changeRole = mutation({
  args: { organizationId: v.id("organizations"), membershipId: v.id("memberships"), role: v.string() },
  handler: async (ctx, { organizationId, membershipId, role }) => {
    await requireAdmin(ctx, organizationId);
    await requireInLabel(ctx, membershipId, organizationId);
    if (!isRole(role)) throw new Error(`Unknown role ${role}`);
    await ctx.db.patch(membershipId, { role });
  },
});

/** Removes a member from the label, with their team assignments. */
export const remove = mutation({
  args: { organizationId: v.id("organizations"), membershipId: v.id("memberships") },
  handler: async (ctx, { organizationId, membershipId }) => {
    await requireAdmin(ctx, organizationId);
    await requireInLabel(ctx, membershipId, organizationId);
    const assignments = await ctx.db
      .query("teamAssignments")
      .withIndex("by_org_membership", (q) => q.eq("organizationId", organizationId).eq("membershipId", membershipId))
      .collect();
    for (const row of assignments) await ctx.db.delete(row._id);
    await ctx.db.delete(membershipId);
  },
});
