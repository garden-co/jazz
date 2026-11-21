import { getAuthUserId } from "@convex-dev/auth/server";
import { ConvexError } from "convex/values";
import type { Id } from "../_generated/dataModel";
import type { QueryCtx } from "../_generated/server";
import { catalogueEditors, type Role } from "../../src/roles";

/**
 * Who can do what in BigLabel (the Jazz app's permissions.ts, as checks every
 * function runs before it reads or writes). Roles are in src/roles.ts:
 * admins run the label, editors maintain the catalogue, viewers read it.
 *
 * - A label and everything in it is visible to its members only.
 * - Browsers never create labels or people: the bootstrap and demo-data
 *   mutations create them, for the caller only.
 * - Only admins add members (by email, never as admins), change roles,
 *   manage teams and catalogues, rename the label and delete records.
 * - Editors add and edit artists and releases and staff releases with teams.
 * - Every reference (release -> artist/catalogue, assignment -> team/
 *   membership, release team -> release/team) stays inside the label.
 * - People are visible to the members of their labels only.
 */
export const denied = () => new ConvexError("permission_denied");

export async function requireUser(ctx: QueryCtx) {
  const userId = await getAuthUserId(ctx);
  if (!userId) throw denied();
  return userId;
}

export function membershipOf(ctx: QueryCtx, organizationId: Id<"organizations">, userId: Id<"users">) {
  return ctx.db
    .query("memberships")
    .withIndex("by_org_user", (q) => q.eq("organizationId", organizationId).eq("userId", userId))
    .first();
}

/** The caller's membership of the label, if their role is one of `roles`. */
export async function requireRole(
  ctx: QueryCtx,
  organizationId: Id<"organizations">,
  roles: readonly Role[] = ["admin", "editor", "viewer"],
) {
  const userId = await requireUser(ctx);
  const membership = await membershipOf(ctx, organizationId, userId);
  if (!membership || !(roles as readonly string[]).includes(membership.role)) throw denied();
  return membership;
}

export const requireMember = (ctx: QueryCtx, organizationId: Id<"organizations">) =>
  requireRole(ctx, organizationId);
export const requireEditor = (ctx: QueryCtx, organizationId: Id<"organizations">) =>
  requireRole(ctx, organizationId, catalogueEditors);
export const requireAdmin = (ctx: QueryCtx, organizationId: Id<"organizations">) =>
  requireRole(ctx, organizationId, ["admin"]);

/** A row of `table` that belongs to `organizationId`; anything else is denied. */
export async function requireInLabel<T extends "artists" | "catalogues" | "teams" | "memberships" | "releases">(
  ctx: QueryCtx,
  id: Id<T>,
  organizationId: Id<"organizations">,
) {
  const row = await ctx.db.get(id);
  if (!row || (row as unknown as { organizationId: Id<"organizations"> }).organizationId !== organizationId)
    throw denied();
  return row;
}

/** Turns an id from the URL into a row id of `table`, or null. */
export function idOf<T extends "artists" | "catalogues" | "teams" | "releases" | "organizations">(
  ctx: QueryCtx,
  table: T,
  id: string,
) {
  return ctx.db.normalizeId(table, id);
}
