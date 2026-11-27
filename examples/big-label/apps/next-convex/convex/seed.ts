// Bench plumbing for scripts/seed.mjs (run with `npx convex run`): the
// server-side steps. Everything a label's admin writes goes through the
// app's public functions instead (see the script).
import { v } from "convex/values";
import { internal } from "./_generated/api";
import type { TableNames } from "./_generated/dataModel";
import { internalAction, internalMutation } from "./_generated/server";

const TABLES: TableNames[] = [
  "releaseTeams", "releases", "artists", "catalogues", "teamAssignments", "teams", "memberships",
  "personEmails", "people", "organizations", "authRefreshTokens", "authSessions", "authAccounts",
  "authVerificationCodes", "authVerifiers", "authRateLimits", "users",
];

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

export const clear = internalMutation({
  args: { table: v.string() },
  handler: async (ctx, { table }) => {
    const docs = await ctx.db.query(table as TableNames).take(4000);
    for (const doc of docs) await ctx.db.delete(doc._id);
    return docs.length === 4000;
  },
});

export const reset = internalAction({
  args: {},
  handler: async (ctx) => {
    for (const table of TABLES) while (await throttled(() => ctx.runMutation(internal.seed.clear, { table }))) {}
  },
});

/**
 * A fixture label and its first admin's membership, written by the server
 * as the demo-data mutation does: browsers never create labels.
 */
export const createLabel = internalMutation({
  args: { name: v.string(), slug: v.string(), adminUserId: v.id("users") },
  handler: async (ctx, { name, slug, adminUserId }) => {
    const person = await ctx.db.query("people").withIndex("by_user", (q) => q.eq("userId", adminUserId)).first();
    if (!person) throw new Error("The admin hasn't run the bootstrap");
    const organizationId = await ctx.db.insert("organizations", { name, slug });
    await ctx.db.insert("memberships", { organizationId, personId: person._id, userId: adminUserId, role: "admin" });
    return organizationId;
  },
});
