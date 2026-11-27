import { v } from "convex/values";
import { mutation, query } from "./_generated/server";
import { denied, requireAccount } from "./lib/access";
import type { Id } from "./_generated/dataModel";
import type { QueryCtx } from "./_generated/server";

/** Checklist items are private to their owner. */
export const list = query({
  args: { search: v.string(), done: v.optional(v.boolean()) },
  handler: async (ctx, { search, done }) => {
    const account = await requireAccount(ctx);
    const items = await ctx.db
      .query("checklistItems")
      .withIndex("by_owner_title", (q) => q.eq("ownerAccount", account))
      .collect();
    return items.filter(
      (item) => item.title.includes(search) && (done === undefined || item.done === done),
    );
  },
});

export const add = mutation({
  args: { title: v.string() },
  handler: async (ctx, { title }) => {
    const ownerAccount = await requireAccount(ctx);
    await ctx.db.insert("checklistItems", { title, done: false, ownerAccount });
  },
});

async function requireMine(ctx: QueryCtx, itemId: Id<"checklistItems">) {
  const item = await ctx.db.get(itemId);
  if (!item || item.ownerAccount !== (await requireAccount(ctx))) throw denied();
  return item;
}

export const setDone = mutation({
  args: { itemId: v.id("checklistItems"), done: v.boolean() },
  handler: async (ctx, { itemId, done }) => {
    await requireMine(ctx, itemId);
    await ctx.db.patch(itemId, { done });
  },
});

export const remove = mutation({
  args: { itemId: v.id("checklistItems") },
  handler: async (ctx, { itemId }) => {
    await requireMine(ctx, itemId);
    await ctx.db.delete(itemId);
  },
});
