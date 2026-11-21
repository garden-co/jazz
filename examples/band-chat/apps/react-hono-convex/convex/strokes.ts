import { v } from "convex/values";
import { mutation, query } from "./_generated/server";
import type { Id } from "./_generated/dataModel";
import type { QueryCtx } from "./_generated/server";
import { denied, requireAuthor, requireMember } from "./lib/access";

/** The canvas, if `author` belongs to its room: a removed member can no longer draw. */
async function requireCanvas(ctx: QueryCtx, canvasId: Id<"canvases">, author: Id<"users">) {
  const canvas = await ctx.db.get(canvasId);
  if (!canvas) throw denied();
  await requireMember(ctx, canvas.roomId, author);
  return canvas;
}

/** A sketch's strokes, oldest first. */
export const byCanvas = query({
  args: { canvasId: v.id("canvases") },
  handler: async (ctx, { canvasId }) => {
    await requireCanvas(ctx, canvasId, await requireAuthor(ctx));
    return ctx.db
      .query("strokes")
      .withIndex("by_canvas", (q) => q.eq("canvasId", canvasId))
      .collect();
  },
});

/** Each finished stroke is one row, drawn as its author. */
export const add = mutation({
  args: { canvasId: v.id("canvases"), color: v.string(), width: v.number(), points: v.array(v.number()) },
  handler: async (ctx, { canvasId, ...stroke }) => {
    const author = await requireAuthor(ctx);
    const canvas = await requireCanvas(ctx, canvasId, author);
    await ctx.db.insert("strokes", { canvasId, roomId: canvas.roomId, author, ...stroke });
  },
});

export const remove = mutation({
  args: { strokeId: v.id("strokes") },
  handler: async (ctx, { strokeId }) => {
    const author = await requireAuthor(ctx);
    const stroke = await ctx.db.get(strokeId);
    if (!stroke || stroke.author !== author) throw denied();
    await requireMember(ctx, stroke.roomId, author);
    await ctx.db.delete(strokeId);
  },
});
