import { v } from "convex/values";
import { mutation, query } from "./_generated/server";
import { instrument } from "./schema";
import { requireAuthor, requireCreator, requireEditor, requireMember, requireTrack } from "./lib/access";
import { insertTrack } from "./lib/setup";
import { MAX_TRACKS } from "../lib/instruments";

export const bySession = query({
  args: { sessionId: v.id("sessions") },
  handler: async (ctx, { sessionId }) => {
    await requireMember(ctx, sessionId, await requireAuthor(ctx));
    return ctx.db
      .query("tracks")
      .withIndex("by_session_position", (q) => q.eq("session_id", sessionId))
      .collect();
  },
});

export const add = mutation({
  args: { sessionId: v.id("sessions"), position: v.number() },
  handler: async (ctx, { sessionId, position }) => {
    await requireEditor(ctx, sessionId, await requireAuthor(ctx));
    const existing = await ctx.db
      .query("tracks")
      .withIndex("by_session_position", (q) => q.eq("session_id", sessionId))
      .collect();
    if (existing.length >= MAX_TRACKS) return;
    await insertTrack(ctx, sessionId, position);
  },
});

/** Editors change a track's name, instrument and mix. */
export const update = mutation({
  args: {
    trackId: v.id("tracks"),
    change: v.object({
      name: v.optional(v.string()),
      instrument: v.optional(instrument),
      volume: v.optional(v.number()),
      muted: v.optional(v.boolean()),
      solo: v.optional(v.boolean()),
    }),
  },
  handler: async (ctx, { trackId, change }) => {
    const track = await requireTrack(ctx, trackId);
    await requireEditor(ctx, track.session_id, await requireAuthor(ctx));
    await ctx.db.patch(trackId, change);
  },
});

/** The creator removes a track with every step it has, in one transaction. */
export const remove = mutation({
  args: { trackId: v.id("tracks") },
  handler: async (ctx, { trackId }) => {
    const track = await requireTrack(ctx, trackId);
    await requireCreator(ctx, track.session_id, await requireAuthor(ctx));
    const steps = await ctx.db
      .query("steps")
      .withIndex("by_track_pattern_position", (q) => q.eq("track_id", trackId))
      .collect();
    for (const step of steps) await ctx.db.delete(step._id);
    await ctx.db.delete(trackId);
  },
});
