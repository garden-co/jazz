import { v } from "convex/values";
import { mutation, query } from "./_generated/server";
import { requireAuthor, requireEditor, requireMember, requirePatternOf, requireTrack } from "./lib/access";
import { setStep } from "./lib/setup";

/** One lane's steps for a pattern, in order: each track lane subscribes to its own. */
export const lane = query({
  args: { trackId: v.id("tracks"), patternId: v.id("patterns") },
  handler: async (ctx, { trackId, patternId }) => {
    const track = await requireTrack(ctx, trackId);
    await requireMember(ctx, track.session_id, await requireAuthor(ctx));
    return ctx.db
      .query("steps")
      .withIndex("by_track_pattern_position", (q) => q.eq("track_id", trackId).eq("pattern_id", patternId))
      .collect();
  },
});

/**
 * Editors turn a pad on or off. The track and the pattern must belong to
 * the same session. The row lookup and write are one transaction, so two
 * bandmates pressing the same untouched pad converge on one row.
 */
export const set = mutation({
  args: { trackId: v.id("tracks"), patternId: v.id("patterns"), position: v.number(), enabled: v.boolean() },
  handler: async (ctx, { trackId, patternId, position, enabled }) => {
    const track = await requireTrack(ctx, trackId);
    await requireEditor(ctx, track.session_id, await requireAuthor(ctx));
    await requirePatternOf(ctx, patternId, track.session_id);
    await setStep(ctx, { session_id: track.session_id, track_id: trackId, pattern_id: patternId, position, enabled });
  },
});
