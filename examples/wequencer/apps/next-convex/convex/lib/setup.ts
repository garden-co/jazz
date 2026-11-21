import type { Doc, Id } from "../_generated/dataModel";
import type { MutationCtx } from "../_generated/server";
import { INSTRUMENTS, instrumentForPosition, trackColor } from "../../lib/instruments";

/** A new track starts silent; its pads exist as soon as someone presses them. */
export function insertTrack(ctx: MutationCtx, sessionId: Id<"sessions">, position: number) {
  const instrument = instrumentForPosition(position);
  return ctx.db.insert("tracks", {
    session_id: sessionId,
    position,
    name: INSTRUMENTS.find((option) => option.value === instrument)!.label,
    color: trackColor(position),
    instrument,
    volume: 80,
    muted: false,
    solo: false,
  });
}

/** Turns one pad on or off: finds its row (or writes the first one). */
export async function setStep(
  ctx: MutationCtx,
  step: Pick<Doc<"steps">, "session_id" | "track_id" | "pattern_id" | "position" | "enabled">,
) {
  const existing = await ctx.db
    .query("steps")
    .withIndex("by_track_pattern_position", (q) =>
      q.eq("track_id", step.track_id).eq("pattern_id", step.pattern_id).eq("position", step.position),
    )
    .first();
  if (existing) await ctx.db.patch(existing._id, { enabled: step.enabled });
  else await ctx.db.insert("steps", { ...step, velocity: 100, probability: 100 });
}
