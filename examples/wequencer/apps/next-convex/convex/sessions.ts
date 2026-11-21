import { v } from "convex/values";
import { mutation, query } from "./_generated/server";
import { readableSession, requireAuthor } from "./lib/access";
import { insertTrack, setStep } from "./lib/setup";
import { starterStep, MAX_STEPS } from "../lib/instruments";

/** Sessions you created or belong to, newest first. */
export const list = query({
  args: {},
  handler: async (ctx) => {
    const author = await requireAuthor(ctx);
    const memberships = await ctx.db
      .query("session_members")
      .withIndex("by_author", (q) => q.eq("member_author", author))
      .collect();
    const created = await ctx.db
      .query("sessions")
      .withIndex("by_creator", (q) => q.eq("creator", author))
      .collect();
    const ids = new Set([...memberships.map((m) => m.session_id), ...created.map((s) => s._id)]);
    const sessions = await Promise.all([...ids].map((id) => ctx.db.get(id)));
    return sessions
      .filter((session) => session !== null)
      .sort((a, b) => b._creationTime - a._creationTime);
  },
});

/** One session, or null when it doesn't exist or you can't read it. */
export const get = query({
  args: { sessionId: v.string() },
  handler: async (ctx, args) => {
    const author = await requireAuthor(ctx);
    const sessionId = ctx.db.normalizeId("sessions", args.sessionId);
    return sessionId ? readableSession(ctx, sessionId, author) : null;
  },
});

/**
 * Creates a session, its first pattern and a starter groove in one
 * transaction, so bandmates never see a half-built session.
 */
export const create = mutation({
  args: { title: v.string(), tempo: v.number(), trackCount: v.number(), length: v.number() },
  handler: async (ctx, { title, tempo, trackCount, length }) => {
    const author = await requireAuthor(ctx);
    const sessionId = await ctx.db.insert("sessions", { title, tempo_bpm: tempo, creator: author });
    await ctx.db.insert("session_members", { session_id: sessionId, member_author: author, role: "owner" });
    const patternId = await ctx.db.insert("patterns", {
      session_id: sessionId,
      position: 0,
      name: "Pattern 1",
      length,
    });
    for (let position = 0; position < Math.min(trackCount, 16); position += 1) {
      const trackId = await insertTrack(ctx, sessionId, position);
      const track = (await ctx.db.get(trackId))!;
      for (let step = 0; step < MAX_STEPS; step += 1) {
        if (!starterStep(track.instrument, step)) continue;
        await setStep(ctx, { session_id: sessionId, track_id: trackId, pattern_id: patternId, position: step, enabled: true });
      }
    }
    await ctx.db.insert("transport_observations", {
      session_id: sessionId,
      playing: false,
      bar: 0,
      observed_at: Date.now(),
      tempo_bpm: tempo,
      pattern_id: patternId,
    });
    return sessionId;
  },
});
