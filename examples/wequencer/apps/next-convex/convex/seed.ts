// Bench plumbing for scripts/seed.mjs (run with `npx convex run`).
import { v } from "convex/values";
import { internal } from "./_generated/api";
import type { TableNames } from "./_generated/dataModel";
import { internalAction, internalMutation } from "./_generated/server";
import { insertTrack, setStep } from "./lib/setup";
import { BENCH_SESSION, wequencerFixture } from "../../../../../dev/app-comparisons/fixtures/wequencer.mjs";

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

const TABLES: TableNames[] = [
  "presence", "transport_observations", "steps", "patterns", "tracks", "session_members",
  "sessions", "profiles", "authRefreshTokens", "authSessions", "authAccounts",
  "authVerificationCodes", "authVerifiers", "authRateLimits", "users",
];

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

type Fixture = ReturnType<typeof wequencerFixture>;

/**
 * One fixture session as its creator writes it (like `sessions.create`, with
 * the fixture's members, tracks and pads instead of the starter groove).
 */
export const createSession = internalMutation({
  args: { key: v.string(), users: v.record(v.string(), v.id("users")) },
  handler: async (ctx, { key, users }) => {
    const fixture: Fixture = wequencerFixture();
    const session = fixture.sessions.find((s: { key: string }) => s.key === key)!;
    const sessionId = await ctx.db.insert("sessions", {
      title: session.name,
      tempo_bpm: session.bpm,
      creator: users.seed!,
    });
    for (const m of fixture.members.filter((m: { sessionKey: string }) => m.sessionKey === key)) {
      await ctx.db.insert("session_members", { session_id: sessionId, member_author: users[m.user]!, role: m.role as "owner" | "editor" | "viewer" });
    }
    const patternId = await ctx.db.insert("patterns", { session_id: sessionId, position: 0, name: "Pattern 1", length: 64 });
    const trackIds = new Map<string, Awaited<ReturnType<typeof insertTrack>>>();
    for (const t of fixture.tracks.filter((t: { sessionKey: string }) => t.sessionKey === key)) {
      trackIds.set(t.key, await insertTrack(ctx, sessionId, t.position));
    }
    for (const pad of fixture.pads) {
      const trackId = trackIds.get(pad.trackKey);
      if (!trackId) continue;
      await setStep(ctx, { session_id: sessionId, track_id: trackId, pattern_id: patternId, position: pad.step, enabled: true });
    }
    await ctx.db.insert("transport_observations", {
      session_id: sessionId,
      playing: false,
      bar: 0,
      observed_at: Date.now(),
      tempo_bpm: session.bpm,
      pattern_id: patternId,
    });
    return sessionId;
  },
});

/** Every fixture session; returns the bench session's id and its enabled pads. */
export const writeSessions = internalAction({
  args: { users: v.record(v.string(), v.id("users")) },
  handler: async (ctx, { users }) => {
    const fixture: Fixture = wequencerFixture();
    let sessionId = "";
    for (const session of fixture.sessions) {
      const id = await throttled(() => ctx.runMutation(internal.seed.createSession, { key: session.key, users }));
      if (session.key === BENCH_SESSION) sessionId = id;
    }
    const onPads = fixture.pads.filter((p: { trackKey: string }) => p.trackKey.startsWith(`${BENCH_SESSION}-`)).length;
    return { sessionId, onPads };
  },
});
