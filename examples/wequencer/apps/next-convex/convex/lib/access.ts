import { getAuthUserId } from "@convex-dev/auth/server";
import { ConvexError } from "convex/values";
import type { Id } from "../_generated/dataModel";
import type { QueryCtx } from "../_generated/server";

/**
 * Who can do what in Wequencer (the Jazz app's permissions.ts, as checks the
 * functions run before they read or write):
 *
 * - Members of a session (and its creator) see it, its members, tracks,
 *   patterns, steps, transport and presence.
 * - Owners and editors change tracks, patterns, pads and the transport;
 *   viewers only read.
 * - Only the creator adds or removes members and removes tracks.
 * - A step's track and pattern, and the transport's pattern, must belong to
 *   the same session.
 * - Profiles are readable by their owner and through presence in a session
 *   you can read.
 */
export const denied = () => new ConvexError("permission_denied");

export async function requireAuthor(ctx: QueryCtx) {
  const author = await getAuthUserId(ctx);
  if (!author) throw denied();
  return author;
}

export function memberships(ctx: QueryCtx, sessionId: Id<"sessions">, author: Id<"users">) {
  return ctx.db
    .query("session_members")
    .withIndex("by_session_author", (q) => q.eq("session_id", sessionId).eq("member_author", author))
    .collect();
}

export async function requireMember(ctx: QueryCtx, sessionId: Id<"sessions">, author: Id<"users">) {
  if ((await memberships(ctx, sessionId, author)).length === 0) throw denied();
}

export async function requireEditor(ctx: QueryCtx, sessionId: Id<"sessions">, author: Id<"users">) {
  const rows = await memberships(ctx, sessionId, author);
  if (!rows.some((row) => row.role === "owner" || row.role === "editor")) throw denied();
}

export async function requireCreator(ctx: QueryCtx, sessionId: Id<"sessions">, author: Id<"users">) {
  const session = await ctx.db.get(sessionId);
  if (!session || session.creator !== author) throw denied();
  return session;
}

/** The session, if `author` created it or is a member; null otherwise. */
export async function readableSession(ctx: QueryCtx, sessionId: Id<"sessions">, author: Id<"users">) {
  const session = await ctx.db.get(sessionId);
  if (!session) return null;
  if (session.creator === author) return session;
  return (await memberships(ctx, sessionId, author)).length > 0 ? session : null;
}

export async function requireTrack(ctx: QueryCtx, trackId: Id<"tracks">) {
  const track = await ctx.db.get(trackId);
  if (!track) throw denied();
  return track;
}

/** A pattern of `sessionId`; anything else is denied. */
export async function requirePatternOf(ctx: QueryCtx, patternId: Id<"patterns">, sessionId: Id<"sessions">) {
  const pattern = await ctx.db.get(patternId);
  if (!pattern || pattern.session_id !== sessionId) throw denied();
  return pattern;
}
