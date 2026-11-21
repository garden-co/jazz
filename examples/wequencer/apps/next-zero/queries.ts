import { defineQueries, defineQuery, type ExpressionBuilder } from "@rocicorp/zero";
import { z } from "zod";
import { zql, type AuthData, type Schema } from "./schema";

/**
 * Who can read what in Wequencer (writes are checked in mutators.ts):
 *
 * - A session is visible to its creator and its members; its members list,
 *   tracks, patterns, steps, transport and presence to its members.
 * - A profile is visible to its owner, and to members of a session where it
 *   has shown presence (through the presence query's `profile`).
 *
 * zero-cache resolves each query on the server with the caller's context.
 */
const me = (ctx: AuthData | undefined) => ctx?.userID ?? "";

const isMember =
  (ctx: AuthData | undefined) =>
  ({ exists }: ExpressionBuilder<"sessions", Schema>) =>
    exists("members", (member) => member.where("member_author", me(ctx)));
const canRead =
  (ctx: AuthData | undefined) =>
  (eb: ExpressionBuilder<"sessions", Schema>) =>
    eb.or(eb.cmp("created_by", me(ctx)), isMember(ctx)(eb));

const sessionArgs = z.object({ sessionId: z.string() });

export const queries = defineQueries({
  profiles: {
    mine: defineQuery(({ ctx }) => zql.profiles.where("author", me(ctx)).one()),
  },
  sessions: {
    mine: defineQuery(({ ctx }) => zql.sessions.where(canRead(ctx)).orderBy("created_at", "desc")),
    one: defineQuery(sessionArgs, ({ ctx, args }) =>
      zql.sessions.where("id", args.sessionId).where(canRead(ctx)).one(),
    ),
  },
  members: {
    bySession: defineQuery(sessionArgs, ({ ctx, args }) =>
      zql.session_members.where("session_id", args.sessionId).whereExists("session", (s) => s.where(canRead(ctx))),
    ),
  },
  tracks: {
    bySession: defineQuery(sessionArgs, ({ ctx, args }) =>
      zql.tracks
        .where("session_id", args.sessionId)
        .whereExists("session", (s) => s.where(isMember(ctx)))
        .orderBy("position", "asc"),
    ),
  },
  patterns: {
    bySession: defineQuery(sessionArgs, ({ ctx, args }) =>
      zql.patterns
        .where("session_id", args.sessionId)
        .whereExists("session", (s) => s.where(isMember(ctx)))
        .orderBy("position", "asc"),
    ),
  },
  steps: {
    /** One lane: a track's steps in a pattern, in order. */
    lane: defineQuery(z.object({ trackId: z.string(), patternId: z.string() }), ({ ctx, args }) =>
      zql.steps
        .where("track_id", args.trackId)
        .where("pattern_id", args.patternId)
        .whereExists("session", (s) => s.where(isMember(ctx)))
        .orderBy("position", "asc"),
    ),
  },
  transport: {
    latest: defineQuery(sessionArgs, ({ ctx, args }) =>
      zql.transport_observations
        .where("session_id", args.sessionId)
        .whereExists("session", (s) => s.where(isMember(ctx)))
        .orderBy("observed_at", "desc")
        .limit(1),
    ),
  },
  presence: {
    bySession: defineQuery(sessionArgs, ({ ctx, args }) =>
      zql.presence
        .where("session_id", args.sessionId)
        .whereExists("session", (s) => s.where(isMember(ctx)))
        .related("profile"),
    ),
  },
});
