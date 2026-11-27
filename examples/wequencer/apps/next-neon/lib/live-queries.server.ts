import "server-only";
import { rawSql } from "@neon/realtime/server";
import { z } from "zod";

/**
 * Who can read what in Wequencer (writes are checked in mutators.server.ts):
 *
 * - A session is visible to its creator and its members; its members list,
 *   tracks, patterns, steps, transport and presence to its members.
 * - A profile is visible to its owner, and to members of a session where it
 *   has shown presence (through the presence query's `profile`).
 *
 * Realtime authorizes the query, not the connection: each query is built for
 * the signed-in user ($1) and sealed. The membership checks are part of the
 * live query, so they stay live.
 */
const isMember = (sessionId: string) =>
  `EXISTS (SELECT 1 FROM session_members sm WHERE sm.session_id = ${sessionId} AND sm.member_author = $1)`;
const canRead = (sessionId: string) =>
  `(${isMember(sessionId)} OR EXISTS (SELECT 1 FROM sessions cs WHERE cs.id = ${sessionId} AND cs.created_by = $1))`;

const sessionArgs = z.object({ sessionId: z.string() });
const SESSION = `s.id, s.title, s.tempo_bpm, s.created_by, s.created_at`;

export const liveQueries: Record<string, (me: string, args: unknown) => ReturnType<typeof rawSql>> = {
  "profiles.mine": (me) => rawSql(`SELECT id, author, "displayName" FROM profiles WHERE author = $1`, [me]),

  "sessions.mine": (me) =>
    rawSql(`SELECT ${SESSION} FROM sessions s WHERE ${canRead("s.id")} ORDER BY s.created_at DESC, s.id`, [me]),

  "sessions.one": (me, args) => {
    const { sessionId } = sessionArgs.parse(args);
    return rawSql(`SELECT ${SESSION} FROM sessions s WHERE s.id = $2 AND ${canRead("$2")}`, [me, sessionId]);
  },

  "members.bySession": (me, args) => {
    const { sessionId } = sessionArgs.parse(args);
    return rawSql(
      `SELECT id, session_id, member_author, role FROM session_members WHERE session_id = $2 AND ${canRead("$2")}`,
      [me, sessionId],
    );
  },

  "tracks.bySession": (me, args) => {
    const { sessionId } = sessionArgs.parse(args);
    return rawSql(
      `SELECT id, session_id, position, name, color, instrument, volume, muted, solo FROM tracks
       WHERE session_id = $2 AND ${isMember("$2")} ORDER BY position ASC, id`,
      [me, sessionId],
    );
  },

  "patterns.bySession": (me, args) => {
    const { sessionId } = sessionArgs.parse(args);
    return rawSql(
      `SELECT id, session_id, position, name, length FROM patterns
       WHERE session_id = $2 AND ${isMember("$2")} ORDER BY position ASC, id`,
      [me, sessionId],
    );
  },

  /** One lane: a track's steps in a pattern, in order. */
  "steps.lane": (me, args) => {
    const { trackId, patternId } = z.object({ trackId: z.string(), patternId: z.string() }).parse(args);
    return rawSql(
      `SELECT st.id, st.session_id, st.track_id, st.pattern_id, st.position, st.enabled, st.velocity, st.probability
       FROM steps st WHERE st.track_id = $2 AND st.pattern_id = $3 AND ${isMember("st.session_id")}
       ORDER BY st.position ASC, st.id`,
      [me, trackId, patternId],
    );
  },

  "transport.latest": (me, args) => {
    const { sessionId } = sessionArgs.parse(args);
    return rawSql(
      `SELECT id, session_id, playing, bar, observed_at, tempo_bpm, pattern_id FROM transport_observations
       WHERE session_id = $2 AND ${isMember("$2")} ORDER BY observed_at DESC, id DESC LIMIT 1`,
      [me, sessionId],
    );
  },

  "presence.bySession": (me, args) => {
    const { sessionId } = sessionArgs.parse(args);
    return rawSql(
      `SELECT pr.id, pr.session_id, pr.profile_id, pr.cursor_step, pr.heartbeat_at,
         jsonb_build_object('id', p.id, 'author', p.author, 'displayName', p."displayName") AS profile
       FROM presence pr JOIN profiles p ON p.id = pr.profile_id
       WHERE pr.session_id = $2 AND ${isMember("$2")}`,
      [me, sessionId],
    );
  },
};
