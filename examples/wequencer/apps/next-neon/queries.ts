import type { Pattern, Presence, Profile, Session, SessionMember, Step, Track, TransportObservation } from "./schema";

/**
 * The live queries the UI subscribes to, by name. The server builds the SQL
 * for each (lib/live-queries.server.ts), scoped to the signed-in user, and
 * seals it; the browser only names the query and its arguments.
 */
export type QueryDef<Row, Result> = { name: string; args: Record<string, unknown>; one: boolean; _?: [Row, Result] };
const many =
  <Row, Args extends Record<string, unknown> = Record<string, never>>(name: string) =>
  (args?: Args): QueryDef<Row, Row[]> => ({ name, args: args ?? {}, one: false });
const one =
  <Row, Args extends Record<string, unknown> = Record<string, never>>(name: string) =>
  (args?: Args): QueryDef<Row, Row | undefined> => ({ name, args: args ?? {}, one: true });

type SessionArgs = { sessionId: string };

export const queries = {
  profiles: { mine: one<Profile>("profiles.mine") },
  sessions: { mine: many<Session>("sessions.mine"), one: one<Session, SessionArgs>("sessions.one") },
  members: { bySession: many<SessionMember, SessionArgs>("members.bySession") },
  tracks: { bySession: many<Track, SessionArgs>("tracks.bySession") },
  patterns: { bySession: many<Pattern, SessionArgs>("patterns.bySession") },
  /** One lane: a track's steps in a pattern, in order. */
  steps: { lane: many<Step, { trackId: string; patternId: string }>("steps.lane") },
  transport: { latest: many<TransportObservation, SessionArgs>("transport.latest") },
  presence: { bySession: many<Presence & { profile: Profile | null }, SessionArgs>("presence.bySession") },
};
