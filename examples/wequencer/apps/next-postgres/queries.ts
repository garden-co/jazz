// The app's live queries: where each one is fetched from, and which tables
// under which topic it depends on (the server's change events name them).
import type { Live } from "@/lib/data/live";
import type { Pattern, Presence, Profile, Session, SessionMember, Step, Track, TransportObservation } from "@/schema";

const topic = (sessionId: string) => `session:${sessionId}`;
const inSession = <T,>(sessionId: string, what: string, path: string, tables: string[], empty: T): Live<T> => ({
  key: ["session", sessionId, what],
  path: `/api/sessions/${sessionId}${path}`,
  empty,
  watch: () => [[topic(sessionId), tables]],
});

export type PresenceWithProfile = Presence & { profile: Profile | null };

export const queries = {
  profiles: {
    mine: (): Live<Profile | null> => ({ key: ["profile"], path: "/api/profile", empty: null, watch: () => [["me", ["profiles"]]] }),
  },
  sessions: {
    mine: (): Live<Session[]> => ({ key: ["sessions"], path: "/api/sessions", empty: [], watch: () => [["me", ["sessions"]]] }),
    one: ({ sessionId }: { sessionId: string }) =>
      inSession<Session | null>(sessionId, "session", "", ["sessions", "session_members"], null),
  },
  members: {
    bySession: ({ sessionId }: { sessionId: string }) =>
      inSession<SessionMember[]>(sessionId, "members", "/members", ["session_members"], []),
  },
  tracks: {
    bySession: ({ sessionId }: { sessionId: string }) => inSession<Track[]>(sessionId, "tracks", "/tracks", ["tracks"], []),
  },
  patterns: {
    bySession: ({ sessionId }: { sessionId: string }) =>
      inSession<Pattern[]>(sessionId, "patterns", "/patterns", ["patterns"], []),
  },
  steps: {
    /**
     * One lane: a track's steps in a pattern, in order. All lanes share one
     * request for the pattern's steps and each selects its own track.
     */
    lane: ({ sessionId, trackId, patternId }: { sessionId: string; trackId: string; patternId: string }): Live<Step[]> => ({
      ...inSession<Step[]>(sessionId, "steps", `/patterns/${patternId}/steps`, ["steps"], []),
      key: ["session", sessionId, "steps", patternId],
      select: (steps: Step[]) => steps.filter((step) => step.track_id === trackId),
    }),
  },
  transport: {
    latest: ({ sessionId }: { sessionId: string }) =>
      inSession<TransportObservation[]>(sessionId, "transport", "/transport", ["transport_observations"], []),
  },
  presence: {
    bySession: ({ sessionId }: { sessionId: string }) =>
      inSession<PresenceWithProfile[]>(sessionId, "presence", "/presence", ["presence", "profiles"], []),
  },
};
