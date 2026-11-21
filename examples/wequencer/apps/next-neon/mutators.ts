import type { MutationDef, Overlay } from "@/lib/live";
import type { Pattern, Session, SessionMember, Step, Track, TransportObservation } from "./schema";

/**
 * Wequencer's writes, as the components call them: `db.mutate(mutators.x.y(args))`.
 * Each names a server write handler (lib/mutators.server.ts, where the checks
 * are) and says how it changes the live queries at once, until its
 * transaction has synced (Neon's optimistic-mutations pattern).
 */
type Rows = Parameters<Overlay[string]>[0];
type Row = Rows[number];
const def = (name: string, args: unknown, overlay?: (me: string) => Overlay): MutationDef => ({ name, args, overlay });

const has = (rows: Rows, id: string) => rows.some((row) => row.id === id);
const without = (rows: Rows, match: (row: Row) => boolean) => (rows.some(match) ? rows.filter((row) => !match(row)) : rows);
const patch = (rows: Rows, id: string, change: Record<string, unknown>) =>
  has(rows, id) ? rows.map((row) => (row.id === id ? { ...row, ...change } : row)) : rows;
const byPosition = (a: Row, b: Row) => (a.position as number) - (b.position as number);
const inSession = (sessionId: string, change: (rows: Rows) => Rows) => (rows: Rows, args: Record<string, unknown>) =>
  args.sessionId === sessionId ? change(rows) : rows;
const addTo = (row: Row) => (rows: Rows) => (has(rows, row.id as string) ? rows : [...rows, row].sort(byPosition));

/** A pad, by its derived id, in its lane. */
function setStep(step: Step): Overlay {
  return {
    "steps.lane": (rows, args) => {
      if (args.trackId !== step.track_id || args.patternId !== step.pattern_id) return rows;
      return has(rows, step.id) ? patch(rows, step.id, step) : [...rows, step].sort(byPosition);
    },
  };
}

const NEW_TRACK = { volume: 80, muted: false, solo: false };
type TrackFields = Pick<Track, "position" | "name" | "color" | "instrument">;

export const mutators = {
  sessions: {
    /** A session, its owner membership, first pattern, starter tracks and pads and transport, together. */
    create: (args: {
      id: string;
      title: string;
      tempo_bpm: number;
      memberId: string;
      pattern: { id: string; length: number };
      tracks: (TrackFields & { id: string })[];
      steps: Step[];
      observationId: string;
    }) =>
      def("sessions.create", args, (me) => {
        const now = Date.now();
        const session: Session = { id: args.id, title: args.title, tempo_bpm: args.tempo_bpm, created_by: me, created_at: now };
        const member: SessionMember = { id: args.memberId, session_id: args.id, member_author: me, role: "owner" };
        const pattern: Pattern = { id: args.pattern.id, session_id: args.id, position: 0, name: "Pattern 1", length: args.pattern.length };
        const tracks: Track[] = args.tracks.map((track) => ({ ...track, ...NEW_TRACK, session_id: args.id }));
        const observation: TransportObservation = {
          id: args.observationId,
          session_id: args.id,
          playing: false,
          bar: 0,
          observed_at: now,
          tempo_bpm: args.tempo_bpm,
          pattern_id: args.pattern.id,
        };
        const fresh = (rows: Rows, value: Rows) => (rows.length ? rows : value);
        return {
          "sessions.mine": (rows) => (has(rows, session.id) ? rows : [session, ...rows]),
          "sessions.one": inSession(args.id, (rows) => fresh(rows, [session])),
          "members.bySession": inSession(args.id, (rows) => fresh(rows, [member])),
          "patterns.bySession": inSession(args.id, (rows) => fresh(rows, [pattern])),
          "tracks.bySession": inSession(args.id, (rows) => fresh(rows, tracks)),
          "transport.latest": inSession(args.id, (rows) => fresh(rows, [observation])),
          "steps.lane": (rows, query) =>
            query.patternId !== args.pattern.id || rows.length
              ? rows
              : args.steps.filter((step) => step.track_id === query.trackId).sort(byPosition),
        };
      }),
  },

  members: {
    add: (args: { id: string; session_id: string; member_author: string; role: "editor" | "viewer" }) =>
      def("members.add", args, () => ({ "members.bySession": inSession(args.session_id, addTo(args)) })),
    remove: (args: { id: string }) =>
      def("members.remove", args, () => ({ "members.bySession": (rows) => without(rows, (row) => row.id === args.id) })),
  },

  tracks: {
    /** A new track starts silent; its pads exist as soon as someone presses them. */
    add: (args: TrackFields & { id: string; session_id: string }) =>
      def("tracks.add", args, () => ({ "tracks.bySession": inSession(args.session_id, addTo({ ...args, ...NEW_TRACK })) })),
    update: (args: { id: string; name?: string; instrument?: Track["instrument"]; volume?: number; muted?: boolean; solo?: boolean }) =>
      def("tracks.update", args, () => {
        const change = Object.fromEntries(Object.entries(args).filter(([key, value]) => key !== "id" && value !== undefined));
        return { "tracks.bySession": (rows) => patch(rows, args.id, change) };
      }),
    /** Removes a track and every step it has, in any pattern, together. */
    remove: (args: { id: string }) =>
      def("tracks.remove", args, () => ({ "tracks.bySession": (rows) => without(rows, (row) => row.id === args.id) })),
  },

  patterns: {
    add: (args: { id: string; session_id: string; position: number; name: string; length: number }) =>
      def("patterns.add", args, () => ({ "patterns.bySession": inSession(args.session_id, addTo(args)) })),
    setLength: (args: { id: string; length: number }) =>
      def("patterns.setLength", args, () => ({ "patterns.bySession": (rows) => patch(rows, args.id, { length: args.length }) })),
  },

  steps: {
    /** Upserts a pad by its derived id: concurrent first presses write the same row. */
    set: (args: Step) => def("steps.set", args, () => setStep(args)),
  },

  transport: {
    /** Appends an observation; the newest one is the transport's state. */
    observe: (args: TransportObservation) =>
      def("transport.observe", args, () => ({ "transport.latest": inSession(args.session_id, () => [args]) })),
  },

  presence: {
    /** Publishes (or refreshes) this member's presence for their own profile. */
    beat: (args: { id: string; session_id: string; profile_id: string; cursor_step: number; heartbeat_at: number }) =>
      def("presence.beat", args),
  },
};
