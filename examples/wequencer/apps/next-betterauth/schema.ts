import { schema as s } from "jazz-tools";
import { schema as betterAuthSchema } from "./schema-better-auth/schema";

const schema = {
  ...betterAuthSchema,
  profiles: s.table(
    {
      // Stable Jazz account ownership. Provider identities stay inside Better
      // Auth and are only used while enrolling this account.
      author: s.uuid(),
      displayName: s.string(),
    },
    { presenceViaProfile: s.reverse("presence", "profile") },
  ),
  sessions: s.table(
    {
      title: s.string(),
      tempo_bpm: s.int(),
      loop_steps: s.int(),
    },
    {
      session_membersViaSession: s.reverse("session_members", "session"),
      tracksViaSession: s.reverse("tracks", "session"),
      transport_observationsViaSession: s.reverse("transport_observations", "session"),
      presenceViaSession: s.reverse("presence", "session"),
    },
  ),
  session_members: s
    .table(
      {
        session_id: s.uuid(),
        member_author: s.uuid(),
        role: s.enum("owner", "editor", "viewer"),
      },
      { session: s.rel("sessions", "session_id") },
    )
    .indexOnly(["session_id", "member_author"]),
  // These per-column indexes support the parent filter and ordered subscriptions
  // below. They are not one compound index. Position is ordinary application data: concurrent reordering uses
  // Jazz's normal row merge behavior, rather than a hidden list CRDT.
  tracks: s
    .table(
      {
        session_id: s.uuid(),
        position: s.int(),
        name: s.string(),
        color: s.string(),
      },
      { session: s.rel("sessions", "session_id"), stepsViaTrack: s.reverse("steps", "track") },
    )
    .indexOnly(["session_id", "position"]),
  steps: s
    .table(
      {
        track_id: s.uuid(),
        position: s.int(),
        enabled: s.boolean(),
        velocity: s.int(),
        probability: s.int(),
      },
      { track: s.rel("tracks", "track_id") },
    )
    .indexOnly(["track_id", "position"]),
  // A transport receipt is deliberately just a row with timing fields. It
  // makes a collaborator's UI state observable; it is not a synchronized
  // audio clock or an authority for scheduling playback.
  transport_observations: s
    .table(
      {
        session_id: s.uuid(),
        playing: s.boolean(),
        bar: s.int(),
        observed_at: s.timestamp(),
      },
      { session: s.rel("sessions", "session_id") },
    )
    .indexOnly(["session_id", "observed_at"]),
  presence: s
    .table(
      {
        session_id: s.uuid(),
        profile_id: s.uuid(),
        cursor_step: s.int(),
        heartbeat_at: s.timestamp(),
      },
      { session: s.rel("sessions", "session_id"), profile: s.rel("profiles", "profile_id") },
    )
    .indexOnly(["session_id", "heartbeat_at"]),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
export type Session = s.RowOf<typeof app.sessions>;
export type Track = s.RowOf<typeof app.tracks>;
export type Step = s.RowOf<typeof app.steps>;
