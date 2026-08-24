import { schema as s } from "jazz-tools";

const schema = {
  profiles: s.table({
    user_id: s.string(),
    display_name: s.string(),
  }),
  sessions: s.table({
    title: s.string(),
    tempo_bpm: s.int(),
    loop_steps: s.int(),
  }),
  session_members: s
    .table({
      session_id: s.ref("sessions"),
      user_id: s.string(),
      role: s.enum("owner", "editor", "viewer"),
    })
    .indexOnly(["session_id", "user_id"]),
  // These compound indexes mirror the ordered, parent-scoped subscriptions
  // below. Position is ordinary application data: concurrent reordering uses
  // Jazz's normal row merge behavior, rather than a hidden list CRDT.
  tracks: s
    .table({
      session_id: s.ref("sessions"),
      position: s.int(),
      name: s.string(),
      color: s.string(),
    })
    .indexOnly(["session_id", "position"]),
  steps: s
    .table({
      track_id: s.ref("tracks"),
      position: s.int(),
      enabled: s.boolean(),
      velocity: s.int(),
      probability: s.int(),
    })
    .indexOnly(["track_id", "position"]),
  // A transport receipt is deliberately just a row with timing fields. It
  // makes a collaborator's UI state observable; it is not a synchronized
  // audio clock or an authority for scheduling playback.
  transport_observations: s
    .table({
      session_id: s.ref("sessions"),
      playing: s.boolean(),
      bar: s.int(),
      observed_at: s.timestamp(),
    })
    .indexOnly(["session_id", "observed_at"]),
  presence: s
    .table({
      session_id: s.ref("sessions"),
      profile_id: s.ref("profiles"),
      cursor_step: s.int(),
      heartbeat_at: s.timestamp(),
    })
    .indexOnly(["session_id", "heartbeat_at"]),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
export type Session = s.RowOf<typeof app.sessions>;
export type Track = s.RowOf<typeof app.tracks>;
export type Step = s.RowOf<typeof app.steps>;
