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
  session_members: s.table({
    session_id: s.ref("sessions"),
    user_id: s.string(),
    role: s.enum("owner", "editor", "viewer"),
  }),
  tracks: s.table({
    session_id: s.ref("sessions"),
    position: s.int(),
    name: s.string(),
    color: s.string(),
  }),
  steps: s.table({
    track_id: s.ref("tracks"),
    position: s.int(),
    enabled: s.boolean(),
    velocity: s.int(),
    probability: s.int(),
  }),
  transport_observations: s.table({
    session_id: s.ref("sessions"),
    playing: s.boolean(),
    bar: s.int(),
    observed_at: s.timestamp(),
  }),
  presence: s.table({
    session_id: s.ref("sessions"),
    profile_id: s.ref("profiles"),
    cursor_step: s.int(),
    heartbeat_at: s.timestamp(),
  }),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
export type Session = s.RowOf<typeof app.sessions>;
export type Track = s.RowOf<typeof app.tracks>;
export type Step = s.RowOf<typeof app.steps>;
