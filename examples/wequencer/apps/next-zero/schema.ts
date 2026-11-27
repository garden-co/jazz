import {
  boolean,
  createBuilder,
  createSchema,
  enumeration,
  number,
  relationships,
  string,
  table,
  type Row,
} from "@rocicorp/zero";

// Wequencer's synced tables (Postgres: db/001_init.sql). Better Auth's tables
// live in Postgres' "auth" schema and are not synced.

export type MemberRole = "owner" | "editor" | "viewer";
export type Instrument = "kick" | "snare" | "clap" | "closed_hat" | "open_hat" | "tom" | "bass" | "lead";

const profiles = table("profiles")
  .columns({ id: string(), author: string(), displayName: string() })
  .primaryKey("id");

const sessions = table("sessions")
  .columns({
    id: string(),
    title: string(),
    // The starting tempo; the newest transport observation overrides it.
    tempo_bpm: number(),
    // The creator administers the session, whatever their membership row says.
    created_by: string(),
    created_at: number(),
  })
  .primaryKey("id");

const session_members = table("session_members")
  .columns({ id: string(), session_id: string(), member_author: string(), role: enumeration<MemberRole>() })
  .primaryKey("id");

// A track is one instrument voice shared by every pattern in the session.
// Mix settings are ordinary shared columns, so bandmates hear the same mix.
const tracks = table("tracks")
  .columns({
    id: string(),
    session_id: string(),
    position: number(),
    name: string(),
    color: string(),
    instrument: enumeration<Instrument>(),
    volume: number(),
    muted: boolean(),
    solo: boolean(),
  })
  .primaryKey("id");

// A pattern is a named sequence of up to 64 steps; `length` windows the
// steps that play.
const patterns = table("patterns")
  .columns({ id: string(), session_id: string(), position: number(), name: string(), length: number() })
  .primaryKey("id");

// Steps are sparse: a pad without a row is off. The app derives a step's row
// id from (track, pattern, position) and upserts it.
const steps = table("steps")
  .columns({
    id: string(),
    session_id: string(),
    track_id: string(),
    pattern_id: string(),
    position: number(),
    enabled: boolean(),
    velocity: number(),
    probability: number(),
  })
  .primaryKey("id");

// The newest observation says whether the band is playing, which pattern, at
// what tempo, and which step was sounding at `observed_at`.
const transport_observations = table("transport_observations")
  .columns({
    id: string(),
    session_id: string(),
    playing: boolean(),
    bar: number(),
    observed_at: number(),
    tempo_bpm: number(),
    pattern_id: string().optional(),
  })
  .primaryKey("id");

const presence = table("presence")
  .columns({ id: string(), session_id: string(), profile_id: string(), cursor_step: number(), heartbeat_at: number() })
  .primaryKey("id");

const sessionRelationships = relationships(sessions, ({ many }) => ({
  members: many({ sourceField: ["id"], destSchema: session_members, destField: ["session_id"] }),
}));
// Every session-scoped table has a `session` relationship, for the membership checks.
const toSession = { sourceField: ["session_id"], destSchema: sessions, destField: ["id"] } as const;
const tracksSession = relationships(tracks, ({ one }) => ({ session: one(toSession) }));
const patternsSession = relationships(patterns, ({ one }) => ({ session: one(toSession) }));
const stepsSession = relationships(steps, ({ one }) => ({ session: one(toSession) }));
const transportSession = relationships(transport_observations, ({ one }) => ({ session: one(toSession) }));
const membersSession = relationships(session_members, ({ one }) => ({ session: one(toSession) }));
const presenceRelationships = relationships(presence, ({ one }) => ({
  session: one(toSession),
  profile: one({ sourceField: ["profile_id"], destSchema: profiles, destField: ["id"] }),
}));

export const schema = createSchema({
  tables: [profiles, sessions, session_members, tracks, patterns, steps, transport_observations, presence],
  relationships: [
    sessionRelationships,
    tracksSession,
    patternsSession,
    stepsSession,
    transportSession,
    membersSession,
    presenceRelationships,
  ],
  enableLegacyQueries: false,
  enableLegacyMutators: false,
});

export const zql = createBuilder(schema);
export type Schema = typeof schema;

/** Who is asking: the Better Auth user from the session cookie. */
export type AuthData = { userID: string };

declare module "@rocicorp/zero" {
  interface DefaultTypes {
    schema: typeof schema;
    context: AuthData | undefined;
  }
}

export type Session = Row<typeof schema.tables.sessions>;
export type Track = Row<typeof schema.tables.tracks>;
export type Step = Row<typeof schema.tables.steps>;
export type Pattern = Row<typeof schema.tables.patterns>;
