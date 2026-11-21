import { i } from "@instantdb/react";

export const INSTRUMENT_VALUES = [
  "kick",
  "snare",
  "clap",
  "closed_hat",
  "open_hat",
  "tom",
  "bass",
  "lead",
] as const;
export type Instrument = (typeof INSTRUMENT_VALUES)[number];
export type MemberRole = "owner" | "editor" | "viewer";

const _schema = i.schema({
  entities: {
    $users: i.entity({
      email: i.string().unique().indexed().optional(),
      // The name given at sign-up: the profile's display name.
      name: i.string().optional(),
    }),
    // Email + password sign-in (lib/instant.server.ts). Admin-only: no client rule allows it.
    credentials: i.entity({
      email: i.string().unique().indexed(),
      passwordHash: i.string(),
    }),
    profiles: i.entity({
      author: i.string().unique().indexed(),
      displayName: i.string(),
    }),
    sessions: i.entity({
      title: i.string(),
      // The starting tempo; the newest transport observation overrides it.
      tempo_bpm: i.number(),
      // The creator administers the session (Jazz's immutable `$createdBy`).
      creator: i.string().indexed(),
      createdAt: i.date().indexed(),
    }),
    session_members: i.entity({
      member_author: i.string().indexed(),
      role: i.string<MemberRole>().indexed(),
      // `member_author` when the role may edit (owner, editor), else null. Rules
      // can't filter a linked list by role, so edit rights read this list.
      editor_author: i.string().indexed().optional(),
    }),
    // A track is one instrument voice shared by every pattern in the session.
    // Mix settings are ordinary shared attributes, so bandmates hear the same mix.
    tracks: i.entity({
      position: i.number().indexed(),
      name: i.string(),
      color: i.string(),
      instrument: i.string<Instrument>(),
      volume: i.number(),
      muted: i.boolean(),
      solo: i.boolean(),
    }),
    // A pattern is a named sequence of up to 64 steps; `length` windows the
    // steps that play.
    patterns: i.entity({
      position: i.number().indexed(),
      name: i.string(),
      length: i.number(),
    }),
    // Steps are sparse: a pad without a row is off. The app derives a step's
    // id from (track, pattern, position) and upserts it, so bandmates who
    // toggle the same new pad at once write the same row instead of two.
    steps: i.entity({
      position: i.number().indexed(),
      enabled: i.boolean(),
      velocity: i.number(),
      probability: i.number(),
    }),
    // The newest observation says whether the band is playing, which pattern,
    // at what tempo, and which step (`bar`) was sounding at `observed_at`.
    transport_observations: i.entity({
      playing: i.boolean(),
      bar: i.number(),
      observed_at: i.date().indexed(),
      tempo_bpm: i.number(),
    }),
    presence: i.entity({
      cursor_step: i.number(),
      heartbeat_at: i.date().indexed(),
    }),
  },
  links: {
    membersSession: {
      forward: {
        on: "session_members",
        has: "one",
        label: "session",
        required: true,
        onDelete: "cascade",
      },
      reverse: { on: "sessions", has: "many", label: "members" },
    },
    tracksSession: {
      forward: { on: "tracks", has: "one", label: "session", required: true, onDelete: "cascade" },
      reverse: { on: "sessions", has: "many", label: "tracks" },
    },
    patternsSession: {
      forward: {
        on: "patterns",
        has: "one",
        label: "session",
        required: true,
        onDelete: "cascade",
      },
      reverse: { on: "sessions", has: "many", label: "patterns" },
    },
    stepsTrack: {
      forward: { on: "steps", has: "one", label: "track", required: true, onDelete: "cascade" },
      reverse: { on: "tracks", has: "many", label: "steps" },
    },
    stepsPattern: {
      forward: { on: "steps", has: "one", label: "pattern", required: true, onDelete: "cascade" },
      reverse: { on: "patterns", has: "many", label: "steps" },
    },
    observationsSession: {
      forward: {
        on: "transport_observations",
        has: "one",
        label: "session",
        required: true,
        onDelete: "cascade",
      },
      reverse: { on: "sessions", has: "many", label: "observations" },
    },
    observationsPattern: {
      forward: { on: "transport_observations", has: "one", label: "pattern" },
      reverse: { on: "patterns", has: "many", label: "observations" },
    },
    presenceSession: {
      forward: {
        on: "presence",
        has: "one",
        label: "session",
        required: true,
        onDelete: "cascade",
      },
      reverse: { on: "sessions", has: "many", label: "presence" },
    },
    presenceProfile: {
      forward: {
        on: "presence",
        has: "one",
        label: "profile",
        required: true,
        onDelete: "cascade",
      },
      reverse: { on: "profiles", has: "many", label: "presence" },
    },
  },
});

// This helps TypeScript display better IntelliSense
type _AppSchema = typeof _schema;
interface AppSchema extends _AppSchema {}
const schema: AppSchema = _schema;

export type { AppSchema };
export default schema;
