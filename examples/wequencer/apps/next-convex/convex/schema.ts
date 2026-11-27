import { defineSchema, defineTable } from "convex/server";
import { authTables } from "@convex-dev/auth/server";
import { v } from "convex/values";

/**
 * Wequencer: the Jazz app's tables. `author` and `member_author` hold the
 * Convex Auth user id; who may read or write what is checked inside each
 * function (see lib/access.ts).
 */
export const role = v.union(v.literal("owner"), v.literal("editor"), v.literal("viewer"));
export const instrument = v.union(
  v.literal("kick"),
  v.literal("snare"),
  v.literal("clap"),
  v.literal("closed_hat"),
  v.literal("open_hat"),
  v.literal("tom"),
  v.literal("bass"),
  v.literal("lead"),
);

export default defineSchema({
  ...authTables,
  profiles: defineTable({
    author: v.id("users"),
    displayName: v.string(),
  }).index("by_author", ["author"]),
  sessions: defineTable({
    title: v.string(),
    // The starting tempo; the newest transport observation overrides it.
    tempo_bpm: v.number(),
    // The creator administers the session (Jazz's immutable `$createdBy`).
    creator: v.id("users"),
  }).index("by_creator", ["creator"]),
  session_members: defineTable({
    session_id: v.id("sessions"),
    member_author: v.id("users"),
    role,
  })
    .index("by_session_author", ["session_id", "member_author"])
    .index("by_author", ["member_author"]),
  // A track is one instrument voice shared by every pattern in the session.
  // Mix settings are ordinary shared columns, so bandmates hear the same mix.
  tracks: defineTable({
    session_id: v.id("sessions"),
    position: v.number(),
    name: v.string(),
    color: v.string(),
    instrument,
    volume: v.number(),
    muted: v.boolean(),
    solo: v.boolean(),
  }).index("by_session_position", ["session_id", "position"]),
  // A pattern is a named sequence of up to 64 steps; `length` windows the
  // steps that play.
  patterns: defineTable({
    session_id: v.id("sessions"),
    position: v.number(),
    name: v.string(),
    length: v.number(),
  }).index("by_session_position", ["session_id", "position"]),
  // Steps are sparse: a pad without a row is off. `steps.set` finds the row
  // for (track, pattern, position) inside its transaction, so bandmates who
  // toggle the same new pad at once still end up with one row.
  steps: defineTable({
    session_id: v.id("sessions"),
    track_id: v.id("tracks"),
    pattern_id: v.id("patterns"),
    position: v.number(),
    enabled: v.boolean(),
    velocity: v.number(),
    probability: v.number(),
  }).index("by_track_pattern_position", ["track_id", "pattern_id", "position"]),
  // The newest observation says whether the band is playing, which pattern,
  // at what tempo, and which step (`bar`) was sounding at `observed_at`.
  transport_observations: defineTable({
    session_id: v.id("sessions"),
    playing: v.boolean(),
    bar: v.number(),
    observed_at: v.number(),
    tempo_bpm: v.number(),
    pattern_id: v.optional(v.id("patterns")),
  }).index("by_session_observed", ["session_id", "observed_at"]),
  presence: defineTable({
    session_id: v.id("sessions"),
    profile_id: v.id("profiles"),
    cursor_step: v.number(),
    heartbeat_at: v.number(),
  }).index("by_session_profile", ["session_id", "profile_id"]),
});
