import { schema as s } from "jazz-tools";
import { tables as betterAuthSchema } from "./schema-better-auth/schema";
import { permissions as betterAuthPermissions } from "./schema-better-auth/schema";

export const schema = s.defineSchema({
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
      // The starting tempo; the newest transport observation overrides it.
      tempo_bpm: s.int(),
    },
    {
      session_membersViaSession: s.reverse("session_members", "session"),
      tracksViaSession: s.reverse("tracks", "session"),
      patternsViaSession: s.reverse("patterns", "session"),
      stepsViaSession: s.reverse("steps", "session"),
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
  // A track is one instrument voice shared by every pattern in the session.
  // Mix settings are ordinary shared columns, so bandmates hear the same mix.
  tracks: s
    .table(
      {
        session_id: s.uuid(),
        position: s.int(),
        name: s.string(),
        color: s.string(),
        instrument: s
          .enum("kick", "snare", "clap", "closed_hat", "open_hat", "tom", "bass", "lead")
          .default("kick"),
        volume: s.int().default(80),
        muted: s.boolean().default(false),
        solo: s.boolean().default(false),
      },
      { session: s.rel("sessions", "session_id"), stepsViaTrack: s.reverse("steps", "track") },
    )
    .indexOnly(["session_id", "position"]),
  // A pattern is a named sequence of up to 64 steps; `length` windows the
  // steps that play.
  patterns: s
    .table(
      {
        session_id: s.uuid(),
        position: s.int(),
        name: s.string(),
        length: s.int(),
      },
      { session: s.rel("sessions", "session_id"), stepsViaPattern: s.reverse("steps", "pattern") },
    )
    .indexOnly(["session_id", "position"]),
  // Steps are sparse: a pad without a row is off. The app derives a step's
  // row id from (track, pattern, position) and upserts it, so bandmates who
  // toggle the same new pad at once write the same row instead of two, and a
  // track added concurrently with a pattern still gets working pads.
  // `session_id` lets permissions check that the track and the pattern both
  // belong to the same session.
  steps: s
    .table(
      {
        session_id: s.uuid(),
        track_id: s.uuid(),
        pattern_id: s.uuid(),
        position: s.int(),
        enabled: s.boolean(),
        velocity: s.int(),
        probability: s.int(),
      },
      {
        session: s.rel("sessions", "session_id"),
        track: s.rel("tracks", "track_id"),
        pattern: s.rel("patterns", "pattern_id"),
      },
    )
    .indexOnly(["track_id", "pattern_id", "position"]),
  // A transport receipt is deliberately just a row with timing fields: the
  // newest one says whether the band is playing, which pattern, at what
  // tempo, and which step (`bar`) was sounding at `observed_at`. Each client
  // extrapolates the playhead from its own wall clock, so playback is
  // roughly aligned rather than sample-accurate.
  transport_observations: s
    .table(
      {
        session_id: s.uuid(),
        playing: s.boolean(),
        bar: s.int(),
        observed_at: s.timestamp(),
        tempo_bpm: s.int().default(120),
        pattern_id: s.uuid().optional(),
      },
      { session: s.rel("sessions", "session_id"), pattern: s.rel("patterns", "pattern_id") },
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
});

const { definePermissions, exists, allowedTo, session, anyOf, allOf } = schema;

const isSessionMember = (session_id: s.Uuid) =>
  exists.session_members({ session_id, member_author: session.user });

const hasSessionRole = (session_id: s.Uuid, role: "owner" | "editor" | "viewer") =>
  exists.session_members({ session_id, member_author: session.user, role });

const canEditSession = (session_id: s.Uuid) =>
  anyOf(hasSessionRole(session_id, "editor"), hasSessionRole(session_id, "owner"));

// Administrative authority is the immutable system author of the session
// row. `owner` is only the creator's initial collaboration role: deleting
// or replacing that mutable membership row must not transfer or revoke the
// creator's ability to administer the session.
const createdByMe = { "$createdBy.account": session.user };

const mine = { author: session.user };

// A step's track and pattern must both belong to the step's session, and
// the writer must be able to edit that session.
const stepFitsSession = (step: s.Row<typeof schema, "steps">) =>
  allOf(
    canEditSession(step.session_id),
    step.track.where({ session_id: step.session_id }),
    step.pattern.where({ session_id: step.session_id }),
  );

export const permissions = definePermissions({
  ...betterAuthPermissions,
  patterns: {
    read: (pattern) => isSessionMember(pattern.session_id),
    insert: (pattern) => canEditSession(pattern.session_id),
    update: (pattern) => canEditSession(pattern.session_id),
    delete: (pattern) => pattern.session.where(createdByMe),
  },
  presence: {
    read: (presence) => isSessionMember(presence.session_id),
    insert: (presence) =>
      allOf(isSessionMember(presence.session_id), allowedTo.update(presence.profile)),
    update: (presence) =>
      allOf(isSessionMember(presence.session_id), allowedTo.update(presence.profile)),
    delete: createdByMe,
  },
  profiles: {
    // Bandmates see each other's display name once that profile has shown
    // presence in a session they can both read. Presence stays advisory: it
    // only reveals a name, never grants a write.
    read: (profile) => anyOf(mine, allowedTo.read(profile.presenceViaProfile)),
    insert: mine,
    update: mine,
    delete: mine,
  },
  session_members: {
    read: (member) => allowedTo.read(member.session),
    insert: (member) => allowedTo.update(member.session),
    update: false,
    delete: (member) => allowedTo.update(member.session),
  },
  sessions: {
    read: (sessionRow) => anyOf(createdByMe, isSessionMember(sessionRow.id)),
    insert: true,
    update: createdByMe,
    delete: createdByMe,
  },
  steps: {
    read: (step) => isSessionMember(step.session_id),
    insert: stepFitsSession,
    update: stepFitsSession,
    delete: (step) => canEditSession(step.session_id),
  },
  tracks: {
    read: (track) => isSessionMember(track.session_id),
    insert: (track) => canEditSession(track.session_id),
    update: (track) => canEditSession(track.session_id),
    delete: (track) => track.session.where(createdByMe),
  },
  transport_observations: {
    read: (observation) => isSessionMember(observation.session_id),
    // An observation may name the playing pattern, which must be this session's.
    insert: (observation) =>
      allOf(
        canEditSession(observation.session_id),
        anyOf(
          { pattern_id: null },
          observation.pattern.where({ session_id: observation.session_id }),
        ),
      ),
    update: (observation) => canEditSession(observation.session_id),
    delete: (observation) => canEditSession(observation.session_id),
  },
});
