import { defineMutator, defineMutators, type Transaction } from "@rocicorp/zero";
import { z } from "zod";
import { zql, type AuthData } from "./schema";

/**
 * Who can do what in Wequencer (reads are filtered in queries.ts):
 *
 * - Anyone signed in creates sessions. The creator (`created_by`, immutable)
 *   administers the session: adds and removes members, deletes tracks and
 *   patterns. The `owner` role is only the creator's initial membership.
 * - Editors and owners change tracks, patterns, pads and the transport; a
 *   pad's track and pattern, and the transport's pattern, must belong to the
 *   session. Viewers only read.
 * - Members publish presence for their own profile.
 *
 * Mutators run optimistically on the client and again on the server; the
 * checks run on the server, where reads see all data.
 */
function mustUser(ctx: AuthData | undefined) {
  if (!ctx?.userID) throw new Error("Not signed in");
  return ctx.userID;
}
const onServer = (tx: Transaction) => tx.location === "server";

async function assertCreator(tx: Transaction, me: string, sessionId: string) {
  if (!onServer(tx)) return;
  if (!(await tx.run(zql.sessions.where("id", sessionId).where("created_by", me).one())))
    throw new Error("permission_denied: only the session creator can do that");
}

async function assertRole(tx: Transaction, me: string, sessionId: string, roles: string[]) {
  if (!onServer(tx)) return;
  const rows = await tx.run(zql.session_members.where("session_id", sessionId).where("member_author", me));
  if (!rows.some((row) => roles.includes(row.role))) throw new Error("permission_denied: not allowed in this session");
}
const assertEditor = (tx: Transaction, me: string, sessionId: string) =>
  assertRole(tx, me, sessionId, ["owner", "editor"]);
const assertMember = (tx: Transaction, me: string, sessionId: string) =>
  assertRole(tx, me, sessionId, ["owner", "editor", "viewer"]);

/** The row must exist and belong to the session. */
async function assertInSession(tx: Transaction, table: "tracks" | "patterns", id: string, sessionId: string) {
  if (!onServer(tx)) return;
  const row =
    table === "tracks"
      ? await tx.run(zql.tracks.where("id", id).one())
      : await tx.run(zql.patterns.where("id", id).one());
  if (row?.session_id !== sessionId) throw new Error(`permission_denied: ${table} row is not in this session`);
}

const id = z.string();
// A new track's mix, as the Jazz schema's column defaults.
const NEW_TRACK = { volume: 80, muted: false, solo: false };
const instrument = z.enum(["kick", "snare", "clap", "closed_hat", "open_hat", "tom", "bass", "lead"]);
const trackFields = { position: z.number().int(), name: z.string(), color: z.string(), instrument };
const stepFields = {
  id,
  session_id: id,
  track_id: id,
  pattern_id: id,
  position: z.number().int().min(0).max(63),
  enabled: z.boolean(),
  velocity: z.number().int(),
  probability: z.number().int(),
};
const observation = z.object({
  id,
  session_id: id,
  playing: z.boolean(),
  bar: z.number().int(),
  observed_at: z.number(),
  tempo_bpm: z.number().int(),
  pattern_id: id.nullable().optional(),
});

export const mutators = defineMutators({
  sessions: {
    /** A session, its owner membership, first pattern, starter tracks and pads and transport, together. */
    create: defineMutator(
      z.object({
        id,
        title: z.string().min(1),
        tempo_bpm: z.number().int(),
        memberId: id,
        pattern: z.object({ id, length: z.number().int() }),
        tracks: z.array(z.object({ id, ...trackFields })),
        steps: z.array(z.object(stepFields)),
        observationId: id,
      }),
      async ({ tx, ctx, args }) => {
        const me = mustUser(ctx);
        if (onServer(tx) && (await tx.run(zql.sessions.where("id", args.id).one()))) throw new Error("Session exists");
        await tx.mutate.sessions.insert({ id: args.id, title: args.title, tempo_bpm: args.tempo_bpm, created_by: me, created_at: Date.now() });
        await tx.mutate.session_members.insert({ id: args.memberId, session_id: args.id, member_author: me, role: "owner" });
        await tx.mutate.patterns.insert({ id: args.pattern.id, session_id: args.id, position: 0, name: "Pattern 1", length: args.pattern.length });
        for (const track of args.tracks) await tx.mutate.tracks.insert({ ...track, ...NEW_TRACK, session_id: args.id });
        for (const step of args.steps) {
          if (step.session_id !== args.id || step.pattern_id !== args.pattern.id || !args.tracks.some((t) => t.id === step.track_id))
            throw new Error("Steps must belong to the new session");
          await tx.mutate.steps.upsert(step);
        }
        await tx.mutate.transport_observations.insert({
          id: args.observationId,
          session_id: args.id,
          playing: false,
          bar: 0,
          observed_at: Date.now(),
          tempo_bpm: args.tempo_bpm,
          pattern_id: args.pattern.id,
        });
      },
    ),
  },

  members: {
    add: defineMutator(
      z.object({ id, session_id: id, member_author: z.string().min(1), role: z.enum(["editor", "viewer"]) }),
      async ({ tx, ctx, args }) => {
        await assertCreator(tx, mustUser(ctx), args.session_id);
        await tx.mutate.session_members.insert(args);
      },
    ),
    remove: defineMutator(z.object({ id }), async ({ tx, ctx, args }) => {
      const member = await tx.run(zql.session_members.where("id", args.id).one());
      if (!member) return;
      await assertCreator(tx, mustUser(ctx), member.session_id);
      await tx.mutate.session_members.delete(args);
    }),
  },

  tracks: {
    /** A new track starts silent; its pads exist as soon as someone presses them. */
    add: defineMutator(z.object({ id, session_id: id, ...trackFields }), async ({ tx, ctx, args }) => {
      await assertEditor(tx, mustUser(ctx), args.session_id);
      await tx.mutate.tracks.insert({ ...args, ...NEW_TRACK });
    }),
    update: defineMutator(
      z.object({
        id,
        name: z.string().min(1).optional(),
        instrument: instrument.optional(),
        volume: z.number().int().min(0).max(100).optional(),
        muted: z.boolean().optional(),
        solo: z.boolean().optional(),
      }),
      async ({ tx, ctx, args }) => {
        const track = await tx.run(zql.tracks.where("id", args.id).one());
        if (!track) return;
        await assertEditor(tx, mustUser(ctx), track.session_id);
        await tx.mutate.tracks.update(args);
      },
    ),
    /** Removes a track and every step it has, in any pattern, together. */
    remove: defineMutator(z.object({ id }), async ({ tx, ctx, args }) => {
      const track = await tx.run(zql.tracks.where("id", args.id).one());
      if (!track) return;
      await assertCreator(tx, mustUser(ctx), track.session_id);
      for (const step of await tx.run(zql.steps.where("track_id", args.id))) await tx.mutate.steps.delete({ id: step.id });
      await tx.mutate.tracks.delete(args);
    }),
  },

  patterns: {
    add: defineMutator(
      z.object({ id, session_id: id, position: z.number().int(), name: z.string(), length: z.number().int() }),
      async ({ tx, ctx, args }) => {
        await assertEditor(tx, mustUser(ctx), args.session_id);
        await tx.mutate.patterns.insert(args);
      },
    ),
    setLength: defineMutator(z.object({ id, length: z.number().int().min(1).max(64) }), async ({ tx, ctx, args }) => {
      const pattern = await tx.run(zql.patterns.where("id", args.id).one());
      if (!pattern) return;
      await assertEditor(tx, mustUser(ctx), pattern.session_id);
      await tx.mutate.patterns.update(args);
    }),
  },

  steps: {
    /** Upserts a pad by its derived id: concurrent first presses write the same row. */
    set: defineMutator(z.object(stepFields), async ({ tx, ctx, args }) => {
      await assertEditor(tx, mustUser(ctx), args.session_id);
      await assertInSession(tx, "tracks", args.track_id, args.session_id);
      await assertInSession(tx, "patterns", args.pattern_id, args.session_id);
      if (onServer(tx)) {
        // A squatted id in another session can't be rewritten into this one.
        const existing = await tx.run(zql.steps.where("id", args.id).one());
        if (existing && existing.session_id !== args.session_id) await assertEditor(tx, mustUser(ctx), existing.session_id);
      }
      await tx.mutate.steps.upsert(args);
    }),
  },

  transport: {
    /** Appends an observation; its pattern must be this session's. */
    observe: defineMutator(observation, async ({ tx, ctx, args }) => {
      await assertEditor(tx, mustUser(ctx), args.session_id);
      if (args.pattern_id) await assertInSession(tx, "patterns", args.pattern_id, args.session_id);
      await tx.mutate.transport_observations.insert({ ...args, pattern_id: args.pattern_id ?? null });
    }),
  },

  presence: {
    /** Publishes (or refreshes) this member's presence for their own profile. */
    beat: defineMutator(
      z.object({ id, session_id: id, profile_id: id, cursor_step: z.number().int(), heartbeat_at: z.number() }),
      async ({ tx, ctx, args }) => {
        const me = mustUser(ctx);
        await assertMember(tx, me, args.session_id);
        if (onServer(tx)) {
          const profile = await tx.run(zql.profiles.where("id", args.profile_id).where("author", me).one());
          if (!profile) throw new Error("permission_denied: not your profile");
          const existing = await tx.run(zql.presence.where("id", args.id).one());
          if (existing && existing.profile_id !== args.profile_id) throw new Error("permission_denied: not your presence");
        }
        await tx.mutate.presence.upsert(args);
      },
    ),
  },
});
