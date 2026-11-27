import "server-only";
import type pg from "pg";
import { z } from "zod";

/**
 * Who can do what in Wequencer (reads are filtered in live-queries.server.ts):
 *
 * - Anyone signed in creates sessions. The creator (`created_by`, immutable)
 *   administers the session: adds and removes members, deletes tracks and
 *   patterns. The `owner` role is only the creator's initial membership.
 * - Editors and owners change tracks, patterns, pads and the transport; a
 *   pad's track and pattern, and the transport's pattern, must belong to the
 *   session. Viewers only read.
 * - Members publish presence for their own profile.
 *
 * Every write goes through these handlers (called from the Server Action in
 * app/actions.ts), as Neon's mutations guide does it: validate, authorize,
 * write in one transaction and return the transaction's id.
 */
export class Forbidden extends Error {
  constructor(message: string) {
    super(`permission_denied: ${message}`);
  }
}

export type Tx = {
  me: string;
  rows<T>(sql: string, params?: unknown[]): Promise<T[]>;
  /** A write; its RETURNING carries the transaction id, which the response returns. */
  write(sql: string, params?: unknown[]): Promise<number>;
};
const one = async <T = unknown>(tx: Tx, sql: string, params: unknown[]) => (await tx.rows<T>(sql, params))[0];

async function assertCreator(tx: Tx, sessionId: string) {
  if (!(await one(tx, `SELECT 1 FROM sessions WHERE id = $1 AND created_by = $2`, [sessionId, tx.me])))
    throw new Forbidden("only the session creator can do that");
}
async function assertRole(tx: Tx, sessionId: string, roles: string[]) {
  const row = await one(tx, `SELECT 1 FROM session_members WHERE session_id = $1 AND member_author = $2 AND role = ANY($3)`, [
    sessionId,
    tx.me,
    roles,
  ]);
  if (!row) throw new Forbidden("not allowed in this session");
}
const assertEditor = (tx: Tx, sessionId: string) => assertRole(tx, sessionId, ["owner", "editor"]);
const assertMember = (tx: Tx, sessionId: string) => assertRole(tx, sessionId, ["owner", "editor", "viewer"]);

/** The row must exist and belong to the session. */
async function assertInSession(tx: Tx, table: "tracks" | "patterns", id: string, sessionId: string) {
  if (!(await one(tx, `SELECT 1 FROM ${table} WHERE id = $1 AND session_id = $2`, [id, sessionId])))
    throw new Forbidden(`${table} row is not in this session`);
}

const id = z.string();
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
const step = z.object(stepFields);
type StepRow = z.infer<typeof step>;

function upsertStep(tx: Tx, s: StepRow) {
  return tx.write(
    `INSERT INTO steps (id, session_id, track_id, pattern_id, position, enabled, velocity, probability)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
     ON CONFLICT (id) DO UPDATE SET enabled = EXCLUDED.enabled, velocity = EXCLUDED.velocity, probability = EXCLUDED.probability`,
    [s.id, s.session_id, s.track_id, s.pattern_id, s.position, s.enabled, s.velocity, s.probability],
  );
}

type Mutator = { args: z.ZodType; run: (tx: Tx, args: never) => Promise<unknown> };
const mutator = <S extends z.ZodType>(args: S, run: (tx: Tx, args: z.infer<S>) => Promise<unknown>): Mutator => ({
  args,
  run: run as Mutator["run"],
});

export const mutators: Record<string, Mutator> = {
  /** A session, its owner membership, first pattern, starter tracks and pads and transport, together. */
  "sessions.create": mutator(
    z.object({
      id,
      title: z.string().min(1),
      tempo_bpm: z.number().int(),
      memberId: id,
      pattern: z.object({ id, length: z.number().int() }),
      tracks: z.array(z.object({ id, ...trackFields })),
      steps: z.array(step),
      observationId: id,
    }),
    async (tx, args) => {
      const now = Date.now();
      const created = await tx.write(
        `INSERT INTO sessions (id, title, tempo_bpm, created_by, created_at) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (id) DO NOTHING`,
        [args.id, args.title, args.tempo_bpm, tx.me, now],
      );
      if (!created) throw new Error("Session exists");
      await tx.write(`INSERT INTO session_members (id, session_id, member_author, role) VALUES ($1, $2, $3, 'owner')`, [
        args.memberId,
        args.id,
        tx.me,
      ]);
      await tx.write(`INSERT INTO patterns (id, session_id, position, name, length) VALUES ($1, $2, 0, 'Pattern 1', $3)`, [
        args.pattern.id,
        args.id,
        args.pattern.length,
      ]);
      for (const track of args.tracks) {
        await tx.write(`INSERT INTO tracks (id, session_id, position, name, color, instrument) VALUES ($1, $2, $3, $4, $5, $6)`, [
          track.id,
          args.id,
          track.position,
          track.name,
          track.color,
          track.instrument,
        ]);
      }
      for (const s of args.steps) {
        if (s.session_id !== args.id || s.pattern_id !== args.pattern.id || !args.tracks.some((t) => t.id === s.track_id))
          throw new Error("Steps must belong to the new session");
        await upsertStep(tx, s);
      }
      await tx.write(
        `INSERT INTO transport_observations (id, session_id, playing, bar, observed_at, tempo_bpm, pattern_id)
         VALUES ($1, $2, false, 0, $3, $4, $5)`,
        [args.observationId, args.id, now, args.tempo_bpm, args.pattern.id],
      );
    },
  ),

  "members.add": mutator(
    z.object({ id, session_id: id, member_author: z.string().min(1), role: z.enum(["editor", "viewer"]) }),
    async (tx, args) => {
      await assertCreator(tx, args.session_id);
      await tx.write(`INSERT INTO session_members (id, session_id, member_author, role) VALUES ($1, $2, $3, $4)`, [
        args.id,
        args.session_id,
        args.member_author,
        args.role,
      ]);
    },
  ),
  "members.remove": mutator(z.object({ id }), async (tx, args) => {
    const member = await one<{ session_id: string }>(tx, `SELECT session_id FROM session_members WHERE id = $1`, [args.id]);
    if (!member) return;
    await assertCreator(tx, member.session_id);
    await tx.write(`DELETE FROM session_members WHERE id = $1`, [args.id]);
  }),

  /** A new track starts silent; its pads exist as soon as someone presses them. */
  "tracks.add": mutator(z.object({ id, session_id: id, ...trackFields }), async (tx, args) => {
    await assertEditor(tx, args.session_id);
    await tx.write(`INSERT INTO tracks (id, session_id, position, name, color, instrument) VALUES ($1, $2, $3, $4, $5, $6)`, [
      args.id,
      args.session_id,
      args.position,
      args.name,
      args.color,
      args.instrument,
    ]);
  }),
  "tracks.update": mutator(
    z.object({
      id,
      name: z.string().min(1).optional(),
      instrument: instrument.optional(),
      volume: z.number().int().min(0).max(100).optional(),
      muted: z.boolean().optional(),
      solo: z.boolean().optional(),
    }),
    async (tx, { id: trackId, ...change }) => {
      const track = await one<{ session_id: string }>(tx, `SELECT session_id FROM tracks WHERE id = $1`, [trackId]);
      if (!track) return;
      await assertEditor(tx, track.session_id);
      const columns = Object.entries(change).filter(([, value]) => value !== undefined);
      if (columns.length === 0) return;
      await tx.write(`UPDATE tracks SET ${columns.map(([column], i) => `${column} = $${i + 2}`).join(", ")} WHERE id = $1`, [
        trackId,
        ...columns.map(([, value]) => value),
      ]);
    },
  ),
  /** Removes a track and every step it has, in any pattern, together. */
  "tracks.remove": mutator(z.object({ id }), async (tx, args) => {
    const track = await one<{ session_id: string }>(tx, `SELECT session_id FROM tracks WHERE id = $1`, [args.id]);
    if (!track) return;
    await assertCreator(tx, track.session_id);
    await tx.write(`DELETE FROM steps WHERE track_id = $1`, [args.id]);
    await tx.write(`DELETE FROM tracks WHERE id = $1`, [args.id]);
  }),

  "patterns.add": mutator(
    z.object({ id, session_id: id, position: z.number().int(), name: z.string(), length: z.number().int() }),
    async (tx, args) => {
      await assertEditor(tx, args.session_id);
      await tx.write(`INSERT INTO patterns (id, session_id, position, name, length) VALUES ($1, $2, $3, $4, $5)`, [
        args.id,
        args.session_id,
        args.position,
        args.name,
        args.length,
      ]);
    },
  ),
  "patterns.setLength": mutator(z.object({ id, length: z.number().int().min(1).max(64) }), async (tx, args) => {
    const pattern = await one<{ session_id: string }>(tx, `SELECT session_id FROM patterns WHERE id = $1`, [args.id]);
    if (!pattern) return;
    await assertEditor(tx, pattern.session_id);
    await tx.write(`UPDATE patterns SET length = $2 WHERE id = $1`, [args.id, args.length]);
  }),

  /** Upserts a pad by its derived id: concurrent first presses write the same row. */
  "steps.set": mutator(step, async (tx, args) => {
    await assertEditor(tx, args.session_id);
    await assertInSession(tx, "tracks", args.track_id, args.session_id);
    await assertInSession(tx, "patterns", args.pattern_id, args.session_id);
    // A squatted id in another session can't be rewritten into this one.
    const existing = await one<StepRow>(tx, `SELECT * FROM steps WHERE id = $1`, [args.id]);
    if (existing && (existing.session_id !== args.session_id || existing.track_id !== args.track_id || existing.pattern_id !== args.pattern_id))
      throw new Forbidden("that pad id belongs elsewhere");
    await upsertStep(tx, args);
  }),

  /** Appends an observation; its pattern must be this session's. */
  "transport.observe": mutator(
    z.object({
      id,
      session_id: id,
      playing: z.boolean(),
      bar: z.number().int(),
      observed_at: z.number(),
      tempo_bpm: z.number().int(),
      pattern_id: id.nullable().optional(),
    }),
    async (tx, args) => {
      await assertEditor(tx, args.session_id);
      if (args.pattern_id) await assertInSession(tx, "patterns", args.pattern_id, args.session_id);
      await tx.write(
        `INSERT INTO transport_observations (id, session_id, playing, bar, observed_at, tempo_bpm, pattern_id)
         VALUES ($1, $2, $3, $4, $5, $6, $7)`,
        [args.id, args.session_id, args.playing, args.bar, args.observed_at, args.tempo_bpm, args.pattern_id ?? null],
      );
    },
  ),

  /** Publishes (or refreshes) this member's presence for their own profile. */
  "presence.beat": mutator(
    z.object({ id, session_id: id, profile_id: id, cursor_step: z.number().int(), heartbeat_at: z.number() }),
    async (tx, args) => {
      await assertMember(tx, args.session_id);
      if (!(await one(tx, `SELECT 1 FROM profiles WHERE id = $1 AND author = $2`, [args.profile_id, tx.me])))
        throw new Forbidden("not your profile");
      const written = await tx.write(
        `INSERT INTO presence (id, session_id, profile_id, cursor_step, heartbeat_at) VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (id) DO UPDATE SET cursor_step = EXCLUDED.cursor_step, heartbeat_at = EXCLUDED.heartbeat_at
         WHERE presence.profile_id = EXCLUDED.profile_id AND presence.session_id = EXCLUDED.session_id`,
        [args.id, args.session_id, args.profile_id, args.cursor_step, args.heartbeat_at],
      );
      if (!written) throw new Forbidden("not your presence");
    },
  ),
};

export type MutationResponse = { kind: "committed"; txid: string } | { kind: "unchanged" } | { kind: "rejected"; error: string };

/** Runs a mutator in one transaction and returns its txid, read from the writes' own RETURNING. */
export async function runMutation(pool: pg.Pool, me: string, name: string, rawArgs: unknown): Promise<MutationResponse> {
  const def = mutators[name];
  if (!def) return { kind: "rejected", error: `No mutator ${name}` };
  const parsed = def.args.safeParse(rawArgs);
  if (!parsed.success) return { kind: "rejected", error: "invalid input" };
  const client = await pool.connect();
  let txid: string | undefined;
  const tx: Tx = {
    me,
    rows: async (sql, params) => (await client.query(sql, params)).rows,
    write: async (sql, params) => {
      const result = await client.query<{ txid: string }>(`${sql} RETURNING pg_current_xact_id()::text AS txid`, params);
      txid ??= result.rows[0]?.txid;
      return result.rowCount ?? 0;
    },
  };
  try {
    await client.query("BEGIN");
    await def.run(tx, parsed.data as never);
    await client.query("COMMIT");
    return txid ? { kind: "committed", txid } : { kind: "unchanged" };
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    return { kind: "rejected", error: error instanceof Error ? error.message : String(error) };
  } finally {
    client.release();
  }
}
