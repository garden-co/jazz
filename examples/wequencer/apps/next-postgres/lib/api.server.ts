// The app's REST reads and RPC writes, a Hono app mounted at /api by
// app/api/[[...route]]/route.ts (Better Auth and the bootstrap keep their own
// routes). Reads filter by what the caller may see; each write runs in one
// transaction with its checks and NOTIFYs what it changed.
import { Hono } from "hono";
import { z } from "zod";
import { auth } from "@/lib/auth";
import { one, pool, rows, transaction, type Tx } from "@/lib/db.server";
import { assertCreator, assertEditor, assertInSession, assertMember, canReadSql, Forbidden, isMemberSql } from "@/lib/permissions.server";
import type { Presence, SessionMember, Track } from "@/schema";

type Env = { Variables: { me: string } };
export const api = new Hono<Env>().basePath("/api");

api.get("/health", (c) => c.text("ok"));

api.use("*", async (c, next) => {
  const session = await auth.api.getSession({ headers: c.req.raw.headers });
  if (!session) return c.json({ error: "account required" }, 401);
  c.set("me", session.user.id);
  await next();
});

// --- reads ---
const list = (sql: string, params: (me: string, id: string, c: any) => unknown[], single = false) => async (c: any) => {
  const result = await rows(pool, sql, params(c.var.me, c.req.param("id"), c));
  return c.json(single ? (result[0] ?? null) : result);
};
const member = (table: string, order = "") =>
  list(`SELECT t.* FROM ${table} t WHERE t.session_id = $2 AND ${isMemberSql("$2", "$1")} ${order}`, (me, id) => [me, id]);

api.get("/profile", list(`SELECT * FROM profiles WHERE author = $1`, (me) => [me], true));
api.get("/sessions", list(`SELECT s.* FROM sessions s WHERE ${canReadSql("$1")} ORDER BY s.created_at DESC`, (me) => [me]));
api.get("/sessions/:id", list(`SELECT s.* FROM sessions s WHERE s.id = $2 AND ${canReadSql("$1")}`, (me, id) => [me, id], true));
api.get(
  "/sessions/:id/members",
  list(`SELECT m.* FROM session_members m JOIN sessions s ON s.id = m.session_id WHERE m.session_id = $2 AND ${canReadSql("$1")}`, (me, id) => [me, id]),
);
api.get("/sessions/:id/tracks", member("tracks", "ORDER BY t.position"));
api.get("/sessions/:id/patterns", member("patterns", "ORDER BY t.position"));
api.get("/sessions/:id/transport", member("transport_observations", "ORDER BY t.observed_at DESC LIMIT 1"));
api.get(
  "/sessions/:id/patterns/:patternId/steps",
  list(
    `SELECT t.* FROM steps t WHERE t.session_id = $2 AND t.pattern_id = $3 AND ${isMemberSql("$2", "$1")} ORDER BY t.track_id, t.position`,
    (me, id, c) => [me, id, c.req.param("patternId")],
  ),
);
api.get(
  "/sessions/:id/presence",
  list(
    `SELECT t.*, row_to_json(p) AS profile FROM presence t LEFT JOIN profiles p ON p.id = t.profile_id
      WHERE t.session_id = $2 AND ${isMemberSql("$2", "$1")}`,
    (me, id) => [me, id],
  ),
);

// --- writes ---
const session = (id: string) => `session:${id}`;
const me = (account: string) => `me:${account}`;

function insert(tx: Tx, table: string, row: Record<string, unknown>, onConflict = "") {
  const cols = Object.keys(row);
  return tx.query(
    `INSERT INTO ${table} (${cols.map((c) => `"${c}"`).join(", ")}) VALUES (${cols.map((_, i) => `$${i + 1}`).join(", ")}) ${onConflict}`,
    Object.values(row),
  );
}
const upsertStep = (tx: Tx, step: z.infer<typeof stepSchema>) =>
  insert(tx, "steps", step, "ON CONFLICT (id) DO UPDATE SET enabled = EXCLUDED.enabled, velocity = EXCLUDED.velocity, probability = EXCLUDED.probability");

const id = z.string();
const NEW_TRACK = { volume: 80, muted: false, solo: false };
const instrument = z.enum(["kick", "snare", "clap", "closed_hat", "open_hat", "tom", "bass", "lead"]);
const trackFields = { position: z.number().int(), name: z.string(), color: z.string(), instrument };
const stepSchema = z.object({
  id,
  session_id: id,
  track_id: id,
  pattern_id: id,
  position: z.number().int().min(0).max(63),
  enabled: z.boolean(),
  velocity: z.number().int(),
  probability: z.number().int(),
});

type Handler = { input: z.ZodType; run: (tx: Tx, account: string, args: any) => Promise<unknown> };
const rpc = <S extends z.ZodType>(input: S, run: (tx: Tx, account: string, args: z.infer<S>) => Promise<unknown>): Handler => ({ input, run });

const handlers: Record<string, Handler> = {
  // A session, its owner membership, first pattern, starter tracks and pads and transport, together.
  "sessions.create": rpc(
    z.object({
      id,
      title: z.string().min(1),
      tempo_bpm: z.number().int(),
      memberId: id,
      pattern: z.object({ id, length: z.number().int() }),
      tracks: z.array(z.object({ id, ...trackFields })),
      steps: z.array(stepSchema),
      observationId: id,
    }),
    async (tx, account, args) => {
      await insert(tx, "sessions", { id: args.id, title: args.title, tempo_bpm: args.tempo_bpm, created_by: account, created_at: Date.now() });
      await insert(tx, "session_members", { id: args.memberId, session_id: args.id, member_author: account, role: "owner" });
      await insert(tx, "patterns", { id: args.pattern.id, session_id: args.id, position: 0, name: "Pattern 1", length: args.pattern.length });
      for (const track of args.tracks) await insert(tx, "tracks", { ...track, ...NEW_TRACK, session_id: args.id });
      for (const step of args.steps) {
        if (step.session_id !== args.id || step.pattern_id !== args.pattern.id || !args.tracks.some((t) => t.id === step.track_id))
          throw new Forbidden("Steps must belong to the new session");
        await upsertStep(tx, step);
      }
      await insert(tx, "transport_observations", {
        id: args.observationId, session_id: args.id, playing: false, bar: 0, observed_at: Date.now(), tempo_bpm: args.tempo_bpm, pattern_id: args.pattern.id,
      });
      tx.changed(me(account), "sessions");
    },
  ),

  "members.add": rpc(
    z.object({ id, session_id: id, member_author: z.string().min(1), role: z.enum(["editor", "viewer"]) }),
    async (tx, account, args) => {
      await assertCreator(tx, account, args.session_id);
      await insert(tx, "session_members", args);
      tx.changed(session(args.session_id), "session_members");
      tx.changed(me(args.member_author), "sessions");
    },
  ),
  "members.remove": rpc(z.object({ id }), async (tx, account, args) => {
    const member = await one<SessionMember>(tx, `SELECT * FROM session_members WHERE id = $1`, [args.id]);
    if (!member) return;
    await assertCreator(tx, account, member.session_id);
    await tx.query(`DELETE FROM session_members WHERE id = $1`, [args.id]);
    tx.changed(session(member.session_id), "session_members");
    tx.changed(me(member.member_author), "sessions");
    const stillIn = await one(tx, `SELECT 1 FROM session_members WHERE session_id = $1 AND member_author = $2`, [member.session_id, member.member_author]);
    if (!stillIn && member.member_author !== account) tx.revoke(member.member_author, session(member.session_id));
  }),

  // A new track starts silent; its pads exist as soon as someone presses them.
  "tracks.add": rpc(z.object({ id, session_id: id, ...trackFields }), async (tx, account, args) => {
    await assertEditor(tx, account, args.session_id);
    await insert(tx, "tracks", { ...args, ...NEW_TRACK });
    tx.changed(session(args.session_id), "tracks");
  }),
  "tracks.update": rpc(
    z.object({
      id,
      name: z.string().min(1).optional(),
      instrument: instrument.optional(),
      volume: z.number().int().min(0).max(100).optional(),
      muted: z.boolean().optional(),
      solo: z.boolean().optional(),
    }),
    async (tx, account, { id: trackId, ...change }) => {
      const track = await one<Track>(tx, `SELECT * FROM tracks WHERE id = $1`, [trackId]);
      if (!track) return;
      await assertEditor(tx, account, track.session_id);
      const cols = Object.keys(change);
      if (cols.length)
        await tx.query(`UPDATE tracks SET ${cols.map((c, i) => `"${c}" = $${i + 2}`).join(", ")} WHERE id = $1`, [trackId, ...Object.values(change)]);
      tx.changed(session(track.session_id), "tracks");
    },
  ),
  // Removes a track and every step it has, in any pattern, together.
  "tracks.remove": rpc(z.object({ id }), async (tx, account, args) => {
    const track = await one<Track>(tx, `SELECT * FROM tracks WHERE id = $1`, [args.id]);
    if (!track) return;
    await assertCreator(tx, account, track.session_id);
    await tx.query(`DELETE FROM steps WHERE track_id = $1`, [args.id]);
    await tx.query(`DELETE FROM tracks WHERE id = $1`, [args.id]);
    tx.changed(session(track.session_id), "tracks", "steps");
  }),

  "patterns.add": rpc(
    z.object({ id, session_id: id, position: z.number().int(), name: z.string(), length: z.number().int() }),
    async (tx, account, args) => {
      await assertEditor(tx, account, args.session_id);
      await insert(tx, "patterns", args);
      tx.changed(session(args.session_id), "patterns");
    },
  ),
  "patterns.setLength": rpc(z.object({ id, length: z.number().int().min(1).max(64) }), async (tx, account, args) => {
    const pattern = await one<{ session_id: string }>(tx, `SELECT * FROM patterns WHERE id = $1`, [args.id]);
    if (!pattern) return;
    await assertEditor(tx, account, pattern.session_id);
    await tx.query(`UPDATE patterns SET length = $2 WHERE id = $1`, [args.id, args.length]);
    tx.changed(session(pattern.session_id), "patterns");
  }),

  // Upserts a pad by its derived id: concurrent first presses write the same row.
  "steps.set": rpc(stepSchema, async (tx, account, step) => {
    await assertEditor(tx, account, step.session_id);
    await assertInSession(tx, "tracks", step.track_id, step.session_id);
    await assertInSession(tx, "patterns", step.pattern_id, step.session_id);
    // A squatted id in another session can't be rewritten into this one.
    const existing = await one<{ session_id: string }>(tx, `SELECT session_id FROM steps WHERE id = $1`, [step.id]);
    if (existing && existing.session_id !== step.session_id) await assertEditor(tx, account, existing.session_id);
    await upsertStep(tx, step);
    tx.changed(session(step.session_id), "steps");
  }),

  // Appends an observation; its pattern must be this session's.
  "transport.observe": rpc(
    z.object({ id, session_id: id, playing: z.boolean(), bar: z.number().int(), observed_at: z.number(), tempo_bpm: z.number().int(), pattern_id: id.nullable().optional() }),
    async (tx, account, args) => {
      await assertEditor(tx, account, args.session_id);
      if (args.pattern_id) await assertInSession(tx, "patterns", args.pattern_id, args.session_id);
      await insert(tx, "transport_observations", { ...args, pattern_id: args.pattern_id ?? null });
      tx.changed(session(args.session_id), "transport_observations");
    },
  ),

  // Publishes (or refreshes) this member's presence for their own profile.
  "presence.beat": rpc(
    z.object({ id, session_id: id, profile_id: id, cursor_step: z.number().int(), heartbeat_at: z.number() }),
    async (tx, account, args) => {
      await assertMember(tx, account, args.session_id);
      if (!(await one(tx, `SELECT 1 FROM profiles WHERE id = $1 AND author = $2`, [args.profile_id, account])))
        throw new Forbidden("not your profile");
      const existing = await one<Presence>(tx, `SELECT * FROM presence WHERE id = $1`, [args.id]);
      if (existing && existing.profile_id !== args.profile_id) throw new Forbidden("not your presence");
      await insert(tx, "presence", args, "ON CONFLICT (id) DO UPDATE SET cursor_step = EXCLUDED.cursor_step, heartbeat_at = EXCLUDED.heartbeat_at");
      tx.changed(session(args.session_id), "presence");
    },
  ),
};

api.post("/rpc/:name", async (c) => {
  const handler = handlers[c.req.param("name")];
  if (!handler) return c.json({ error: "No such call" }, 404);
  const parsed = handler.input.safeParse(await c.req.json());
  if (!parsed.success) return c.json({ error: parsed.error.message }, 400);
  try {
    const result = await transaction((tx) => handler.run(tx, c.var.me, parsed.data));
    return c.json({ result: result ?? null });
  } catch (error) {
    if (error instanceof Forbidden) return c.json({ error: error.message }, 403);
    throw error;
  }
});
