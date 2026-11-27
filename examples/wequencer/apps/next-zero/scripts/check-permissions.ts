// Runs the server-side mutators and queries against the seeded database as
// different users, checking "Who can do what". Run after `pnpm seed`.
import pg from "pg";
import { zeroNodePg } from "@rocicorp/zero/server/adapters/pg";
import { mutators } from "../mutators";
import { queries } from "../queries";
import { schema } from "../schema";
import { DATABASE_URL } from "./db";

const pool = new pg.Pool({ connectionString: DATABASE_URL });
const db = zeroNodePg(schema, pool);
let failures = 0;
type Mut = { fn: (o: { tx: never; args: never; ctx: never }) => Promise<unknown> };
async function expect(label: string, ok: boolean, user: string, mutator: Mut, args: object) {
  let error: unknown;
  try {
    await db.transaction((tx) => mutator.fn({ tx: tx as never, args: args as never, ctx: { userID: user } as never }));
  } catch (e) {
    error = e;
  }
  const passed = ok ? !error : !!error;
  if (!passed) failures++;
  console.log(`${passed ? "ok  " : "FAIL"} ${label}${error && ok ? `: ${(error as Error).message}` : ""}`);
}
async function rows(user: string, query: { fn: (o: { args: never; ctx: never }) => unknown }, args: object) {
  return db.transaction((tx) => tx.run(query.fn({ args: args as never, ctx: { userID: user } as never }) as never)) as Promise<unknown[] | unknown>;
}
const check = (label: string, ok: boolean) => (console.log(`${ok ? "ok  " : "FAIL"} ${label}`), ok || failures++);
const { stepRow } = await import("../lib/session-setup");

const users = Object.fromEntries((await pool.query(`SELECT u.id, u.email FROM auth."user" u`)).rows.map((r) => [r.email.split("@")[0], r.id]));
const { rows: [bench] } = await pool.query(`SELECT id FROM sessions WHERE title = 'Bench groove'`);
const { rows: [other] } = await pool.query(`SELECT id FROM sessions WHERE title = 'Groove 2'`);
const track = async (s: string) => (await pool.query(`SELECT id FROM tracks WHERE session_id = $1 ORDER BY position LIMIT 1`, [s])).rows[0].id;
const pattern = async (s: string) => (await pool.query(`SELECT id FROM patterns WHERE session_id = $1 LIMIT 1`, [s])).rows[0].id;
const [bt, bp, ot, op] = [await track(bench.id), await pattern(bench.id), await track(other.id), await pattern(other.id)];
const outsider = crypto.randomUUID();
const viewer = crypto.randomUUID();

check("outsider reads no pads", ((await rows(outsider, queries.steps.lane, { trackId: bt, patternId: bp })) as unknown[]).length === 0);
check("editor reads the lane", ((await rows(users.a, queries.steps.lane, { trackId: bt, patternId: bp })) as unknown[]).length > 0);
await expect("editor toggles a pad", true, users.a, mutators.steps.set, stepRow({ sessionId: bench.id, trackId: bt, patternId: bp, position: 5 }, true));
await expect("outsider can't toggle", false, outsider, mutators.steps.set, stepRow({ sessionId: bench.id, trackId: bt, patternId: bp, position: 6 }, true));
await expect("pad's pattern must be the session's", false, users.a, mutators.steps.set, stepRow({ sessionId: bench.id, trackId: bt, patternId: op, position: 7 }, true));
await expect("pad's track must be the session's", false, users.a, mutators.steps.set, stepRow({ sessionId: bench.id, trackId: ot, patternId: bp, position: 7 }, true));
await expect("editor can't add members", false, users.a, mutators.members.add, { id: crypto.randomUUID(), session_id: bench.id, member_author: viewer, role: "viewer" });
await expect("creator adds a viewer", true, users.seed, mutators.members.add, { id: crypto.randomUUID(), session_id: bench.id, member_author: viewer, role: "viewer" });
check("viewer reads the lane", ((await rows(viewer, queries.steps.lane, { trackId: bt, patternId: bp })) as unknown[]).length > 0);
await expect("viewer can't toggle", false, viewer, mutators.steps.set, stepRow({ sessionId: bench.id, trackId: bt, patternId: bp, position: 8 }, true));
await expect("viewer can't change the transport", false, viewer, mutators.transport.observe, { id: crypto.randomUUID(), session_id: bench.id, playing: true, bar: 0, observed_at: Date.now(), tempo_bpm: 120, pattern_id: bp });
await expect("transport can't point at another session's pattern", false, users.a, mutators.transport.observe, { id: crypto.randomUUID(), session_id: bench.id, playing: true, bar: 0, observed_at: Date.now(), tempo_bpm: 120, pattern_id: op });
await expect("editor plays", true, users.a, mutators.transport.observe, { id: crypto.randomUUID(), session_id: bench.id, playing: true, bar: 0, observed_at: Date.now(), tempo_bpm: 120, pattern_id: bp });
await expect("editor can't remove a track", false, users.a, mutators.tracks.remove, { id: bt });
await expect("editor mutes a track", true, users.a, mutators.tracks.update, { id: bt, muted: true });
await expect("viewer can't mute", false, viewer, mutators.tracks.update, { id: bt, muted: false });
await expect("can't take over a session by id", false, outsider, mutators.sessions.create, { id: bench.id, title: "x", tempo_bpm: 120, memberId: crypto.randomUUID(), pattern: { id: crypto.randomUUID(), length: 16 }, tracks: [], steps: [], observationId: crypto.randomUUID() });
await pool.end();
console.log(failures ? `${failures} FAILED` : "all permission checks passed");
process.exit(failures ? 1 : 0);
