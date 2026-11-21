// Runs the server's write handlers and live-query SQL against the seeded
// database as different users, checking "Who can do what". Run after `pnpm seed`
// (with `--conditions react-server`, for the server-only modules).
import pg from "pg";
import { runMutation } from "../lib/mutators.server";
import { liveQueries } from "../lib/live-queries.server";
import { DATABASE_URL } from "./db";

const pool = new pg.Pool({ connectionString: DATABASE_URL });
let failures = 0;
async function expect(label: string, ok: boolean, user: string, mutator: string, args: object) {
  const result = await runMutation(pool, user, mutator, args);
  const error = result.kind === "rejected" ? result.error : undefined;
  const passed = ok ? !error : !!error;
  if (!passed) failures++;
  console.log(`${passed ? "ok  " : "FAIL"} ${label}${error && ok ? `: ${error}` : ""}`);
}
async function rows(user: string, name: string, args: object = {}) {
  const query = liveQueries[name](user, args) as unknown as { sql: string; parameters: { value: unknown }[] };
  return (await pool.query(query.sql, query.parameters.map((p) => p.value))).rows;
}
const check = (label: string, ok: boolean) => (console.log(`${ok ? "ok  " : "FAIL"} ${label}`), ok || failures++);
const { stepRow } = await import("../lib/session-setup");

const users = Object.fromEntries((await pool.query(`SELECT u.id, u.email FROM neon_auth."user" u`)).rows.map((r) => [r.email.split("@")[0], r.id]));
const { rows: [bench] } = await pool.query(`SELECT id FROM sessions WHERE title = 'Bench groove'`);
const { rows: [other] } = await pool.query(`SELECT id FROM sessions WHERE title = 'Groove 2'`);
const track = async (s: string) => (await pool.query(`SELECT id FROM tracks WHERE session_id = $1 ORDER BY position LIMIT 1`, [s])).rows[0].id;
const pattern = async (s: string) => (await pool.query(`SELECT id FROM patterns WHERE session_id = $1 LIMIT 1`, [s])).rows[0].id;
const [bt, bp, ot, op] = [await track(bench.id), await pattern(bench.id), await track(other.id), await pattern(other.id)];
const outsider = crypto.randomUUID();
const viewer = crypto.randomUUID();

check("outsider reads no pads", (await rows(outsider, "steps.lane", { trackId: bt, patternId: bp })).length === 0);
check("editor reads the lane", (await rows(users.a, "steps.lane", { trackId: bt, patternId: bp })).length > 0);
await expect("editor toggles a pad", true, users.a, "steps.set", stepRow({ sessionId: bench.id, trackId: bt, patternId: bp, position: 5 }, true));
await expect("outsider can't toggle", false, outsider, "steps.set", stepRow({ sessionId: bench.id, trackId: bt, patternId: bp, position: 6 }, true));
await expect("pad's pattern must be the session's", false, users.a, "steps.set", stepRow({ sessionId: bench.id, trackId: bt, patternId: op, position: 7 }, true));
await expect("pad's track must be the session's", false, users.a, "steps.set", stepRow({ sessionId: bench.id, trackId: ot, patternId: bp, position: 7 }, true));
await expect("editor can't add members", false, users.a, "members.add", { id: crypto.randomUUID(), session_id: bench.id, member_author: viewer, role: "viewer" });
await expect("creator adds a viewer", true, users.seed, "members.add", { id: crypto.randomUUID(), session_id: bench.id, member_author: viewer, role: "viewer" });
check("viewer reads the lane", (await rows(viewer, "steps.lane", { trackId: bt, patternId: bp })).length > 0);
await expect("viewer can't toggle", false, viewer, "steps.set", stepRow({ sessionId: bench.id, trackId: bt, patternId: bp, position: 8 }, true));
await expect("viewer can't change the transport", false, viewer, "transport.observe", { id: crypto.randomUUID(), session_id: bench.id, playing: true, bar: 0, observed_at: Date.now(), tempo_bpm: 120, pattern_id: bp });
await expect("transport can't point at another session's pattern", false, users.a, "transport.observe", { id: crypto.randomUUID(), session_id: bench.id, playing: true, bar: 0, observed_at: Date.now(), tempo_bpm: 120, pattern_id: op });
await expect("editor plays", true, users.a, "transport.observe", { id: crypto.randomUUID(), session_id: bench.id, playing: true, bar: 0, observed_at: Date.now(), tempo_bpm: 120, pattern_id: bp });
await expect("editor can't remove a track", false, users.a, "tracks.remove", { id: bt });
await expect("editor mutes a track", true, users.a, "tracks.update", { id: bt, muted: true });
await expect("viewer can't mute", false, viewer, "tracks.update", { id: bt, muted: false });
await expect("can't take over a session by id", false, outsider, "sessions.create", { id: bench.id, title: "x", tempo_bpm: 120, memberId: crypto.randomUUID(), pattern: { id: crypto.randomUUID(), length: 16 }, tracks: [], steps: [], observationId: crypto.randomUUID() });
check("outsider reads no session", (await rows(outsider, "sessions.one", { sessionId: bench.id })).length === 0);
check("member reads the session", (await rows(users.b, "sessions.one", { sessionId: bench.id })).length === 1);
check("outsider reads no presence or tracks", (await rows(outsider, "tracks.bySession", { sessionId: bench.id })).length === 0);
const { pool: appPool } = await import("../lib/db.server");
await appPool.end();
await pool.end();
console.log(failures ? `${failures} FAILED` : "all permission checks passed");
process.exit(failures ? 1 : 0);
