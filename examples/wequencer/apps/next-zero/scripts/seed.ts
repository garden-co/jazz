// Resets the database and writes the Wequencer fixture: the three users
// through Better Auth's server API (profiles as the app's bootstrap makes
// them), everything else straight into Postgres. Prints the harness's ids.
import pg from "pg";
import { BENCH_SESSION, wequencerFixture } from "../../../../../dev/app-comparisons/fixtures/wequencer.mjs";
import { DATABASE_URL, resetDatabase } from "./db";

const fixture = wequencerFixture();
await resetDatabase();
const { auth } = await import("../lib/auth");
const { ensureProfile } = await import("../lib/bootstrap");
const { INSTRUMENTS, instrumentForPosition, trackColor } = await import("../lib/instruments");
const { stepRow } = await import("../lib/session-setup");

const authors: Record<string, string> = {};
for (const [key, user] of Object.entries(fixture.users)) {
  const { user: created } = await auth.api.signUpEmail({ body: user });
  authors[key] = created.id;
  await ensureProfile(created.id, user.name);
}

const rows = { sessions: [] as object[], session_members: [] as object[], patterns: [] as object[], tracks: [] as object[], steps: [] as object[], transport_observations: [] as object[] };
const ids: Record<string, string> = {};
const now = Date.now();
fixture.sessions.forEach((session, index) => {
  const sessionId = crypto.randomUUID();
  ids[session.key] = sessionId;
  // Newest first in the browser, as the Jazz seed's insertion order gives.
  rows.sessions.push({ id: sessionId, title: session.name, tempo_bpm: session.bpm, created_by: authors.seed, created_at: now + index });
  for (const m of fixture.members.filter((m) => m.sessionKey === session.key)) {
    rows.session_members.push({ id: crypto.randomUUID(), session_id: sessionId, member_author: authors[m.user], role: m.role });
  }
  const patternId = crypto.randomUUID();
  rows.patterns.push({ id: patternId, session_id: sessionId, position: 0, name: "Pattern 1", length: 64 });
  const trackIds = new Map<string, string>();
  for (const t of fixture.tracks.filter((t) => t.sessionKey === session.key)) {
    const instrument = instrumentForPosition(t.position);
    const trackId = crypto.randomUUID();
    trackIds.set(t.key, trackId);
    rows.tracks.push({
      id: trackId,
      session_id: sessionId,
      position: t.position,
      name: INSTRUMENTS.find((o) => o.value === instrument)!.label,
      color: trackColor(t.position),
      instrument,
      volume: 80,
      muted: false,
      solo: false,
    });
  }
  for (const pad of fixture.pads) {
    const trackId = trackIds.get(pad.trackKey);
    if (trackId) rows.steps.push(stepRow({ sessionId, trackId, patternId, position: pad.step }, true));
  }
  rows.transport_observations.push({
    id: crypto.randomUUID(),
    session_id: sessionId,
    playing: false,
    bar: 0,
    observed_at: now,
    tempo_bpm: session.bpm,
    pattern_id: patternId,
  });
});

const db = new pg.Client({ connectionString: DATABASE_URL });
await db.connect();
await db.query("BEGIN");
for (const [table, list] of Object.entries(rows)) {
  await db.query(`INSERT INTO "${table}" SELECT * FROM json_populate_recordset(null::"${table}", $1)`, [JSON.stringify(list)]);
}
await db.query("COMMIT");
await db.end();

const onPads = fixture.pads.filter((p) => p.trackKey.startsWith(`${BENCH_SESSION}-`)).length;
console.log(JSON.stringify({ sessionId: ids[BENCH_SESSION], onPads, users: fixture.users }));
process.exit(0);
