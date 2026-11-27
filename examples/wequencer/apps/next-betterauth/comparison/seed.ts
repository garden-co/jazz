// Writes the Wequencer fixture as its users: each signs up through Better
// Auth, then the session creator writes through Jazz as themselves (policies
// apply), as the app's createSession does. Prints the harness's ids as JSON.
import { auth } from "@/lib/auth";
import { jazz } from "@/lib/jazz.server";
import { ensureProfile } from "@/lib/bootstrap";
import { INSTRUMENTS, instrumentForPosition, trackColor } from "@/lib/instruments";
import { stepRow } from "@/lib/session-setup";
import { app } from "@/jazz.generated";
import { wequencerFixture, BENCH_SESSION } from "../../../../../dev/app-comparisons/fixtures/wequencer.mjs";

const fixture = wequencerFixture();
const origin = process.env.NEXT_PUBLIC_APP_ORIGIN!;
const j = await jazz();
type UserDb = Awaited<ReturnType<typeof j.forReqUser>>;
const users: Record<string, { db: UserDb; author: string }> = {};
for (const [key, user] of Object.entries(fixture.users)) {
  const { headers, response } = await auth.api.signUpEmail({
    body: { name: user.name, email: user.email, password: user.password },
    returnHeaders: true,
  });
  await j.createAccountFor(response.user.id);
  const cookie = headers.getSetCookie().map((c) => c.split(";")[0]).join("; ");
  const request = new Request(origin, { headers: { cookie } });
  const author = (await j.accountFor(request))!;
  await ensureProfile(author, user.name);
  users[key] = { db: await j.forReqUser(request), author };
}

const seed = users.seed.db;
const ids: Record<string, { sessionId: string; patternId: string; trackIds: string[] }> = {};
for (const session of fixture.sessions) {
  const written = await seed.transaction((tx) => {
    const row = tx.insert(app.sessions, { title: session.name, tempo_bpm: session.bpm });
    for (const m of fixture.members.filter((m) => m.sessionKey === session.key)) {
      tx.insert(app.session_members, { session_id: row.id, member_author: users[m.user].author, role: m.role as never });
    }
    const pattern = tx.insert(app.patterns, { session_id: row.id, position: 0, name: "Pattern 1", length: 64 });
    const trackIds: string[] = [];
    const tracks = fixture.tracks.filter((t) => t.sessionKey === session.key);
    for (const t of tracks) {
      const instrument = instrumentForPosition(t.position);
      const track = tx.insert(app.tracks, {
        session_id: row.id,
        position: t.position,
        name: INSTRUMENTS.find((o) => o.value === instrument)!.label,
        color: trackColor(t.position),
        instrument,
      });
      trackIds.push(track.id);
    }
    const trackIndex = new Map(tracks.map((t, i) => [t.key, i]));
    for (const pad of fixture.pads) {
      const i = trackIndex.get(pad.trackKey);
      if (i === undefined) continue;
      const step = stepRow({ sessionId: row.id, trackId: trackIds[i], patternId: pattern.id, position: pad.step }, true);
      tx.upsert(app.steps, step.id, step.data);
    }
    tx.insert(app.transport_observations, {
      session_id: row.id,
      playing: false,
      bar: 0,
      observed_at: new Date(),
      tempo_bpm: session.bpm,
      pattern_id: pattern.id,
    });
    return { sessionId: row.id, patternId: pattern.id, trackIds };
  });
  await written.wait({ tier: "global" });
  ids[session.key] = written.value;
}
await j.close();
const bench = ids[BENCH_SESSION];
const onPads = fixture.pads.filter((p) => p.trackKey.startsWith(`${BENCH_SESSION}-`)).length;
console.log(JSON.stringify({ sessionId: bench.sessionId, onPads, users: fixture.users }));
