// Resets the Instant app and writes the Wequencer fixture through the admin
// SDK. Users sign up through the app's own auth (lib/instant.server.ts) and
// get their profiles from its bootstrap; the sessions are written as the seed
// user created them (one transaction per session, as createSession does).
// Prints the harness's ids as JSON.
import { id } from "@instantdb/admin";
import { adminDb as db, signUp } from "@/lib/instant.server";
import { ensureProfile } from "@/lib/bootstrap";
import { INSTRUMENTS, instrumentForPosition, trackColor } from "@/lib/instruments";
import { stepId } from "@/lib/step-id";
import { wequencerFixture, BENCH_SESSION } from "../../../../dev/app-comparisons/fixtures/wequencer.mjs";

const fixture = wequencerFixture();

/** Temporary apps rate-limit bursts of admin calls: wait and try again. */
async function retry<T>(call: () => Promise<T>): Promise<T> {
  for (let attempt = 0; ; attempt++) {
    try {
      return await call();
    } catch (error) {
      if ((error as { status?: number }).status !== 429 || attempt > 30) throw error;
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }
  }
}

// Reset: delete every row the app has (users keep their ids).
const namespaces = [
  "sessions",
  "session_members",
  "tracks",
  "patterns",
  "steps",
  "transport_observations",
  "presence",
  "profiles",
  "credentials",
];
for (const ns of namespaces) {
  for (;;) {
    const result: any = await retry(() =>
      db.query({ [ns]: { $: { limit: 1000, fields: ["id"] } } } as any),
    );
    const rows = result[ns] as { id: string }[];
    if (rows.length === 0) break;
    for (let i = 0; i < rows.length; i += 250)
      await retry(() =>
        db.transact(rows.slice(i, i + 250).map((row) => (db.tx as any)[ns][row.id].delete())),
      );
  }
}

const users: Record<string, string> = {};
for (const [key, user] of Object.entries(fixture.users) as [
  string,
  { name: string; email: string; password: string },
][]) {
  await retry(() => signUp(user));
  const author = (await retry(() => db.auth.getUser({ email: user.email })))!.id;
  await retry(() => ensureProfile(author, user.name));
  users[key] = author;
}

const ids: Record<string, string> = {};
for (const session of fixture.sessions) {
  const sessionId = (ids[session.key] = id());
  const patternId = id();
  const chunks: any[] = [
    db.tx.sessions[sessionId].create({
      title: session.name,
      tempo_bpm: session.bpm,
      creator: users.seed!,
      createdAt: Date.now(),
    }),
    db.tx.patterns[patternId]
      .create({ position: 0, name: "Pattern 1", length: 64 })
      .link({ session: sessionId }),
    db.tx.transport_observations[id()]
      .create({ playing: false, bar: 0, observed_at: Date.now(), tempo_bpm: session.bpm })
      .link({ session: sessionId, pattern: patternId }),
  ];
  for (const m of fixture.members.filter((m: any) => m.sessionKey === session.key)) {
    const author = users[m.user]!;
    chunks.push(
      db.tx.session_members[id()]
        .create({
          member_author: author,
          role: m.role,
          editor_author: m.role === "viewer" ? null : author,
        })
        .link({ session: sessionId }),
    );
  }
  const trackIds = new Map<string, string>();
  for (const t of fixture.tracks.filter((t: any) => t.sessionKey === session.key)) {
    const trackId = id();
    trackIds.set(t.key, trackId);
    const instrument = instrumentForPosition(t.position);
    chunks.push(
      db.tx.tracks[trackId]
        .create({
          position: t.position,
          name: INSTRUMENTS.find((o) => o.value === instrument)!.label,
          color: trackColor(t.position),
          instrument,
          volume: 80,
          muted: false,
          solo: false,
        })
        .link({ session: sessionId }),
    );
  }
  for (const pad of fixture.pads) {
    const trackId = trackIds.get(pad.trackKey);
    if (!trackId) continue;
    chunks.push(
      db.tx.steps[stepId(trackId, patternId, pad.step)]
        .update({ position: pad.step, enabled: true, velocity: 100, probability: 100 })
        .link({ track: trackId, pattern: patternId }),
    );
  }
  await retry(() => db.transact(chunks));
}
const onPads = fixture.pads.filter((p: any) => p.trackKey.startsWith(`${BENCH_SESSION}-`)).length;
console.log(JSON.stringify({ sessionId: ids[BENCH_SESSION], onPads, users: fixture.users }));
process.exit(0);
