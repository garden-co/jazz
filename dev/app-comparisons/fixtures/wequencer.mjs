// Wequencer fixture (the hero benchmark's shape): one session, 16 tracks,
// a 64-step pattern (1,024 pads, a quarter of them on), plus 99 more
// sessions of the same shape for scale. Users "seed" (creator), "a" and "b"
// (editors, the harness's two signed-in users).
import { rng } from "./rng.mjs";

export const USERS = {
  seed: { name: "Seed Drummer", email: "seed@bench.invalid", password: "bench-password-seed" },
  a: { name: "Bench A", email: "a@bench.invalid", password: "bench-password-a" },
  b: { name: "Bench B", email: "b@bench.invalid", password: "bench-password-b" },
};
export const SESSIONS = 100;
export const TRACKS = 16;
export const STEPS = 64;
export const BENCH_SESSION = "session-0";

const INSTRUMENTS = ["kick", "snare", "hat", "clap", "tom", "rim", "shaker", "cowbell"];

export function wequencerFixture() {
  const r = rng(3);
  const sessions = [];
  const members = [];
  const tracks = [];
  const patterns = [];
  const pads = [];
  for (let s = 0; s < SESSIONS; s++) {
    const key = `session-${s}`;
    sessions.push({ key, name: s === 0 ? "Bench groove" : `Groove ${s + 1}`, bpm: 120 });
    members.push(...[["seed", "owner"], ["a", "editor"], ["b", "editor"]].map(([user, role]) => ({ sessionKey: key, user, role })));
    const patternKey = `${key}-pattern-0`;
    patterns.push({ key: patternKey, sessionKey: key, name: "A", steps: STEPS, position: 0 });
    for (let t = 0; t < TRACKS; t++) {
      const trackKey = `${key}-track-${t}`;
      tracks.push({ key: trackKey, sessionKey: key, instrument: INSTRUMENTS[t % INSTRUMENTS.length], position: t, volume: 0.8 });
      for (let step = 0; step < STEPS; step++) {
        if (r() < 0.25) pads.push({ trackKey, patternKey, step, velocity: 1 });
      }
    }
  }
  return { users: USERS, sessions, members, tracks, patterns, pads };
}
