import type { Db } from "jazz-tools";
import { app, type Instrument } from "@/schema";
import {
  INSTRUMENTS,
  MAX_STEPS,
  instrumentForPosition,
  starterStep,
  trackColor,
} from "./instruments";

type TrackSeed = { id: string; instrument: Instrument };

/**
 * Inserts one step row per position for a track in a pattern. Rows exist for
 * all 64 positions up front, so lengthening a pattern later never has two
 * bandmates racing to create the same pad.
 */
function insertSteps(db: Db, track: TrackSeed, patternId: string, seeded: boolean) {
  for (let position = 0; position < MAX_STEPS; position += 1) {
    db.insert(app.steps, {
      track_id: track.id,
      pattern_id: patternId,
      position,
      enabled: seeded && starterStep(track.instrument, position),
      velocity: 100,
      probability: 100,
    });
  }
}

export function createSession(
  db: Db,
  author: string,
  options: { title: string; tempo: number; trackCount: number; length: number },
) {
  // An explicit user action, not account bootstrap on a read path.
  const session = db.insert(app.sessions, {
    title: options.title,
    tempo_bpm: options.tempo,
    loop_steps: options.length,
  });
  const sessionId = session.value.id;
  db.insert(app.session_members, { session_id: sessionId, member_author: author, role: "owner" });
  const pattern = db.insert(app.patterns, {
    session_id: sessionId,
    position: 0,
    name: "Pattern 1",
    length: options.length,
  });
  for (let position = 0; position < options.trackCount; position += 1) {
    const instrument = instrumentForPosition(position);
    const track = db.insert(app.tracks, {
      session_id: sessionId,
      position,
      name: INSTRUMENTS.find((option) => option.value === instrument)!.label,
      color: trackColor(position),
      instrument,
    });
    insertSteps(db, { id: track.value.id, instrument }, pattern.value.id, true);
  }
  db.insert(app.transport_observations, {
    session_id: sessionId,
    playing: false,
    bar: 0,
    observed_at: new Date(),
    tempo_bpm: options.tempo,
    pattern_id: pattern.value.id,
  });
  return sessionId;
}

export function addTrack(
  db: Db,
  sessionId: string,
  position: number,
  patternIds: string[],
): string {
  const instrument = instrumentForPosition(position);
  const track = db.insert(app.tracks, {
    session_id: sessionId,
    position,
    name: INSTRUMENTS.find((option) => option.value === instrument)!.label,
    color: trackColor(position),
    instrument,
  });
  for (const patternId of patternIds)
    insertSteps(db, { id: track.value.id, instrument }, patternId, false);
  return track.value.id;
}

export function addPattern(
  db: Db,
  sessionId: string,
  position: number,
  length: number,
  tracks: TrackSeed[],
): string {
  const pattern = db.insert(app.patterns, {
    session_id: sessionId,
    position,
    name: `Pattern ${position + 1}`,
    length,
  });
  for (const track of tracks) insertSteps(db, track, pattern.value.id, false);
  return pattern.value.id;
}
