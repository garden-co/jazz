import type { Db } from "jazz-tools";
import { app } from "../schema";
import {
  INSTRUMENTS,
  MAX_STEPS,
  instrumentForPosition,
  starterStep,
  trackColor,
} from "./instruments";
import { stepId } from "./step-id";

function instrumentName(position: number) {
  const instrument = instrumentForPosition(position);
  return { instrument, name: INSTRUMENTS.find((option) => option.value === instrument)!.label };
}

type StepAddress = { sessionId: string; trackId: string; patternId: string; position: number };

/** The derived row id and full row for one pad, ready to upsert. */
export async function stepRow(step: StepAddress, enabled: boolean) {
  return {
    id: await stepId(step.trackId, step.patternId, step.position),
    data: {
      session_id: step.sessionId,
      track_id: step.trackId,
      pattern_id: step.patternId,
      position: step.position,
      enabled,
      velocity: 100,
      probability: 100,
    },
  };
}

/**
 * Creates a session, its first pattern and a starter groove in one mergeable
 * transaction, so bandmates never see a half-built session.
 */
export async function createSession(
  db: Db,
  author: string,
  options: { title: string; tempo: number; trackCount: number; length: number },
) {
  const result = await db.transaction(async (tx) => {
    const session = tx.insert(app.sessions, { title: options.title, tempo_bpm: options.tempo });
    tx.insert(app.session_members, {
      session_id: session.id,
      member_author: author,
      role: "owner",
    });
    const pattern = tx.insert(app.patterns, {
      session_id: session.id,
      position: 0,
      name: "Pattern 1",
      length: options.length,
    });
    for (let position = 0; position < options.trackCount; position += 1) {
      const { instrument, name } = instrumentName(position);
      const track = tx.insert(app.tracks, {
        session_id: session.id,
        position,
        name,
        color: trackColor(position),
        instrument,
      });
      for (let step = 0; step < MAX_STEPS; step += 1) {
        if (!starterStep(instrument, step)) continue;
        const row = await stepRow(
          { sessionId: session.id, trackId: track.id, patternId: pattern.id, position: step },
          true,
        );
        tx.upsert(app.steps, row.id, row.data);
      }
    }
    tx.insert(app.transport_observations, {
      session_id: session.id,
      playing: false,
      bar: 0,
      observed_at: new Date(),
      tempo_bpm: options.tempo,
      pattern_id: pattern.id,
    });
    return session.id;
  });
  return result.value;
}

/** A new track starts silent; its pads exist as soon as someone presses them. */
export function addTrack(db: Db, sessionId: string, position: number) {
  const { instrument, name } = instrumentName(position);
  return db.insert(app.tracks, {
    session_id: sessionId,
    position,
    name,
    color: trackColor(position),
    instrument,
  });
}

export function addPattern(db: Db, sessionId: string, position: number, length: number) {
  return db.insert(app.patterns, {
    session_id: sessionId,
    position,
    name: `Pattern ${position + 1}`,
    length,
  });
}

/** Removes a track and every step it has, in any pattern, as one transaction. */
export async function removeTrack(db: Db, trackId: string) {
  const result = await db.transaction(async (tx) => {
    for (const step of await tx.all(app.steps.where({ track_id: trackId })))
      tx.delete(app.steps, step.id);
    tx.delete(app.tracks, trackId);
  });
  return result;
}
