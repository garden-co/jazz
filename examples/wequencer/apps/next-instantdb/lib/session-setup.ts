import { id, type TransactionChunk } from "@instantdb/react";
import { db } from "@/lib/db";
import {
  INSTRUMENTS,
  MAX_STEPS,
  instrumentForPosition,
  starterStep,
  trackColor,
} from "./instruments";
import { stepId } from "./step-id";
import type { TransportWrite } from "./transport";

function instrumentName(position: number) {
  const instrument = instrumentForPosition(position);
  return { instrument, name: INSTRUMENTS.find((option) => option.value === instrument)!.label };
}

type StepAddress = { trackId: string; patternId: string; position: number };

/** The upsert of one pad, under the row id every client derives for it. */
export function stepChunk(step: StepAddress, enabled: boolean) {
  return db.tx.steps[stepId(step.trackId, step.patternId, step.position)]
    .update({ position: step.position, enabled, velocity: 100, probability: 100 })
    .link({ track: step.trackId, pattern: step.patternId });
}

/** A new transport observation for the session. */
export function observationChunk(sessionId: string, observation: TransportWrite) {
  const { pattern_id, ...fields } = observation;
  return db.tx.transport_observations[id()]
    .create(fields)
    .link({ session: sessionId, ...(pattern_id ? { pattern: pattern_id } : {}) });
}

function trackChunk(sessionId: string, trackId: string, position: number) {
  const { instrument, name } = instrumentName(position);
  return db.tx.tracks[trackId]
    .create({
      position,
      name,
      color: trackColor(position),
      instrument,
      volume: 80,
      muted: false,
      solo: false,
    })
    .link({ session: sessionId });
}

/**
 * Creates a session, its first pattern and a starter groove in one
 * transaction, so bandmates never see a half-built session.
 */
export async function createSession(
  author: string,
  options: { title: string; tempo: number; trackCount: number; length: number },
) {
  const sessionId = id();
  const patternId = id();
  const chunks: TransactionChunk<any, any>[] = [
    db.tx.sessions[sessionId].create({
      title: options.title,
      tempo_bpm: options.tempo,
      creator: author,
      createdAt: Date.now(),
    }),
    db.tx.session_members[id()]
      .create({ member_author: author, role: "owner", editor_author: author })
      .link({ session: sessionId }),
    db.tx.patterns[patternId]
      .create({ position: 0, name: "Pattern 1", length: options.length })
      .link({ session: sessionId }),
  ];
  for (let position = 0; position < options.trackCount; position += 1) {
    const trackId = id();
    chunks.push(trackChunk(sessionId, trackId, position));
    const { instrument } = instrumentName(position);
    for (let step = 0; step < MAX_STEPS; step += 1) {
      if (starterStep(instrument, step))
        chunks.push(stepChunk({ trackId, patternId, position: step }, true));
    }
  }
  chunks.push(
    observationChunk(sessionId, {
      playing: false,
      bar: 0,
      observed_at: new Date(),
      tempo_bpm: options.tempo,
      pattern_id: patternId,
    }),
  );
  await db.transact(chunks);
  return sessionId;
}

/** A new track starts silent; its pads exist as soon as someone presses them. */
export function addTrack(sessionId: string, position: number) {
  return db.transact(trackChunk(sessionId, id(), position));
}

export function addPattern(sessionId: string, position: number, length: number) {
  const patternId = id();
  const write = db.transact(
    db.tx.patterns[patternId]
      .create({ position, name: `Pattern ${position + 1}`, length })
      .link({ session: sessionId }),
  );
  return { patternId, write };
}

/** Removes a track; its steps, in every pattern, go with it (cascade) in the same transaction. */
export function removeTrack(trackId: string) {
  return db.transact(db.tx.tracks[trackId].delete());
}
