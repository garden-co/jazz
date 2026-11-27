import type { Db } from "./live";
import { mutators } from "../mutators";
import { INSTRUMENTS, MAX_STEPS, instrumentForPosition, starterStep, trackColor } from "./instruments";
import { stepId } from "./step-id";
import type { TransportWrite } from "./transport";

function instrumentName(position: number) {
  const instrument = instrumentForPosition(position);
  return { instrument, name: INSTRUMENTS.find((option) => option.value === instrument)!.label };
}

type StepAddress = { sessionId: string; trackId: string; patternId: string; position: number };

/** The derived row id and full row for one pad, ready to upsert. */
export function stepRow(step: StepAddress, enabled: boolean) {
  return {
    id: stepId(step.trackId, step.patternId, step.position),
    session_id: step.sessionId,
    track_id: step.trackId,
    pattern_id: step.patternId,
    position: step.position,
    enabled,
    velocity: 100,
    probability: 100,
  };
}

/** A new track's row: it starts silent; its pads exist as soon as someone presses them. */
export function trackRow(position: number) {
  const { instrument, name } = instrumentName(position);
  return { id: crypto.randomUUID(), position, name, color: trackColor(position), instrument };
}

/**
 * Creates a session, its first pattern and a starter groove in one mutation,
 * so bandmates never see a half-built session. Resolves with the session id
 * once it is applied locally.
 */
export async function createSession(
  db: Db,
  _author: string,
  options: { title: string; tempo: number; trackCount: number; length: number },
) {
  const sessionId = crypto.randomUUID();
  const patternId = crypto.randomUUID();
  const tracks = Array.from({ length: options.trackCount }, (_, position) => trackRow(position));
  const steps = tracks.flatMap((track) =>
    Array.from({ length: MAX_STEPS }, (_, step) => step)
      .filter((step) => starterStep(track.instrument, step))
      .map((step) => stepRow({ sessionId, trackId: track.id, patternId, position: step }, true)),
  );
  const write = db.mutate(
    mutators.sessions.create({
      id: sessionId,
      title: options.title,
      tempo_bpm: options.tempo,
      memberId: crypto.randomUUID(),
      pattern: { id: patternId, length: options.length },
      tracks,
      steps,
      observationId: crypto.randomUUID(),
    }),
  );
  // Applied locally at once (the overlay); the server's answer arrives later.
  await write.client;
  return sessionId;
}

export function addTrack(db: Db, sessionId: string, position: number) {
  return db.mutate(mutators.tracks.add({ ...trackRow(position), session_id: sessionId }));
}

export function addPattern(db: Db, sessionId: string, position: number, length: number) {
  const id = crypto.randomUUID();
  const write = db.mutate(
    mutators.patterns.add({ id, session_id: sessionId, position, name: `Pattern ${position + 1}`, length }),
  );
  return { id, write };
}

/** Removes a track and every step it has, in any pattern, as one mutation. */
export function removeTrack(db: Db, trackId: string) {
  return db.mutate(mutators.tracks.remove({ id: trackId }));
}

/** Appends a transport observation (play, stop, tempo, pattern). */
export function observe(db: Db, sessionId: string, observation: TransportWrite) {
  return db.mutate(
    mutators.transport.observe({
      id: crypto.randomUUID(),
      session_id: sessionId,
      ...observation,
      observed_at: observation.observed_at.getTime(),
      pattern_id: observation.pattern_id ?? null,
    }),
  );
}
