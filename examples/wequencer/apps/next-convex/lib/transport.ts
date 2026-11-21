import { STEPS_PER_BEAT } from "./instruments";

/**
 * The shared transport, read from the newest `transport_observations` row.
 * `anchorStep` was sounding at `anchorAt` (wall-clock milliseconds on the
 * client that wrote it). Every client extrapolates from its own clock, so
 * bandmates hear roughly the same step, not a sample-accurate one.
 */
export type TransportState = {
  playing: boolean;
  anchorStep: number;
  anchorAt: number;
  tempo: number;
  patternId: string | undefined;
};

/** The columns of a transport observation this client writes. */
export type TransportWrite = {
  playing: boolean;
  bar: number;
  observed_at: Date;
  tempo_bpm: number;
  pattern_id: string | undefined;
};

export function stepDurationMs(tempo: number) {
  return 60_000 / tempo / STEPS_PER_BEAT;
}

/** The absolute (unwrapped) step sounding at `now`. */
export function absoluteStepAt(state: TransportState, now: number) {
  if (!state.playing) return state.anchorStep;
  return state.anchorStep + Math.floor((now - state.anchorAt) / stepDurationMs(state.tempo));
}

export function stepStartMs(state: TransportState, absoluteStep: number) {
  return state.anchorAt + (absoluteStep - state.anchorStep) * stepDurationMs(state.tempo);
}

export function wrapStep(absoluteStep: number, length: number) {
  return ((absoluteStep % length) + length) % length;
}

export function startPlayback(state: TransportState, now: number): TransportWrite {
  return {
    playing: true,
    bar: 0,
    observed_at: new Date(now),
    tempo_bpm: state.tempo,
    pattern_id: state.patternId,
  };
}

export function stopPlayback(state: TransportState, now: number): TransportWrite {
  return {
    playing: false,
    bar: 0,
    observed_at: new Date(now),
    tempo_bpm: state.tempo,
    pattern_id: state.patternId,
  };
}

/**
 * Changes tempo or pattern without restarting. While playing, the new row is
 * anchored at the start of the step that is sounding now, which is never
 * later than `now`, so a later Stop from any bandmate still sorts after it.
 */
export function retime(
  state: TransportState,
  now: number,
  change: { tempo?: number; patternId?: string },
): TransportWrite {
  const tempo = change.tempo ?? state.tempo;
  const patternId = change.patternId ?? state.patternId;
  if (!state.playing)
    return {
      playing: false,
      bar: 0,
      observed_at: new Date(now),
      tempo_bpm: tempo,
      pattern_id: patternId,
    };
  const current = absoluteStepAt(state, now);
  return {
    playing: true,
    bar: current,
    observed_at: new Date(Math.min(now, stepStartMs(state, current))),
    tempo_bpm: tempo,
    pattern_id: patternId,
  };
}
