import type { Instrument } from "@/schema";
import { absoluteStepAt, stepStartMs, wrapStep, type TransportState } from "./transport";

/**
 * A small Web Audio drum machine. Every voice is synthesised from oscillators
 * and a shared noise buffer, so the example needs no sample downloads.
 */
export class DrumSynth {
  readonly context: AudioContext;
  private readonly output: GainNode;
  private readonly noise: AudioBuffer;

  constructor() {
    this.context = new AudioContext({ latencyHint: "interactive" });
    const compressor = this.context.createDynamicsCompressor();
    compressor.threshold.value = -12;
    compressor.ratio.value = 4;
    this.output = this.context.createGain();
    this.output.gain.value = 0.8;
    this.output.connect(compressor).connect(this.context.destination);
    this.noise = this.context.createBuffer(1, this.context.sampleRate, this.context.sampleRate);
    const channel = this.noise.getChannelData(0);
    for (let index = 0; index < channel.length; index += 1) channel[index] = Math.random() * 2 - 1;
  }

  resume() {
    return this.context.resume();
  }

  close() {
    return this.context.close();
  }

  /** Plays one hit. `step` lets the pitched voices follow a simple riff. */
  trigger(instrument: Instrument, time: number, level: number, step: number) {
    if (level <= 0) return;
    switch (instrument) {
      case "kick":
        return this.sweep("sine", 150, 45, 0.12, 0.4, level * 1.1, time);
      case "tom":
        return this.sweep("sine", 190, 95, 0.2, 0.35, level * 0.8, time);
      case "snare":
        this.sweep("triangle", 200, 160, 0.05, 0.12, level * 0.5, time);
        return this.noiseHit({ type: "highpass", frequency: 1200 }, 0.18, level * 0.7, time);
      case "clap":
        for (const offset of [0, 0.012, 0.024])
          this.noiseHit({ type: "bandpass", frequency: 1400 }, 0.02, level * 0.5, time + offset);
        return this.noiseHit({ type: "bandpass", frequency: 1400 }, 0.2, level * 0.6, time + 0.036);
      case "closed_hat":
        return this.noiseHit({ type: "highpass", frequency: 7500 }, 0.05, level * 0.45, time);
      case "open_hat":
        return this.noiseHit({ type: "highpass", frequency: 7000 }, 0.32, level * 0.4, time);
      case "bass":
        return this.note("sawtooth", BASS_NOTES[Math.floor(step / 8) % BASS_NOTES.length]!, {
          time,
          level: level * 0.55,
          decay: 0.28,
          cutoff: 500,
        });
      case "lead":
        return this.note("square", LEAD_NOTES[step % LEAD_NOTES.length]!, {
          time,
          level: level * 0.22,
          decay: 0.22,
          cutoff: 2400,
        });
    }
  }

  private envelope(time: number, level: number, decay: number) {
    const gain = this.context.createGain();
    gain.gain.setValueAtTime(level, time);
    gain.gain.exponentialRampToValueAtTime(0.0001, time + decay);
    gain.connect(this.output);
    return gain;
  }

  private sweep(
    type: OscillatorType,
    from: number,
    to: number,
    sweep: number,
    decay: number,
    level: number,
    time: number,
  ) {
    const oscillator = this.context.createOscillator();
    oscillator.type = type;
    oscillator.frequency.setValueAtTime(from, time);
    oscillator.frequency.exponentialRampToValueAtTime(to, time + sweep);
    oscillator.connect(this.envelope(time, level, decay));
    oscillator.start(time);
    oscillator.stop(time + decay + 0.05);
  }

  private noiseHit(
    filter: { type: BiquadFilterType; frequency: number },
    decay: number,
    level: number,
    time: number,
  ) {
    const source = this.context.createBufferSource();
    source.buffer = this.noise;
    const biquad = this.context.createBiquadFilter();
    biquad.type = filter.type;
    biquad.frequency.value = filter.frequency;
    source.connect(biquad).connect(this.envelope(time, level, decay));
    source.start(time, Math.random() * 0.5);
    source.stop(time + decay + 0.05);
  }

  private note(
    type: OscillatorType,
    frequency: number,
    options: { time: number; level: number; decay: number; cutoff: number },
  ) {
    const oscillator = this.context.createOscillator();
    oscillator.type = type;
    oscillator.frequency.value = frequency;
    const lowpass = this.context.createBiquadFilter();
    lowpass.type = "lowpass";
    lowpass.frequency.value = options.cutoff;
    oscillator.connect(lowpass).connect(this.envelope(options.time, options.level, options.decay));
    oscillator.start(options.time);
    oscillator.stop(options.time + options.decay + 0.05);
  }
}

// A minor pentatonic riff: roots change every half bar, the lead walks the scale.
const BASS_NOTES = [55, 55, 65.41, 49];
const LEAD_NOTES = [440, 523.25, 587.33, 659.25, 783.99, 659.25, 587.33, 523.25];

export type PlaybackTrack = {
  instrument: Instrument;
  volume: number;
  muted: boolean;
  solo: boolean;
  /** Enabled flags by step position, for the playing pattern. */
  steps: boolean[];
};

export type PlaybackSnapshot = {
  transport: TransportState;
  length: number;
  tracks: PlaybackTrack[];
};

const LOOKAHEAD_MS = 120;
const LATE_TOLERANCE_MS = 30;
const TICK_MS = 25;

/** Whether a track is heard, following the usual solo-over-mute rule. */
export function isAudible(track: Pick<PlaybackTrack, "muted" | "solo">, anySolo: boolean) {
  return anySolo ? track.solo && !track.muted : !track.muted;
}

/** Converts a 0–100 volume to gain on a rough perceptual curve. */
export function volumeToGain(volume: number) {
  const clamped = Math.min(100, Math.max(0, volume)) / 100;
  return clamped * clamped;
}

/**
 * Look-ahead scheduler: every 25 ms it queues the steps starting in the next
 * 120 ms on the audio clock. It reads the latest shared snapshot on each
 * tick, so pad edits from any bandmate are heard on the next pass.
 */
export class PlaybackScheduler {
  private timer: ReturnType<typeof setInterval> | undefined;
  private lastScheduled: { key: string; step: number } | undefined;

  constructor(
    private readonly synth: DrumSynth,
    private readonly snapshot: () => PlaybackSnapshot,
  ) {}

  start() {
    if (this.timer) return;
    this.timer = setInterval(() => this.tick(), TICK_MS);
  }

  stop() {
    clearInterval(this.timer);
    this.timer = undefined;
    this.lastScheduled = undefined;
  }

  private tick() {
    const { transport, length, tracks } = this.snapshot();
    if (!transport.playing || length <= 0) {
      this.lastScheduled = undefined;
      return;
    }
    const key = `${transport.anchorAt}:${transport.anchorStep}:${transport.tempo}:${transport.patternId}`;
    const now = Date.now();
    const audioNow = this.synth.context.currentTime;
    const anySolo = tracks.some((track) => track.solo);
    let step =
      this.lastScheduled?.key === key
        ? this.lastScheduled.step + 1
        : absoluteStepAt(transport, now);
    for (; stepStartMs(transport, step) < now + LOOKAHEAD_MS; step += 1) {
      const startsAt = stepStartMs(transport, step);
      this.lastScheduled = { key, step };
      if (startsAt < now - LATE_TOLERANCE_MS) continue;
      const time = Math.max(audioNow, audioNow + (startsAt - now) / 1000);
      const position = wrapStep(step, length);
      for (const track of tracks) {
        if (!track.steps[position] || !isAudible(track, anySolo)) continue;
        this.synth.trigger(track.instrument, time, volumeToGain(track.volume), position);
      }
    }
  }
}
