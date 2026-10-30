/**
 * Deterministic, dependency-free audio for the demo library.
 *
 * Every demo track is a short 16-bit mono PCM WAV synthesised from a seed, so
 * the example works without uploads and produces identical bytes on every
 * device. The files are deliberately small (a few hundred KiB) but still large
 * enough to be read back in several range windows by the player.
 */

export const DEMO_SAMPLE_RATE = 8_000;

export type DemoTrackSpec = {
  title: string;
  /** Semitone offsets from `rootHz`, played in sequence and looped. */
  pattern: number[];
  rootHz: number;
  beatMs: number;
  durationMs: number;
};

export type DemoAlbumSpec = {
  title: string;
  artist: string;
  tracks: DemoTrackSpec[];
};

export const DEMO_LIBRARY: DemoAlbumSpec[] = [
  {
    title: "Sine studies",
    artist: "The oscillators",
    tracks: [
      {
        title: "Morning tone",
        pattern: [0, 4, 7, 12],
        rootHz: 220,
        beatMs: 400,
        durationMs: 12_000,
      },
      {
        title: "Fifths",
        pattern: [0, 7, 0, 7, 5, 12],
        rootHz: 196,
        beatMs: 350,
        durationMs: 10_000,
      },
      {
        title: "Slow minor",
        pattern: [0, 3, 7, 10],
        rootHz: 174.6,
        beatMs: 600,
        durationMs: 14_000,
      },
    ],
  },
  {
    title: "Night shift",
    artist: "Quiet carrier",
    tracks: [
      {
        title: "Dial tone blues",
        pattern: [0, 3, 5, 6, 7, 10],
        rootHz: 146.8,
        beatMs: 300,
        durationMs: 11_000,
      },
      { title: "Relay", pattern: [12, 7, 3, 0], rootHz: 261.6, beatMs: 250, durationMs: 9_000 },
      {
        title: "Last train",
        pattern: [0, 5, 9, 12, 9, 5],
        rootHz: 164.8,
        beatMs: 450,
        durationMs: 13_000,
      },
      { title: "Standby", pattern: [0, 12], rootHz: 110, beatMs: 800, durationMs: 10_000 },
    ],
  },
  {
    title: "Local first",
    artist: "Sync ensemble",
    tracks: [
      {
        title: "Offline",
        pattern: [0, 2, 4, 7, 9],
        rootHz: 293.7,
        beatMs: 280,
        durationMs: 10_000,
      },
      {
        title: "Reconnect",
        pattern: [9, 7, 4, 2, 0],
        rootHz: 293.7,
        beatMs: 280,
        durationMs: 10_000,
      },
      {
        title: "Converge",
        pattern: [0, 4, 7, 11, 12, 11, 7, 4],
        rootHz: 246.9,
        beatMs: 220,
        durationMs: 12_000,
      },
    ],
  },
];

/** Synthesises a mono 16-bit PCM WAV for a demo track. */
export function synthesizeWav(spec: DemoTrackSpec, sampleRate = DEMO_SAMPLE_RATE): Uint8Array {
  const sampleCount = Math.round((spec.durationMs / 1000) * sampleRate);
  const dataBytes = sampleCount * 2;
  const bytes = new Uint8Array(44 + dataBytes);
  const view = new DataView(bytes.buffer);
  writeAscii(bytes, 0, "RIFF");
  view.setUint32(4, 36 + dataBytes, true);
  writeAscii(bytes, 8, "WAVE");
  writeAscii(bytes, 12, "fmt ");
  view.setUint32(16, 16, true);
  view.setUint16(20, 1, true); // PCM
  view.setUint16(22, 1, true); // mono
  view.setUint32(24, sampleRate, true);
  view.setUint32(28, sampleRate * 2, true);
  view.setUint16(32, 2, true);
  view.setUint16(34, 16, true);
  writeAscii(bytes, 36, "data");
  view.setUint32(40, dataBytes, true);

  const beatSamples = Math.max(1, Math.round((spec.beatMs / 1000) * sampleRate));
  const fadeSamples = Math.min(sampleRate / 2, sampleCount / 4);
  for (let i = 0; i < sampleCount; i++) {
    const beat = Math.floor(i / beatSamples);
    const inBeat = (i % beatSamples) / beatSamples;
    const semitone = spec.pattern[beat % spec.pattern.length]!;
    const hz = spec.rootHz * 2 ** (semitone / 12);
    const t = i / sampleRate;
    // A plucked envelope per note plus a short fade in/out for the whole track.
    const note = Math.exp(-3.5 * inBeat) * Math.min(1, inBeat * 40);
    const edge = Math.min(1, i / fadeSamples, (sampleCount - i) / fadeSamples);
    const tone = Math.sin(2 * Math.PI * hz * t) + 0.3 * Math.sin(4 * Math.PI * hz * t);
    const sample = Math.round(tone * note * edge * 0.45 * 32767);
    view.setInt16(44 + i * 2, Math.max(-32768, Math.min(32767, sample)), true);
  }
  return bytes;
}

function writeAscii(bytes: Uint8Array, offset: number, text: string) {
  for (let i = 0; i < text.length; i++) bytes[offset + i] = text.charCodeAt(i);
}
