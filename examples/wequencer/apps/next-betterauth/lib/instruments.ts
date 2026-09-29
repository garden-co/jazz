import type { Instrument } from "@/schema";

export const MAX_TRACKS = 16;
export const MAX_STEPS = 64;
export const STEPS_PER_BEAT = 4;
export const PATTERN_LENGTHS = [16, 32, 48, 64] as const;
export const MIN_TEMPO = 60;
export const MAX_TEMPO = 200;

export const INSTRUMENTS: Array<{ value: Instrument; label: string }> = [
  { value: "kick", label: "Kick" },
  { value: "snare", label: "Snare" },
  { value: "clap", label: "Clap" },
  { value: "closed_hat", label: "Closed hat" },
  { value: "open_hat", label: "Open hat" },
  { value: "tom", label: "Tom" },
  { value: "bass", label: "Bass" },
  { value: "lead", label: "Lead" },
];

/**
 * Track colours are names of the theme's categorical data colours, stored as
 * plain strings so every client resolves them against its own light or dark
 * token values.
 */
export const TRACK_COLORS = [
  "red",
  "orange",
  "teal",
  "blue",
  "purple",
  "green",
  "pink",
  "indigo",
  "cyan",
  "brown",
] as const;

export function trackColor(position: number) {
  return TRACK_COLORS[position % TRACK_COLORS.length]!;
}

export function instrumentLabel(instrument: Instrument) {
  return INSTRUMENTS.find((option) => option.value === instrument)?.label ?? instrument;
}

/** The instrument a new track gets, cycling through the kit. */
export function instrumentForPosition(position: number): Instrument {
  return INSTRUMENTS[position % INSTRUMENTS.length]!.value;
}

/**
 * A starter groove for new sessions, one rule per instrument, so the first
 * press of Play sounds like music. Tests import it to know the seeded pattern.
 */
export function starterStep(instrument: Instrument, step: number): boolean {
  const inBar = step % 16;
  switch (instrument) {
    case "kick":
      return inBar % 4 === 0;
    case "snare":
      return inBar === 4 || inBar === 12;
    case "clap":
      return inBar === 12;
    case "closed_hat":
      return inBar % 2 === 0;
    case "open_hat":
      return inBar === 14;
    case "tom":
      return inBar === 15;
    case "bass":
      return inBar === 0 || inBar === 3 || inBar === 8 || inBar === 11;
    case "lead":
      return inBar === 6 || inBar === 10;
  }
}
