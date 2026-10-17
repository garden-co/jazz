import type { StopStatus } from "../schema.js";
import {
  defaultBandName,
  descriptions,
  privateNotes,
  venues,
  type SeedVenue,
} from "./seed-data.js";

/** The demo tour always uses this seed, so every fresh install shows the same tour. */
export const DEFAULT_SEED = 1917;

export interface FixtureStop {
  venue: SeedVenue;
  date: Date;
  status: StopStatus;
  publicDescription: string;
  privateNote?: string;
}

export interface TourFixture {
  bandName: string;
  stops: FixtureStop[];
}

/** mulberry32: a tiny seedable PRNG returning floats in [0, 1). */
export function createRng(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function shuffle<T>(items: readonly T[], rand: () => number): T[] {
  const out = [...items];
  for (let i = out.length - 1; i > 0; i--) {
    const j = Math.floor(rand() * (i + 1));
    [out[i], out[j]] = [out[j], out[i]];
  }
  return out;
}

function pick<T>(items: readonly T[], rand: () => number): T {
  return items[Math.floor(rand() * items.length)];
}

function pickStatus(rand: () => number): StopStatus {
  const r = rand();
  if (r < 0.7) return "confirmed";
  if (r < 0.95) return "tentative";
  return "cancelled";
}

/**
 * Builds the demo tour: `stopCount` evening shows on distinct days within
 * `days` of `start`, routed west to east. The output depends only on the
 * arguments, so the same seed and start date always give the same tour.
 */
export function buildTourFixture({
  seed = DEFAULT_SEED,
  start,
  stopCount = 12,
  days = 21,
}: {
  seed?: number;
  start: Date;
  stopCount?: number;
  days?: number;
}): TourFixture {
  const rand = createRng(seed);
  const count = Math.min(stopCount, days, venues.length);

  const dayOffsets = shuffle(
    Array.from({ length: days }, (_, i) => i),
    rand,
  )
    .slice(0, count)
    .sort((a, b) => a - b);
  const route = shuffle(venues, rand)
    .slice(0, count)
    .sort((a, b) => a.lng - b.lng);

  const stops = dayOffsets.map((offset, i): FixtureStop => {
    const hour = 18 + Math.floor(rand() * 4);
    const date = new Date(start.getFullYear(), start.getMonth(), start.getDate() + offset, hour);
    const status = i === 0 ? "confirmed" : pickStatus(rand);
    const publicDescription = pick(descriptions, rand);
    const privateNote = rand() < 0.7 ? pick(privateNotes, rand) : undefined;
    return { venue: route[i], date, status, publicDescription, privateNote };
  });

  return { bandName: defaultBandName, stops };
}
