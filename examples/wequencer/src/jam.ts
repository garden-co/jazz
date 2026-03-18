import type { Db } from "jazz-tools";
import { app } from "../schema/app.js";

export const SEED_INSTRUMENTS = [
  { name: "Kick", file: "/kick.mp3", display_order: 0 },
  { name: "Snare", file: "/snare.mp3", display_order: 1 },
  { name: "Hi-hat", file: "/hihat-open.mp3", display_order: 2 },
  { name: "Piano 1", file: "/fsharpminor7.mp3", display_order: 3 },
  { name: "Piano 2", file: "/b13.mp3", display_order: 4 },
  { name: "Guitar 1", file: "/emaj7.mp3", display_order: 5 },
  { name: "Guitar 2", file: "/amaj7.mp3", display_order: 6 },
] as const;

export const SEEDED_STORAGE_KEY = "wequencer-seeded";

/** Returns the seeds whose names are absent from the given set. */
export function missingSeeds(existingNames: Set<string>) {
  return SEED_INSTRUMENTS.filter((s) => !existingNames.has(s.name));
}

export async function ensureInstrumentsSeeded(db: Db): Promise<void> {
  // Return visits on the same device: skip the network round-trip entirely.
  // This makes the app fully offline-capable after the first successful seed.
  if (localStorage.getItem(SEEDED_STORAGE_KEY)) return;

  // First visit on this device: wait for edge confirmation before deciding what
  // to insert. Without deterministic IDs in the Jazz API we cannot make
  // concurrent inserts from two fresh clients idempotent at the DB layer, so
  // tier: 'edge' remains the dedup guard here.
  const existing = await db.all(app.instruments, { tier: "edge" });
  const existingNames = new Set(existing.map((i) => i.name));

  for (const seed of missingSeeds(existingNames)) {
    const res = await fetch(seed.file);
    const blob = await res.blob();
    const file = await db.createFileFromBlob(app, blob, { tier: "edge" });
    db.insert(app.instruments, {
      name: seed.name,
      soundFileId: file.id,
      display_order: seed.display_order,
    });
  }

  localStorage.setItem(SEEDED_STORAGE_KEY, "1");
}

/** Floor the current time to the nearest minute. */
export function currentMinuteDate(): Date {
  const epochMs = Math.floor(Date.now() / 60_000) * 60_000;
  return new Date(epochMs);
}

/**
 * Get or create a jam for the current minute.
 * Returns the jam ID.
 */
export async function getCurrentJam(db: Db): Promise<string> {
  const now = currentMinuteDate();

  const existing = await db.all(app.jams.where({ created_at: now }).limit(1));
  if (existing.length > 0) {
    return existing[0].id;
  }

  const jam = db.insert(app.jams, { created_at: now, bpm: 95, beat_count: 16 });
  return jam.id;
}
