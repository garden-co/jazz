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

/**
 * Ensure instruments are seeded.
 * Checks both local OPFS and server-synced data before inserting.
 */
export async function ensureInstrumentsSeeded(db: Db): Promise<void> {
  // Check local first (fast path)
  const local = await db.all(app.instruments, "worker");
  if (local.length > 0) return;

  // Wait briefly for server-synced instruments to arrive
  const hasServerData = await new Promise<boolean>((resolve) => {
    let settled = false;
    let unsubFn: (() => void) | null = null;

    function cleanup() {
      if (unsubFn) {
        unsubFn();
        unsubFn = null;
      }
    }

    unsubFn = db.subscribeAll(app.instruments, (delta) => {
      if (!settled && delta.all.length > 0) {
        settled = true;
        // Defer cleanup to avoid calling unsub before it's assigned
        queueMicrotask(cleanup);
        resolve(true);
      }
    });

    // If the callback already resolved synchronously, clean up
    if (settled) {
      cleanup();
    } else {
      setTimeout(() => {
        if (!settled) {
          settled = true;
          cleanup();
          resolve(false);
        }
      }, 2000);
    }
  });

  if (hasServerData) return;

  for (const seed of SEED_INSTRUMENTS) {
    const res = await fetch(seed.file);
    const buffer = await res.arrayBuffer();
    db.insert(app.instruments, {
      name: seed.name,
      sound: new Uint8Array(buffer),
      display_order: seed.display_order,
    });
  }
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

  const existing = await db.all(app.jams.where({ created_at: now }).limit(1), "worker");
  if (existing.length > 0) {
    return existing[0].id;
  }

  return db.insert(app.jams, { created_at: now, bpm: 95, beat_count: 16 });
}
