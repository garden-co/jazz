import type { Db } from "jazz-tools";
import { app } from "../schema.js";
import { buildTourFixture, DEFAULT_SEED } from "./fixture.js";

/**
 * The band an empty server's first visitor creates. A fixed id lets the server
 * accept exactly one of two concurrent first visits (see `claimDemoBand`).
 */
export const DEMO_BAND_ID = "1917d0e0-5eed-4b0d-8a4d-000000001917";

/** A fresh invite code. Codes are bearer secrets, so keep the full UUID's entropy. */
export function newInviteCode(): string {
  return crypto.randomUUID();
}

/**
 * Creates the demo band on an empty server. The insert runs in an exclusive
 * transaction with a fixed id, so when two first visitors race, the server commits
 * one band and rejects the other; the loser then sees the winner's tour.
 * Resolves to false when this visitor lost (or the demo band was deleted).
 */
export async function claimDemoBand(
  db: Db,
  { userId, ownerName }: { userId: string; ownerName: string },
): Promise<boolean> {
  const fixture = buildTourFixture({ seed: DEFAULT_SEED, start: new Date() });
  try {
    const claim = await db.exclusiveTransaction((tx) =>
      tx.insert(app.bands, { name: fixture.bandName, ownerId: userId }, { id: DEMO_BAND_ID }),
    );
    await claim.wait();
  } catch {
    return false;
  }
  await writeTour(db, DEMO_BAND_ID, { userId, ownerName }, fixture);
  return true;
}

/** Creates a new band owned by `userId` with the seeded demo tour starting today. */
export async function startDemoTour(
  db: Db,
  { userId, ownerName, seed = DEFAULT_SEED }: { userId: string; ownerName: string; seed?: number },
): Promise<string> {
  const fixture = buildTourFixture({ seed, start: new Date() });
  const band = db.insert(app.bands, { name: fixture.bandName, ownerId: userId });
  await band.wait({ tier: "global" });
  await writeTour(db, band.value.id, { userId, ownerName }, fixture);
  return band.value.id;
}

/**
 * Writes the owner's membership, an invite, and the tour for a band that exists
 * on the server.
 *
 * Three transactions rather than one: permission `exists` checks only see
 * committed rows, not rows staged earlier in the same transaction (INV-RLS-9).
 * The membership and invite inserts need the band, the venue and stop inserts
 * need the membership, and the notes need their stops, so each group commits
 * before the next. Whether `exists` should see a transaction's own writes is an
 * open question for the core team; if it does, this becomes one transaction.
 */
async function writeTour(
  db: Db,
  bandId: string,
  { userId, ownerName }: { userId: string; ownerName: string },
  fixture: ReturnType<typeof buildTourFixture>,
): Promise<void> {
  const access = await db.transaction((tx) => {
    tx.insert(app.members, { bandId, userId, name: ownerName });
    tx.insert(app.bandInvites, { bandId, code: newInviteCode() });
  });
  await access.wait({ tier: "global" });

  // Each band gets its own venues: a shared venue could be moved or deleted by
  // another band, taking this band's stops with it.
  const tour = await db.transaction((tx) => {
    const venueIds = new Map<string, string>();
    return fixture.stops.map((stop) => {
      let venueId = venueIds.get(stop.venue.name);
      if (!venueId) {
        venueId = tx.insert(app.venues, { ...stop.venue, ownerId: userId, bandId }).id;
        venueIds.set(stop.venue.name, venueId);
      }
      const row = tx.insert(app.stops, {
        bandId,
        venueId,
        date: stop.date,
        status: stop.status,
        publicDescription: stop.publicDescription,
      });
      return { stopId: row.id, note: stop.privateNote };
    });
  });
  await tour.wait({ tier: "global" });

  const notes = tour.value.filter((s): s is { stopId: string; note: string } => !!s.note);
  if (notes.length === 0) return;
  const written = await db.transaction((tx) => {
    for (const { stopId, note } of notes) tx.insert(app.stopNotes, { stopId, bandId, body: note });
  });
  await written.wait({ tier: "global" });
}
