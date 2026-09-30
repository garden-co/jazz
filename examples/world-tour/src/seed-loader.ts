import type { Db, TransactionScope } from "jazz-tools";
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
 * Creates the demo band on an empty server.
 *
 * The band insert runs in an exclusive transaction with the fixed DEMO_BAND_ID, and
 * an exclusive insert with an explicit id is create-only: when two first visitors
 * race, the server commits one band and rejects the other, whose app then shows
 * the winner's tour. The same rejection happens if the demo band was ever deleted,
 * so an empty server whose demo band is gone stays empty. Resolves to false when
 * the claim is rejected.
 *
 * The rest of the tour is written in one transaction after the claim commits (see
 * `stageTour`). If that transaction is lost (the tab closes before it syncs) or
 * rejected, the band stays without a tour and is not reseeded; the caller only
 * reports the error.
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
  const tour = await db.transaction((tx) =>
    stageTour(tx, DEMO_BAND_ID, { userId, ownerName }, fixture),
  );
  await tour.wait({ tier: "global" });
  return true;
}

/** Creates a new band owned by `userId` with the seeded demo tour starting today. */
export async function startDemoTour(
  db: Db,
  { userId, ownerName, seed = DEFAULT_SEED }: { userId: string; ownerName: string; seed?: number },
): Promise<string> {
  const fixture = buildTourFixture({ seed, start: new Date() });
  const tour = await db.transaction((tx) => {
    const band = tx.insert(app.bands, { name: fixture.bandName, ownerId: userId });
    stageTour(tx, band.id, { userId, ownerName }, fixture);
    return band.id;
  });
  await tour.wait({ tier: "global" });
  return tour.value;
}

/**
 * Stages the owner's membership, an invite, and the tour for a band into one
 * transaction. The membership and invite need the band, the venues need the
 * membership, the stops need their venues, and the notes need their stops; each
 * row's policy sees the rows staged before it in the same transaction.
 */
function stageTour(
  tx: TransactionScope<"mergeable">,
  bandId: string,
  { userId, ownerName }: { userId: string; ownerName: string },
  fixture: ReturnType<typeof buildTourFixture>,
): void {
  tx.insert(app.members, { bandId, userId, name: ownerName });
  tx.insert(app.bandInvites, { bandId, code: newInviteCode() });

  // Each band gets its own venues: a shared venue could be moved or deleted by
  // another band, taking this band's stops with it.
  const venueIds = new Map<string, string>();
  for (const { venue } of fixture.stops) {
    if (!venueIds.has(venue.name))
      venueIds.set(venue.name, tx.insert(app.venues, { ...venue, ownerId: userId, bandId }).id);
  }

  for (const stop of fixture.stops) {
    const row = tx.insert(app.stops, {
      bandId,
      venueId: venueIds.get(stop.venue.name)!,
      date: stop.date,
      status: stop.status,
      publicDescription: stop.publicDescription,
    });
    if (stop.privateNote)
      tx.insert(app.stopNotes, { stopId: row.id, bandId, body: stop.privateNote });
  }
}
