import { app } from "../schema/app.js";
import { bandName, venues, stops } from "./seed-data.js";

/**
 * Seeds the Jazz database with tour data if no bands exist yet.
 * Accepts the db instance from useDb() — typed loosely to avoid
 * importing the Db class which isn't in the public Vue exports.
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export async function seedIfEmpty(db: any, userId?: string): Promise<void> {
  const existingBands = await db.all(app.bands);
  if (existingBands.length > 0) return;

  const band = db.insert(app.bands, { name: bandName });

  if (userId) {
    db.insert(app.members, { bandId: band.id, userId });
  }

  const insertedVenues = venues.map((v) => db.insert(app.venues, v));

  for (const stop of stops) {
    const venue = insertedVenues[stop.venueIndex];
    if (!venue) continue;

    db.insert(app.stops, {
      bandId: band.id,
      venueId: venue.id,
      date: new Date(stop.date),
      status: stop.status,
      publicDescription: stop.publicDescription,
      privateNotes: stop.privateNotes,
    });
  }
}
