import type { Db } from "jazz-tools";
import { app } from "../schema.js";
import { buildTourFixture, DEFAULT_SEED } from "./fixture.js";

/** A fresh invite code. Codes are bearer secrets, so keep the full UUID's entropy. */
export function newInviteCode(): string {
  return crypto.randomUUID();
}

/**
 * Creates a band owned by `userId`, with the owner's membership, an invite code,
 * and the seeded demo tour starting today. Each step waits for the server so the
 * next step's permission check (membership, band ownership) can see it.
 */
export async function startDemoTour(
  db: Db,
  { userId, ownerName, seed = DEFAULT_SEED }: { userId: string; ownerName: string; seed?: number },
): Promise<string> {
  const fixture = buildTourFixture({ seed, start: new Date() });

  const band = db.insert(app.bands, { name: fixture.bandName, ownerId: userId });
  await band.wait({ tier: "global" });
  const bandId = band.value.id;

  await Promise.all([
    db.insert(app.members, { bandId, userId, name: ownerName }).wait({ tier: "global" }),
    db.insert(app.bandInvites, { bandId, code: newInviteCode() }).wait({ tier: "global" }),
  ]);

  // Venues are shared: reuse one that already exists with the same name.
  const existing = new Map((await db.all(app.venues)).map((v) => [v.name, v.id]));
  const venueIds = new Map<string, string>();
  const venueWrites: Promise<unknown>[] = [];
  for (const { venue } of fixture.stops) {
    const id = existing.get(venue.name);
    if (id) {
      venueIds.set(venue.name, id);
      continue;
    }
    const handle = db.insert(app.venues, { ...venue, ownerId: userId, bandId });
    venueIds.set(venue.name, handle.value.id);
    venueWrites.push(handle.wait({ tier: "global" }));
  }
  await Promise.all(venueWrites);

  await Promise.all(
    fixture.stops.map(async (stop) => {
      const handle = db.insert(app.stops, {
        bandId,
        venueId: venueIds.get(stop.venue.name)!,
        date: stop.date,
        status: stop.status,
        publicDescription: stop.publicDescription,
      });
      if (!stop.privateNote) return;
      await handle.wait({ tier: "global" });
      db.insert(app.stopNotes, { stopId: handle.value.id, bandId, body: stop.privateNote });
    }),
  );

  return bandId;
}
