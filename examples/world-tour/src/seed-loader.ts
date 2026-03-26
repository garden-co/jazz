import { app } from "../schema/app.js";
import {
  defaultBandName,
  venues as seedVenues,
  descriptions,
  privateNotes,
} from "./seed-data.js";

function seededRandom(seed: number): () => number {
  let s = seed;
  return () => {
    s = (s * 16807 + 0) % 2147483647;
    return (s - 1) / 2147483646;
  };
}

function pickWeightedStatus(rand: number): "confirmed" | "tentative" | "cancelled" {
  if (rand < 0.7) return "confirmed";
  if (rand < 0.95) return "tentative";
  return "cancelled";
}

export async function ensureData(
  db: any,
  userId: string | undefined,
  isMember: boolean,
): Promise<void> {
  const existingBands = await db.all(app.bands);
  let bandId: string;

  if (existingBands.length === 0) {
    const band = db.insert(app.bands, { name: defaultBandName });
    bandId = band.id;
  } else {
    bandId = existingBands[0].id;
  }

  if (userId && isMember) {
    const myMembership = await db.all(
      app.members.where({ userId }),
    );
    if (myMembership.length === 0) {
      db.insert(app.members, { bandId, userId });
    }
  }

  const existingVenues = await db.all(app.venues);
  const existingNames = new Set(existingVenues.map((v: any) => v.name));
  const insertedVenues: { id: string }[] = [];
  for (const v of seedVenues) {
    if (!existingNames.has(v.name)) {
      try {
        const result = db.insert(app.venues, v);
        if (result?.id) insertedVenues.push(result);
      } catch {
        // Permission denied or other error — skip
      }
    }
  }

  const allVenues = [...existingVenues, ...insertedVenues];
  console.log("[ensureData] venues:", existingVenues.length, "existing,", insertedVenues.length, "inserted,", allVenues.length, "total");
  if (allVenues.length > 0) {
    console.log("[ensureData] sample venue:", JSON.stringify(Object.keys(allVenues[0])), "id:", allVenues[0].id);
  }

  if (!isMember) return;

  const now = new Date();
  const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  const threeWeeks = new Date(today.getTime() + 21 * 24 * 60 * 60 * 1000);

  const upcomingStops = await db.all(
    app.stops
      .where({ date: { gte: today, lte: threeWeeks } })
      .limit(12),
  );

  const needed = 12 - upcomingStops.length;
  const myMembers = await db.all(app.members.where({ userId }));
  console.log("[ensureData] member rows for user:", myMembers.length, "userId:", userId);
  const allStops = await db.all(app.stops);
  console.log("[ensureData] total stops in DB:", allStops.length, "upcoming (date filtered):", upcomingStops.length, "needed:", needed);
  console.log("[ensureData] date range:", today.toISOString(), "to", threeWeeks.toISOString());
  if (allStops.length > 0) {
    console.log("[ensureData] first stop date:", allStops[0].date, "status:", allStops[0].status);
  }
  if (needed <= 0) return;

  if (allVenues.length === 0) return;

  const existingDates = new Set(
    upcomingStops.map((s: any) => {
      const d = s.date instanceof Date ? s.date : new Date(s.date);
      return `${d.getFullYear()}-${d.getMonth()}-${d.getDate()}`;
    }),
  );

  const rand = Math.random;
  const availableDays: Date[] = [];
  for (let i = 0; i < 21; i++) {
    const d = new Date(today.getTime() + i * 24 * 60 * 60 * 1000);
    const key = `${d.getFullYear()}-${d.getMonth()}-${d.getDate()}`;
    if (!existingDates.has(key)) {
      availableDays.push(d);
    }
  }

  // Shuffle available days then pick `needed`
  for (let i = availableDays.length - 1; i > 0; i--) {
    const j = Math.floor(rand() * (i + 1));
    [availableDays[i], availableDays[j]] = [availableDays[j], availableDays[i]];
  }
  const pickedDays = availableDays.slice(0, needed).sort(
    (a, b) => a.getTime() - b.getTime(),
  );

  // Pick random venues, sort by longitude for a believable west-to-east route
  const shuffledVenues = [...allVenues].sort(() => rand() - 0.5);
  const pickedVenues = shuffledVenues.slice(0, needed).sort(
    (a: any, b: any) => (a.lng ?? 0) - (b.lng ?? 0),
  );

  for (let i = 0; i < pickedDays.length; i++) {
    const day = pickedDays[i];
    const venue = pickedVenues[i % pickedVenues.length];
    if (!venue?.id) continue;

    const hour = 18 + Math.floor(rand() * 4);
    day.setHours(hour, 0, 0, 0);

    db.insert(app.stops, {
      bandId,
      venueId: venue.id,
      date: day,
      status: pickWeightedStatus(rand()),
      publicDescription: descriptions[Math.floor(rand() * descriptions.length)],
      privateNotes: rand() > 0.3
        ? privateNotes[Math.floor(rand() * privateNotes.length)]
        : undefined,
    });
  }
}
