import "server-only";
import { app } from "@/schema";
import { addDays } from "@/src/agent/tools";
import { queueAssistantTurn } from "@/src/agent/runner";
import { backendJazzClient } from "@/src/lib/backend-jazz-client";
import { retryOnConflict } from "@/src/lib/retry";
import { demoRoughMix } from "./demo-audio";

const SONGS = [
  ["Blue Hour Overture", 312, "medium"],
  ["Lake Shore Drive at 3am", 408, "low"],
  ["Brass Weather", 265, "high"],
  ["Second City Stomp", 241, "high"],
  ["Paper Lanterns", 356, "low"],
  ["The Long Way Round", 298, "medium"],
  ["Night Bus Samba", 274, "high"],
  ["Quiet Streetlights", 330, "low"],
  ["Uptown Ledger", 287, "medium"],
  ["Last Call Blues", 395, "medium"],
] as const;

const VENUES = [
  ["The Green Room", "Chicago", 120, "listening room, seated", "bookings@greenroom.example"],
  ["Hyde Park Hall", "Chicago", 450, "concert hall, all ages", "programming@hydeparkhall.example"],
  ["Lantern Club", "Chicago", 220, "late-night club, standing", "talent@lanternclub.example"],
  ["Motor Row Ballroom", "Detroit", 800, "ballroom, standing", "holds@motorrow.example"],
  ["Cass Corridor Cafe", "Detroit", 90, "cafe, seated", "music@casscafe.example"],
  ["Third Ward Social", "Milwaukee", 300, "club, standing", "book@thirdward.example"],
  ["Loring Park Stage", "Minneapolis", 600, "theatre, seated", "shows@loringpark.example"],
  ["Kensington Loft", "Toronto", 180, "loft, standing", "hello@kensingtonloft.example"],
] as const;

const CALENDAR = [
  [3, "show", "Record store in-store", "Chicago"],
  [6, "studio", "Mixing session", "Chicago"],
  [9, "hold", "Festival hold, second option", "Milwaukee"],
  [10, "travel", "Drive to Detroit", "Detroit"],
  [11, "show", "Motor Row Ballroom support slot", "Detroit"],
  [17, "studio", "Horn overdubs", "Chicago"],
  [24, "show", "Radio session", "Chicago"],
] as const;

export const FIRST_PROMPT =
  "Here's the rough mix of our new single. Which Chicago venues would suit a release show, and which weekend nights are we free?";

/**
 * Prepare a workspace the first time an account opens the app: its booking
 * data and a first conversation whose reply streams in live. Runs server-side
 * with backend authority and is idempotent: the profile row is the marker,
 * and the exclusive transaction makes concurrent first opens seed only once.
 */
export async function ensureWorkspace(accountId: string, authUserId: string, displayName: string) {
  const db = (await backendJazzClient()).db;
  // A concurrent first open that wins the race makes this attempt conflict; the
  // retry then finds its profile and returns null.
  return retryOnConflict(async () => {
    const write = await db.exclusiveTransaction(async (tx) => {
      const existing = await tx.all(app.profiles.where({ accountId }));
      if (existing[0]) return null;
      const ownerAccount = accountId;
      tx.insert(app.profiles, { accountId, authUserId, displayName }, { id: crypto.randomUUID() });
      const artistId = crypto.randomUUID();
      tx.insert(
        app.artists,
        { ownerAccount, name: "The Night Shift Trio", homeCity: "Chicago", genre: "jazz" },
        { id: artistId },
      );
      for (const [title, durationSeconds, energy] of SONGS)
        tx.insert(
          app.songs,
          { ownerAccount, artistId, title, durationSeconds, energy },
          { id: crypto.randomUUID() },
        );
      for (const [name, city, capacity, style, bookingContact] of VENUES)
        tx.insert(
          app.venues,
          { ownerAccount, name, city, capacity, style, bookingContact },
          { id: crypto.randomUUID() },
        );
      const today = new Date().toISOString().slice(0, 10);
      for (const [offset, kind, title, city] of CALENDAR)
        tx.insert(
          app.calendarEvents,
          { ownerAccount, artistId, date: addDays(today, offset), kind, title, city },
          { id: crypto.randomUUID() },
        );

      const conversationId = crypto.randomUUID();
      const userTurnId = crypto.randomUUID();
      tx.insert(
        app.conversations,
        {
          ownerAccount,
          artistId,
          title: "Single release show",
          headTurnId: userTurnId,
        },
        { id: conversationId },
      );
      tx.insert(
        app.turns,
        {
          conversationId,
          role: "user",
          body: FIRST_PROMPT,
          status: "complete",
        },
        { id: userTurnId },
      );
      const payload = demoRoughMix();
      tx.insert(
        app.attachments,
        {
          conversationId,
          turnId: userTurnId,
          filename: "night-shift-single-rough-mix.wav",
          mediaType: "audio/wav",
          byteLength: payload.byteLength,
          payload,
        },
        { id: crypto.randomUUID() },
      );
      return { conversationId, userTurnId };
    });
    await write.wait();
    return write.value;
  });
}

/** Seed, then queue the first reply. Returns the turn to generate, if any. */
export async function bootstrapWorkspace(
  accountId: string,
  authUserId: string,
  displayName: string,
) {
  const seeded = await ensureWorkspace(accountId, authUserId, displayName);
  if (!seeded) return undefined;
  const db = (await backendJazzClient()).db;
  const reply = await queueAssistantTurn(db, seeded.conversationId, seeded.userTurnId, {
    reuse: true,
  });
  return reply.turnId;
}
