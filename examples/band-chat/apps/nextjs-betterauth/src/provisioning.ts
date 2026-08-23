import { app } from "../schema";
import { demoRoom } from "./fixture";
import type { Db } from "jazz-tools";

type ProvisionDb = Pick<Db, "insert">;

/** Explicit, idempotent example provisioning. Never call this from a read hook. */
export function provisionDemo(
  db: ProvisionDb,
  userId: string,
  existing: { profileId?: string; roomId?: string },
): string {
  if (existing.roomId) return existing.roomId;
  const profileId =
    existing.profileId ??
    db.insert(app.profiles, { userId, displayName: "Local musician" }).value.id;
  const room = db.insert(app.rooms, { name: demoRoom.name }).value;
  db.insert(app.roomMembers, { roomId: room.id, userId });
  db.insert(app.messages, {
    roomId: room.id,
    senderId: profileId,
    text: demoRoom.welcome,
  });
  return room.id;
}
