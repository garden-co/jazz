import type { Db } from "jazz-tools";
import { app } from "./schema.js";

export const workloadShape = { rooms: 24, membersPerRoom: 4, messagesPerRoom: 40 } as const;

export function seedBandChat(db: Db) {
  const profiles = Array.from(
    { length: workloadShape.membersPerRoom },
    (_, index) =>
      db.insert(app.profiles, {
        userId: `fixture-user-${index}`,
        displayName: `Player ${index + 1}`,
      }).value,
  );
  const rooms = Array.from({ length: workloadShape.rooms }, (_, roomIndex) => {
    const room = db.insert(app.rooms, {
      name: `Stage ${String(roomIndex + 1).padStart(2, "0")}`,
    }).value;
    for (const profile of profiles)
      db.insert(app.roomMembers, { roomId: room.id, userId: profile.userId });
    for (let messageIndex = 0; messageIndex < workloadShape.messagesPerRoom; messageIndex++) {
      db.insert(app.messages, {
        roomId: room.id,
        senderId: profiles[messageIndex % profiles.length]!.id,
        text: `fixture-${roomIndex}-${messageIndex}`,
      });
    }
    return room;
  });
  return { profiles, rooms };
}
