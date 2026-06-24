import { useAll, useSession } from "jazz-tools/react";
import { app, type Room } from "../../schema.js";

type RoomWithLastAccessed = Room & {
  lastAccessedAt: Date;
};

export function useRooms(): RoomWithLastAccessed[] {
  const session = useSession();
  const sessionUserId = session?.user_id ?? null;
  const participants = useAll(
    sessionUserId ? app.roomParticipants.where({ session_user_id: sessionUserId }) : undefined,
  );
  const rooms = useAll(app.rooms) ?? [];

  if (!participants) return [];

  const roomById = new Map(rooms.map((room) => [room.id, room]));
  return participants
    .map((participant) => {
      const room = roomById.get(participant.room_id);
      if (!room) return null;
      return { ...room, lastAccessedAt: participant.lastAccessedAt };
    })
    .filter((room): room is RoomWithLastAccessed => room !== null)
    .sort((a, b) => b.lastAccessedAt.getTime() - a.lastAccessedAt.getTime());
}
