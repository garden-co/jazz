import type { RoomSummary } from "../components/RoomNav";

/** A room's newest message, read through plain columns only. */
export interface NewestMessage {
  id: string;
  senderId: string;
  $createdAt: Date;
}

type ListedRoom = RoomSummary["room"];

/**
 * The room list: every room the reader is sent, newest message first (a room
 * with no message sorts by its own creation time). A room is unread when its
 * newest message is from someone else and newer than the reader's marker.
 * Nothing here reads a message's text or needs a key.
 *
 * `newest` has no entry for a room whose newest-message query has not
 * answered yet, and `null` for a room with no message. `lastRead` is
 * undefined until the markers have arrived: no room is unread before then.
 */
export function summariseRooms({
  rooms,
  newest,
  lastRead,
  author,
  profileId,
}: {
  rooms: readonly ListedRoom[];
  newest: ReadonlyMap<string, NewestMessage | null>;
  lastRead: ReadonlyMap<string, Date> | undefined;
  author: string;
  profileId: string;
}): RoomSummary[] {
  return rooms
    .map((room) => {
      const latest = newest.get(room.id) ?? null;
      const readAt = lastRead?.get(room.id);
      const hasUnread =
        !!lastRead &&
        !!latest &&
        latest.senderId !== profileId &&
        (!readAt || readAt.getTime() < latest.$createdAt.getTime());
      return {
        room,
        activityAt: latest?.$createdAt ?? room.$createdAt,
        readAt,
        isCreator: room.$createdBy.account === author,
        hasUnread,
      };
    })
    .sort((a, b) => b.activityAt.getTime() - a.activityAt.getTime());
}
