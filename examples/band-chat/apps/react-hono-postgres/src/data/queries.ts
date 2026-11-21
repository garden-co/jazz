// The app's live queries: where each one is fetched from, and which tables
// under which topic it depends on (the server's change events name them).
import type { JoinRequest, Message, Profile, ReadMarker, Reaction, Room, RoomMember, Stroke } from "../../shared/types";
import type { Live, Watch } from "./live";

// The newest page of a room's history; unread counts stop at the cap.
export const HISTORY_PAGE = 200;
export const UNREAD_CAP = 100;

const room = (id: string) => `room:${id}`;
const mine = <T,>(key: string, path: string, tables: string[], empty: T): Live<T> => ({
  key: [key],
  path,
  empty,
  watch: () => [["me", tables]],
});
const inRoom = <T,>(roomId: string, what: string, path: string, tables: string[], empty: T): Live<T> => ({
  key: ["room", roomId, what],
  path,
  empty,
  watch: (): Watch[] => [[room(roomId), tables]],
});

export const queries = {
  profiles: {
    mine: () => mine<Profile[]>("myProfile", "/profiles/mine", ["profiles"], []),
    /** Everyone this account may know: one query feeds every name. */
    known: () => mine<Profile[]>("profiles", "/profiles/known", ["profiles"], []),
  },
  rooms: { mine: () => mine<Room[]>("rooms", "/rooms", ["rooms", "roomMembers"], []) },
  roomMembers: {
    mine: () => mine<RoomMember[]>("memberships", "/memberships", ["roomMembers"], []),
    byRoom: ({ roomId }: { roomId: string }) =>
      inRoom<RoomMember[]>(roomId, "members", `/rooms/${roomId}/members`, ["roomMembers"], []),
  },
  readMarkers: { mine: () => mine<ReadMarker[]>("readMarkers", "/read-markers", ["readMarkers"], []) },
  joinRequests: {
    /** The room creator's view of who asked. */
    byRoom: ({ roomId }: { roomId: string }) =>
      inRoom<JoinRequest[]>(roomId, "joinRequests", `/rooms/${roomId}/join-requests`, ["joinRequests"], []),
    /** The requester's own pending request (they can't subscribe to the room yet). */
    mine: ({ roomId }: { roomId: string }): Live<JoinRequest[]> => ({
      ...mine<JoinRequest[]>("myJoinRequests", `/rooms/${roomId}/my-join-requests`, ["joinRequests"], []),
      key: ["myJoinRequests", roomId],
    }),
  },
  messages: {
    /** A room's newest messages, newest first. */
    newest: ({ roomId }: { roomId: string }) =>
      inRoom<Message[]>(roomId, "messages", `/rooms/${roomId}/messages`, ["messages"], []),
    /** Unread counts per room, for the whole room list in one request. */
    unreadCounts: () => mine<Record<string, number>>("unread", "/unread", ["unread"], {}),
  },
  reactions: {
    byRoom: ({ roomId }: { roomId: string }) =>
      inRoom<Reaction[]>(roomId, "reactions", `/rooms/${roomId}/reactions`, ["reactions"], []),
  },
  strokes: {
    byCanvas: ({ roomId, canvasId }: { roomId: string; canvasId: string }): Live<Stroke[]> => ({
      ...inRoom<Stroke[]>(roomId, "strokes", `/rooms/${roomId}/canvases/${canvasId}/strokes`, ["strokes"], []),
      key: ["room", roomId, "strokes", canvasId],
    }),
  },
};
