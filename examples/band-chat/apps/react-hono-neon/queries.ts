import type { JoinRequest, Message, Profile, Reaction, ReadMarker, Room, RoomMember, Stroke } from "./schema";

/**
 * The live queries the UI subscribes to, by name. The server builds the SQL
 * for each (server/queries.ts), scoped to the signed-in user, and seals it;
 * the browser only names the query and its arguments.
 */
export type QueryDef<Row, Result> = { name: string; args: Record<string, unknown>; one: boolean; _?: [Row, Result] };
const many =
  <Row, Args extends Record<string, unknown> = Record<string, never>>(name: string) =>
  (args?: Args): QueryDef<Row, Row[]> => ({ name, args: args ?? {}, one: false });

// The newest page of a room's history.
export const HISTORY_PAGE = 200;
export const UNREAD_CAP = 100;

type RoomArgs = { roomId: string };

export const queries = {
  profiles: { mine: many<Profile>("profiles.mine"), known: many<Profile>("profiles.known") },
  rooms: { mine: many<Room>("rooms.mine") },
  roomMembers: { mine: many<RoomMember>("roomMembers.mine"), byRoom: many<RoomMember, RoomArgs>("roomMembers.byRoom") },
  readMarkers: { mine: many<ReadMarker>("readMarkers.mine") },
  joinRequests: {
    byRoom: many<JoinRequest, RoomArgs>("joinRequests.byRoom"),
    mine: many<JoinRequest, RoomArgs>("joinRequests.mine"),
  },
  messages: {
    newest: many<Message, RoomArgs>("messages.newest"),
    unread: many<Message, { roomId: string; myProfileId: string; readAt?: number }>("messages.unread"),
  },
  reactions: { byRoom: many<Reaction, RoomArgs>("reactions.byRoom") },
  strokes: { byCanvas: many<Stroke, { canvasId: string }>("strokes.byCanvas") },
};
