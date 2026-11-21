// BandChat's rows as the API returns them (the tables are in db/001_init.sql).
export type Profile = { id: string; author: string; displayName: string; createdAt: number };
export type Room = { id: string; name: string; createdBy: string; createdAt: number; lastActivityAt: number | null };
export type RoomMember = { id: string; roomId: string; memberAuthor: string; memberProfileId: string | null };
export type JoinRequest = { id: string; roomId: string; requester: string; profileId: string };
export type ReadMarker = { id: string; roomId: string; reader: string; lastReadAt: number };
export type Canvas = { id: string; roomId: string; title: string };
export type Message = { id: string; roomId: string; senderId: string; text: string; canvasId: string | null; createdAt: number };
export type Reaction = { id: string; roomId: string; messageId: string; author: string; emoji: string };
export type Stroke = {
  id: string;
  canvasId: string;
  roomId: string;
  author: string;
  color: string;
  width: number;
  /** Flat [x0, y0, x1, y1, …] in canvas units (0–1000 on both axes). */
  points: number[];
  createdAt: number;
};

/** What the WebSocket tells subscribers: these tables changed under this topic. */
export type ChangeEvent = { type: "change"; topic: string; tables: string[] };
