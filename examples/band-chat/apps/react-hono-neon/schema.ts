// BandChat's tables (Postgres: db/001_init.sql), as live queries return them.
// Managed Better Auth keeps its users in the branch's own "neon_auth" schema.

export type Profile = { id: string; author: string; displayName: string; createdAt: number };
export type Room = { id: string; name: string; createdBy: string; createdAt: number; lastActivityAt: number | null };
/** `memberProfileId`: the admitted member's profile, so co-members can see their name. */
export type RoomMember = { id: string; roomId: string; memberAuthor: string; memberProfileId: string | null };
export type JoinRequest = { id: string; roomId: string; requester: string; profileId: string };
export type ReadMarker = { id: string; roomId: string; reader: string; lastReadAt: number };
export type Message = { id: string; roomId: string; senderId: string; text: string; canvasId: string | null; createdAt: number };
export type Reaction = { id: string; roomId: string; messageId: string; author: string; emoji: string };
export type Canvas = { id: string; roomId: string; title: string };
/** `points`: flat [x0, y0, x1, y1, ...] in canvas units (0-1000 on both axes). */
export type Stroke = { id: string; canvasId: string; roomId: string; author: string; color: string; width: number; points: number[]; createdAt: number };
