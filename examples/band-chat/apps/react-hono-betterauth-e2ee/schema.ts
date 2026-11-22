import { schema as s } from "jazz-tools";
import { tables as betterAuthSchema } from "./schema-better-auth/schema";

export const schema = s.defineSchema({
  ...betterAuthSchema,
  profiles: s.table(
    {
      author: s.uuid(),
      displayName: s.string(),
      // A small, client-downscaled avatar image. Large media belongs in a
      // message attachment, which is streamed and read lazily.
      avatar: s.bytes().optional(),
      avatarType: s.string().optional(),
    },
    {
      messagesViaSender: s.reverse("messages", "sender"),
      membershipsViaProfile: s.reverse("roomMembers", "memberProfile"),
      joinRequestsViaProfile: s.reverse("joinRequests", "profile"),
    },
  ),
  // Each room is an encrypted space: its members hold its key. The name stays
  // plain, so a room is listed before its key arrives. Only the creator may
  // update a room, because whoever may update it may manage its members.
  rooms: s.table(
    { name: s.string() },
    {
      roomMembersViaRoom: s.reverse("roomMembers", "room"),
      messagesViaRoom: s.reverse("messages", "room"),
      reactionsViaRoom: s.reverse("reactions", "room"),
      joinRequestsViaRoom: s.reverse("joinRequests", "room"),
      readMarkersViaRoom: s.reverse("readMarkers", "room"),
      canvasesViaRoom: s.reverse("canvases", "room"),
    },
  ),
  // A member's profile, so co-members see their name and avatar. Who may read
  // a room is Jazz's members table (`isMember` in permissions.ts), not this.
  roomMembers: s.table(
    {
      roomId: s.uuid(),
      memberAuthor: s.uuid(),
      // The admitted member's profile, so co-members can see their name and
      // avatar. Permissions require it to belong to `memberAuthor`.
      memberProfileId: s.uuid().optional(),
    },
    { room: s.rel("rooms", "roomId"), memberProfile: s.rel("profiles", "memberProfileId") },
  ),
  // "Ask to join": anyone holding a room link may ask; only the room creator
  // can see the request and admit the requester.
  joinRequests: s.table(
    { roomId: s.uuid(), requester: s.uuid(), profileId: s.uuid() },
    { room: s.rel("rooms", "roomId"), profile: s.rel("profiles", "profileId") },
  ),
  // One private row per reader and room: everything newer is unread. The time
  // is sealed with the room's key; when a marker is written stays visible to
  // the server.
  readMarkers: s.table(
    { roomId: s.uuid(), reader: s.uuid(), lastReadAt: s.timestamp().encrypted() },
    { room: s.rel("rooms", "roomId") },
    { space: "room" },
  ),
  // The text and the attachment's name are sealed with the room's key. Who
  // wrote when, the attachment's bytes, type and size, and sketches stay
  // plain.
  messages: s
    .table(
      {
        roomId: s.uuid(),
        senderId: s.uuid(),
        text: s.string().encrypted(),
        attachment: s.bytes().optional(),
        attachmentName: s.string().optional().encrypted(),
        attachmentType: s.string().optional(),
        attachmentSize: s.int().optional(),
        // A message can carry a shared sketch instead of (or as well as) a file.
        canvasId: s.uuid().optional(),
      },
      {
        room: s.rel("rooms", "roomId"),
        sender: s.rel("profiles", "senderId"),
        canvas: s.rel("canvases", "canvasId"),
        reactionsViaMessage: s.reverse("reactions", "message"),
      },
      { space: "room" },
    )
    // A room's newest message is one index entry: the room list reads it
    // without sorting the room's history.
    .compositeIndex(["roomId", "$createdAt"]),
  reactions: s.table(
    {
      roomId: s.uuid(),
      messageId: s.uuid(),
      author: s.uuid(),
      emoji: s.string(),
    },
    { room: s.rel("rooms", "roomId"), message: s.rel("messages", "messageId") },
  ),
  canvases: s.table(
    { roomId: s.uuid(), title: s.string() },
    {
      room: s.rel("rooms", "roomId"),
      strokesViaCanvas: s.reverse("strokes", "canvas"),
      messagesViaCanvas: s.reverse("messages", "canvas"),
    },
  ),
  strokes: s.table(
    {
      canvasId: s.uuid(),
      roomId: s.uuid(),
      author: s.uuid(),
      color: s.string(),
      width: s.int(),
      // Flat [x0, y0, x1, y1, …] in canvas units (0–1000 on both axes).
      points: s.array(s.int()),
    },
    { canvas: s.rel("canvases", "canvasId"), room: s.rel("rooms", "roomId") },
  ),
});
