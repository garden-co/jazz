import { schema as s } from "jazz-tools";
import { schema as betterAuthSchema } from "./schema-better-auth/schema";

const schema = {
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
  rooms: s.table(
    {
      name: s.string(),
      // Denormalized "last activity" carrier, written by members when they
      // post. Unread state is derived per reader from `readMarkers`.
      lastActivityAt: s.timestamp().optional(),
    },
    {
      roomMembersViaRoom: s.reverse("roomMembers", "room"),
      messagesViaRoom: s.reverse("messages", "room"),
      reactionsViaRoom: s.reverse("reactions", "room"),
      joinRequestsViaRoom: s.reverse("joinRequests", "room"),
      readMarkersViaRoom: s.reverse("readMarkers", "room"),
      canvasesViaRoom: s.reverse("canvases", "room"),
    },
  ),
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
  // One private row per reader and room: everything newer is unread.
  readMarkers: s.table(
    { roomId: s.uuid(), reader: s.uuid(), lastReadAt: s.timestamp() },
    { room: s.rel("rooms", "roomId") },
  ),
  messages: s.table(
    {
      roomId: s.uuid(),
      senderId: s.uuid(),
      text: s.string(),
      attachment: s.bytes().optional(),
      attachmentName: s.string().optional(),
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
  ),
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
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
export type Profile = s.RowOf<typeof app.profiles>;
export type Room = s.RowOf<typeof app.rooms>;
export type RoomMember = s.RowOf<typeof app.roomMembers>;
export type JoinRequest = s.RowOf<typeof app.joinRequests>;
export type ReadMarker = s.RowOf<typeof app.readMarkers>;
export type Message = s.RowOf<typeof app.messages>;
export type Reaction = s.RowOf<typeof app.reactions>;
export type Canvas = s.RowOf<typeof app.canvases>;
export type Stroke = s.RowOf<typeof app.strokes>;
