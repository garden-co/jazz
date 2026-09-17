import { schema as s } from "jazz-tools";
import { schema as betterAuthSchema } from "./schema-better-auth/schema";

const schema = {
  ...betterAuthSchema,
  profiles: s.table(
    { author: s.uuid(), displayName: s.string() },
    { messagesViaSender: s.reverse("messages", "sender") },
  ),
  rooms: s.table(
    { name: s.string() },
    {
      roomMembersViaRoom: s.reverse("roomMembers", "room"),
      messagesViaRoom: s.reverse("messages", "room"),
      reactionsViaRoom: s.reverse("reactions", "room"),
    },
  ),
  roomMembers: s.table(
    { roomId: s.uuid(), memberAuthor: s.uuid() },
    { room: s.rel("rooms", "roomId") },
  ),
  messages: s.table(
    {
      roomId: s.uuid(),
      senderId: s.uuid(),
      text: s.string(),
      attachment: s.bytes().optional(),
      attachmentName: s.string().optional(),
    },
    {
      room: s.rel("rooms", "roomId"),
      sender: s.rel("profiles", "senderId"),
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
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
export type Room = s.RowOf<typeof app.rooms>;
export type Reaction = s.RowOf<typeof app.reactions>;
