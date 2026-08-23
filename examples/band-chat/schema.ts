import { schema as s } from "jazz-tools";

const schema = {
  profiles: s.table({ userId: s.string(), displayName: s.string() }),
  rooms: s.table({ name: s.string(), createdBy: s.string() }),
  roomMembers: s.table({ roomId: s.ref("rooms"), userId: s.string() }),
  messages: s.table({
    roomId: s.ref("rooms"),
    senderId: s.ref("profiles"),
    text: s.string(),
    attachment: s.bytes().optional(),
    attachmentName: s.string().optional(),
    createdAt: s.timestamp(),
  }),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
export type Room = s.RowOf<typeof app.rooms>;
export type Profile = s.RowOf<typeof app.profiles>;
