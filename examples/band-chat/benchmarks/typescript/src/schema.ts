import { schema as s } from "jazz-tools";

// Intentionally duplicated from the app: this package measures the recognizable
// BandChat data shape without importing UI or authentication ceremony.
const schema = {
  profiles: s.table({ userId: s.string(), displayName: s.string() }),
  rooms: s.table({ name: s.string() }),
  roomMembers: s.table({ roomId: s.ref("rooms"), userId: s.string() }),
  messages: s.table({
    roomId: s.ref("rooms"),
    senderId: s.ref("profiles"),
    text: s.string(),
    attachment: s.bytes().optional(),
    attachmentName: s.string().optional(),
  }),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
