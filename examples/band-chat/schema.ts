import { schema as s } from "jazz-tools";

const schema = {
  better_auth_user: s.table({
    name: s.string(),
    email: s.string(),
    emailVerified: s.boolean(),
    image: s.string().optional(),
    createdAt: s.timestamp(),
    updatedAt: s.timestamp(),
  }),
  better_auth_session: s.table({
    expiresAt: s.timestamp(),
    token: s.string(),
    createdAt: s.timestamp(),
    updatedAt: s.timestamp(),
    userId: s.ref("better_auth_user"),
    ipAddress: s.string().optional(),
    userAgent: s.string().optional(),
  }),
  better_auth_account: s.table({
    accountId: s.string(),
    providerId: s.string(),
    userId: s.ref("better_auth_user"),
    password: s.string().optional(),
    createdAt: s.timestamp(),
    updatedAt: s.timestamp(),
  }),
  better_auth_verification: s.table({
    identifier: s.string(),
    value: s.string(),
    expiresAt: s.timestamp(),
    createdAt: s.timestamp(),
    updatedAt: s.timestamp(),
  }),
  better_auth_jwks: s.table({
    publicKey: s.string(),
    privateKey: s.string(),
    createdAt: s.timestamp(),
    expiresAt: s.timestamp().optional(),
  }),
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
export type Room = s.RowOf<typeof app.rooms>;
export type Profile = s.RowOf<typeof app.profiles>;
