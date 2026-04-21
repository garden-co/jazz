import { schema as s } from "jazz-tools";

const schema = {
  better_auth_user: s.table({
    name: s.string(),
    email: s.string(),
    emailVerified: s.boolean(),
    image: s.string().optional(),
    createdAt: s.timestamp(),
    updatedAt: s.timestamp(),
    role: s.string().optional(),
    banned: s.boolean().optional(),
    banReason: s.string().optional(),
    banExpires: s.timestamp().optional(),
  }),

  better_auth_session: s.table({
    expiresAt: s.timestamp(),
    token: s.string(),
    createdAt: s.timestamp(),
    updatedAt: s.timestamp(),
    ipAddress: s.string().optional(),
    userAgent: s.string().optional(),
    userId: s.ref("better_auth_user"),
    impersonatedBy: s.string().optional(),
  }),

  better_auth_account: s.table({
    accountId: s.string(),
    providerId: s.string(),
    userId: s.ref("better_auth_user"),
    accessToken: s.string().optional(),
    refreshToken: s.string().optional(),
    idToken: s.string().optional(),
    accessTokenExpiresAt: s.timestamp().optional(),
    refreshTokenExpiresAt: s.timestamp().optional(),
    scope: s.string().optional(),
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
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
export const wasmSchema = app.wasmSchema;
