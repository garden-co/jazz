import { table, col } from "jazz-tools";

table("better_auth_user", {
  name: col.string(),
  email: col.string(),
  emailVerified: col.boolean(),
  image: col.string().optional(),
  createdAt: col.timestamp(),
  updatedAt: col.timestamp(),
  role: col.string().optional(),
  banned: col.boolean().optional(),
  banReason: col.string().optional(),
  banExpires: col.timestamp().optional(),
});

table("better_auth_session", {
  expiresAt: col.timestamp(),
  token: col.string(),
  createdAt: col.timestamp(),
  updatedAt: col.timestamp(),
  ipAddress: col.string().optional(),
  userAgent: col.string().optional(),
  userId: col.ref("better_auth_user"),
  impersonatedBy: col.string().optional(),
});

table("better_auth_account", {
  accountId: col.string(),
  providerId: col.string(),
  userId: col.ref("better_auth_user"),
  accessToken: col.string().optional(),
  refreshToken: col.string().optional(),
  idToken: col.string().optional(),
  accessTokenExpiresAt: col.timestamp().optional(),
  refreshTokenExpiresAt: col.timestamp().optional(),
  scope: col.string().optional(),
  password: col.string().optional(),
  createdAt: col.timestamp(),
  updatedAt: col.timestamp(),
});

table("better_auth_verification", {
  identifier: col.string(),
  value: col.string(),
  expiresAt: col.timestamp(),
  createdAt: col.timestamp(),
  updatedAt: col.timestamp(),
});

table("better_auth_jwks", {
  publicKey: col.string(),
  privateKey: col.string(),
  createdAt: col.timestamp(),
  expiresAt: col.timestamp().optional(),
});
