import { schema as s } from "jazz-tools";

export const tables = {
  better_auth_user: s.table(
    {
      name: s.string(),
      email: s.string(),
      emailVerified: s.boolean(),
      image: s.string().optional(),
      createdAt: s.allowExternalProvenanceName(s.timestamp()),
      updatedAt: s.allowExternalProvenanceName(s.timestamp()),
    },
    {},
  ),

  better_auth_session: s.table(
    {
      expiresAt: s.timestamp(),
      token: s.string(),
      createdAt: s.allowExternalProvenanceName(s.timestamp()),
      updatedAt: s.allowExternalProvenanceName(s.timestamp()),
      ipAddress: s.string().optional(),
      userAgent: s.string().optional(),
      userId: s.uuid(),
    },
    {
      userIdRelation: s.rel("better_auth_user", "userId"),
    },
  ),

  better_auth_account: s.table(
    {
      accountId: s.string(),
      providerId: s.string(),
      userId: s.uuid(),
      accessToken: s.string().optional(),
      refreshToken: s.string().optional(),
      idToken: s.string().optional(),
      accessTokenExpiresAt: s.timestamp().optional(),
      refreshTokenExpiresAt: s.timestamp().optional(),
      scope: s.string().optional(),
      password: s.string().optional(),
      createdAt: s.allowExternalProvenanceName(s.timestamp()),
      updatedAt: s.allowExternalProvenanceName(s.timestamp()),
    },
    {
      userIdRelation: s.rel("better_auth_user", "userId"),
    },
  ),

  better_auth_verification: s.table(
    {
      identifier: s.string(),
      value: s.string(),
      expiresAt: s.timestamp(),
      createdAt: s.allowExternalProvenanceName(s.timestamp()),
      updatedAt: s.allowExternalProvenanceName(s.timestamp()),
    },
    {},
  ),
};

export const permissions = {
  better_auth_user: { read: false, insert: false, update: false, delete: false },
  better_auth_session: { read: false, insert: false, update: false, delete: false },
  better_auth_account: { read: false, insert: false, update: false, delete: false },
  better_auth_verification: { read: false, insert: false, update: false, delete: false },
};
