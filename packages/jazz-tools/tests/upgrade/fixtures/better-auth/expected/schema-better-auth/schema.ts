import { schema as s } from "jazz-tools";

// Generated from the Better Auth 1.7 model used by the current Next reference.
// Keep credential/session storage isolated from the tenant domain so adapter
// upgrades are explicit rather than silently changing policy-bearing tables.
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
    {
      better_auth_sessionViaUser: s.reverse("better_auth_session", "user"),
      better_auth_accountViaUser: s.reverse("better_auth_account", "user"),
    },
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
    { user: s.rel("better_auth_user", "userId") },
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
    { user: s.rel("better_auth_user", "userId") },
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
  better_auth_jwks: s.table(
    {
      publicKey: s.string(),
      privateKey: s.string(),
      createdAt: s.allowExternalProvenanceName(s.timestamp()),
      expiresAt: s.timestamp().optional(),
      alg: s.string().optional(),
      crv: s.string().optional(),
    },
    {},
  ),
};

export const permissions = {
  better_auth_user: { read: false, insert: false, update: false, delete: false },
  better_auth_session: { read: false, insert: false, update: false, delete: false },
  better_auth_account: { read: false, insert: false, update: false, delete: false },
  better_auth_verification: { read: false, insert: false, update: false, delete: false },
  better_auth_jwks: { read: false, insert: false, update: false, delete: false },
};
