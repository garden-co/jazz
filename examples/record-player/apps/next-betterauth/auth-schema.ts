import { schema as s } from "jazz-tools";

// Better Auth persists ordinary application rows through the trusted backend
// context. These timestamp names belong to Better Auth's wire format, not
// application-defined Jazz provenance.
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
      userId: s.uuid(),
      ipAddress: s.string().optional(),
      userAgent: s.string().optional(),
    },
    { user: s.rel("better_auth_user", "userId") },
  ),
  better_auth_account: s.table(
    {
      accountId: s.string(),
      providerId: s.string(),
      userId: s.uuid(),
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

/**
 * Better Auth is backed by Jazz, but its implementation tables are a trusted
 * server concern. The browser authenticates through Better Auth's endpoints;
 * it must never subscribe to or mutate these rows through the application DB.
 */
export const permissions = {
  better_auth_user: { read: false, insert: false, update: false, delete: false },
  better_auth_session: { read: false, insert: false, update: false, delete: false },
  better_auth_account: { read: false, insert: false, update: false, delete: false },
  better_auth_verification: { read: false, insert: false, update: false, delete: false },
  better_auth_jwks: { read: false, insert: false, update: false, delete: false },
};
