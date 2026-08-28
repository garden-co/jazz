import { schema as s } from "jazz-tools";

// Better Auth persists ordinary application rows through the trusted backend
// context. These timestamp names belong to Better Auth's wire format, not
// application-defined Jazz provenance.
export const betterAuthSchema = {
  better_auth_user: s.table({
    name: s.string(),
    email: s.string(),
    emailVerified: s.boolean(),
    image: s.string().optional(),
    createdAt: s.allowExternalProvenanceName(s.timestamp()),
    updatedAt: s.allowExternalProvenanceName(s.timestamp()),
  }),
  better_auth_session: s.table({
    expiresAt: s.timestamp(),
    token: s.string(),
    createdAt: s.allowExternalProvenanceName(s.timestamp()),
    updatedAt: s.allowExternalProvenanceName(s.timestamp()),
    userId: s.ref("better_auth_user"),
    ipAddress: s.string().optional(),
    userAgent: s.string().optional(),
  }),
  better_auth_account: s.table({
    issuer: s.string(),
    accountId: s.string(),
    providerId: s.string(),
    userId: s.ref("better_auth_user"),
    password: s.string().optional(),
    createdAt: s.allowExternalProvenanceName(s.timestamp()),
    updatedAt: s.allowExternalProvenanceName(s.timestamp()),
  }),
  better_auth_verification: s.table({
    identifier: s.string(),
    value: s.string(),
    expiresAt: s.timestamp(),
    createdAt: s.allowExternalProvenanceName(s.timestamp()),
    updatedAt: s.allowExternalProvenanceName(s.timestamp()),
  }),
  better_auth_jwks: s.table({
    publicKey: s.string(),
    privateKey: s.string(),
    createdAt: s.allowExternalProvenanceName(s.timestamp()),
    expiresAt: s.timestamp().optional(),
  }),
};

type BetterAuthSchema = s.Schema<typeof betterAuthSchema>;
const betterAuthApp: s.App<BetterAuthSchema> = s.defineApp(betterAuthSchema);

/**
 * Better Auth is backed by Jazz, but its implementation tables are a trusted
 * server concern. The browser authenticates through Better Auth's endpoints;
 * it must never subscribe to or mutate these rows through the application DB.
 */
export const betterAuthPermissions = s.definePermissions(betterAuthApp, ({ policy }) => {
  for (const table of [
    policy.better_auth_user,
    policy.better_auth_session,
    policy.better_auth_account,
    policy.better_auth_verification,
    policy.better_auth_jwks,
  ]) {
    table.allowRead.never();
    table.allowInsert.never();
    table.allowUpdate.never();
    table.allowDelete.never();
  }
});
