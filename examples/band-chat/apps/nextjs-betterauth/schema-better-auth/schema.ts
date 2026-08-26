import { schema as s } from "jazz-tools";

// Generated Better Auth tables are deliberately a separate module. The root
// application spreads the schema and deny-all policies into its own app.
export const schema = {
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
    ipAddress: s.string().optional(),
    userAgent: s.string().optional(),
    userId: s.ref("better_auth_user"),
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

type BetterAuthSchema = s.Schema<typeof schema>;
export const app: s.App<BetterAuthSchema> = s.defineApp(schema);

export const permissions = s.definePermissions(app, ({ policy }) => {
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
