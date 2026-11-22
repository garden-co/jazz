import { schema as s } from "jazz-tools";

export const schema = {
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

type AppSchema = s.Schema<typeof schema>;
// Never deployed on its own: the app that spreads `schema` ships the migrations.
export const app: s.App<AppSchema> = s.defineApp(schema, { migrations: "none" });
export const wasmSchema = app.wasmSchema;

export const permissions = s.definePermissions(app, ({ policy }) => {
  policy.better_auth_user.allowRead.never();
  policy.better_auth_user.allowInsert.never();
  policy.better_auth_user.allowUpdate.never();
  policy.better_auth_user.allowDelete.never();

  policy.better_auth_session.allowRead.never();
  policy.better_auth_session.allowInsert.never();
  policy.better_auth_session.allowUpdate.never();
  policy.better_auth_session.allowDelete.never();

  policy.better_auth_account.allowRead.never();
  policy.better_auth_account.allowInsert.never();
  policy.better_auth_account.allowUpdate.never();
  policy.better_auth_account.allowDelete.never();

  policy.better_auth_verification.allowRead.never();
  policy.better_auth_verification.allowInsert.never();
  policy.better_auth_verification.allowUpdate.never();
  policy.better_auth_verification.allowDelete.never();
});
