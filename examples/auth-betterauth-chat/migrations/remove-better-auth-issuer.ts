// Existing issuer cells keep their stored values. New rows viewed by an old
// schema get an empty, non-authoritative sentinel; this does not support an
// authentication-package downgrade or reconstruct an issuer from providerId.
import { schema as s, migration as m } from "jazz-tools";

export default m.defineMigration({
  migrate: {
    better_auth_account: {
      issuer: m.drop.string({ backwardsDefault: "" }),
    },
  },
  fromHash: "8acd8a85eef5",
  toHash: "c95f3dba1806",
  from: {
    better_auth_account: s.table(
      {
        issuer: s.string(),
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
        createdAt: s.timestamp(),
        updatedAt: s.timestamp(),
      },
      {
        user: s.rel("better_auth_user", "userId"),
      },
    ),
    better_auth_session: s.table(
      {
        expiresAt: s.timestamp(),
        token: s.string(),
        createdAt: s.timestamp(),
        updatedAt: s.timestamp(),
        ipAddress: s.string().optional(),
        userAgent: s.string().optional(),
        userId: s.uuid(),
        impersonatedBy: s.string().optional(),
      },
      {
        user: s.rel("better_auth_user", "userId"),
      },
    ),
    better_auth_user: s.table(
      {
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
      },
      {
        better_auth_sessionViaUser: s.reverse("better_auth_session", "user"),
        better_auth_accountViaUser: s.reverse("better_auth_account", "user"),
      },
    ),
  },
  to: {
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
        createdAt: s.timestamp(),
        updatedAt: s.timestamp(),
      },
      {
        user: s.rel("better_auth_user", "userId"),
      },
    ),
    better_auth_session: s.table(
      {
        expiresAt: s.timestamp(),
        token: s.string(),
        createdAt: s.timestamp(),
        updatedAt: s.timestamp(),
        ipAddress: s.string().optional(),
        userAgent: s.string().optional(),
        userId: s.uuid(),
        impersonatedBy: s.string().optional(),
      },
      {
        user: s.rel("better_auth_user", "userId"),
      },
    ),
    better_auth_user: s.table(
      {
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
      },
      {
        better_auth_sessionViaUser: s.reverse("better_auth_session", "user"),
        better_auth_accountViaUser: s.reverse("better_auth_account", "user"),
      },
    ),
  },
});
