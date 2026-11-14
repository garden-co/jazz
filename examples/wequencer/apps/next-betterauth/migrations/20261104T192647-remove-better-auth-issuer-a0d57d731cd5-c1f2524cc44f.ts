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
  fromHash: "a0d57d731cd5",
  toHash: "c1f2524cc44f",
  from: {
    better_auth_account: s.table(
      {
        issuer: s.string(),
        accountId: s.string(),
        providerId: s.string(),
        userId: s.uuid(),
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
      },
      {
        better_auth_sessionViaUser: s.reverse("better_auth_session", "user"),
        better_auth_accountViaUser: s.reverse("better_auth_account", "user"),
      },
    ),
  },
});
