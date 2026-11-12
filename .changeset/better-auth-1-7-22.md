---
"jazz-tools": minor
---

Breaking: the Better Auth adapter now targets Better Auth 1.7.22, and the optional `better-auth` peer range is `~1.7.22` instead of an exact pin, so later 1.7.x security patches install without a jazz-tools release. Better Auth 1.7.16 fixed the advisories published on 3 November 2026 (revoked sessions staying valid, unverified phone numbers and OAuth emails treated as verified, email OTP and email-change flaws, and more); the previous pin, `1.7.1`, is affected by them.

Upgrading changes the auth schema and needs a data check. Do these in order, and finish them before the new Better Auth version serves traffic:

1. **Remove duplicate accounts first, on your current version.** The adapter now enforces that `better_auth_account` is unique on `(providerId, accountId)` (Better Auth 1.7.3 dropped the `(issuer, accountId)` index it used to declare). While duplicates remain, Better Auth writes to those accounts, such as token refreshes, password changes and account links, fail with `Unique constraint violated`. List the accounts, for example with `jazz-tools inspect query '{"table":"better_auth_account","orderBy":[["providerId","asc"],["accountId","asc"]]}'`, and for every repeated `(providerId, accountId)` pair keep the row that belongs to the right user and delete the others.
2. **Upgrade** `better-auth` to `1.7.22` (or a later 1.7.x) and `jazz-tools`.
3. **Regenerate the auth tables** with `npx auth@latest generate --config ./auth-generate.ts --output ./schema-better-auth/schema.ts --yes`. The `better_auth_account` table loses its required `issuer` column.
4. **Write the migration** with `npx jazz-tools migrations create --name remove-better-auth-issuer`. In the generated file, give the dropped column a backwards default, `issuer: m.drop.string({ backwardsDefault: "" })`, then run `npx jazz-tools validate` to update `migrations/index.ts`.
5. **Deploy the backend with the new schema and migration** in the same release that switches Better Auth version; `createJazz` publishes them when it starts. Better Auth 1.7.3 and later cannot create accounts against the old schema, because `issuer` is still required there, so sign-ups and account links fail until the migration is published.

Existing `issuer` values stay stored. The empty backwards default only fills `issuer` for rows that new code creates when an older schema reads them; it does not reconstruct an issuer and does not make downgrading Better Auth safe. The Better Auth examples ship this migration.
