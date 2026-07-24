---
name: jazz-auth
description: Configure and troubleshoot authentication and identity in Jazz TypeScript applications. Use for anonymous, local-first, external JWT, or cookie-backed auth; auth providers and JWKS; Better Auth integration; session and claim handling; token expiry or refresh; logout and storage lifecycle; recovery passphrases or passkeys; and upgrading a local-first identity to an external account. For row-level authorization rules, use jazz-schema-evolution.
---

# Jazz Authentication

Implement authentication through the installed `jazz-tools` public API. Preserve the distinction
between identity, transport credentials, local database storage, and row-level permissions.

## Start from the project

1. Read the installed `jazz-tools` version and inspect its public types when an auth API is
   uncertain.
2. Locate the Jazz provider or `createDb(...)` setup, the auth provider integration, server auth
   configuration, and the code that creates or replaces the active client.
3. Identify the current principal source: anonymous, local-first secret, bearer JWT, or an HttpOnly
   cookie mirrored through `cookieSession`.
4. Locate logout, token-refresh, account-upgrade, and local-storage reset flows before changing any
   one of them.
5. Read the bundled reference that matches the task:
   - [modes-and-lifecycle.md](references/modes-and-lifecycle.md) for choosing an auth mode, managing
     a live client, sessions, recovery, logout, and storage.
   - [providers-and-upgrades.md](references/providers-and-upgrades.md) for JWT/JWKS providers,
     Better Auth, provider claims, and local-first identity upgrades.

## Preserve principal identity

- Treat the local-first secret as the user's credential and identity. The same secret produces the
  same Jazz user ID; losing it can make identity-owned rows inaccessible.
- Treat JWT `sub` as `session.user_id`. Use a stable account identifier, never an email address or
  session ID.
- Use `db.updateAuthToken(jwt)` only to refresh a token for the same principal.
- Recreate `Db` or `JazzProvider` for sign-in, sign-out, local-first-to-external transition, or any
  principal change. Do not use `updateAuthToken(null)` to switch modes.
- With cookie auth, remember that the HttpOnly cookie is the transport credential and
  `cookieSession` is the client's mirrored session. Update it only for the same principal.

## Keep lifecycle operations distinct

- Use `db.logout()` to shut down the current client. Clear provider tokens or cookies through the
  auth provider as a separate operation.
- Use `db.logout({ wipeData: true })` when browser OPFS data for that Jazz namespace must also be
  deleted.
- Use `db.deleteClientStorage()` only for a browser worker-backed database reset. It intentionally
  leaves local-first identity storage untouched.
- Do not clear or replace a local-first secret without an explicit identity-loss or recovery plan.
- Check auth state errors as well as `session`; Jazz can preserve the last-known session while
  authenticated sync is paused after expiry or rejection.

## Integrate providers deliberately

- Configure exactly one external verification source: `jwksUrl` or `jwtPublicKey`.
- Keep signing keys, backend secrets, and admin secrets out of client-visible environment variables.
- Match JWT claim names exactly when permissions consume them.
- For Better Auth as Jazz's database adapter, use the generated Better Auth schema and permissions,
  merge them into the app schema, and give the adapter a backend-scoped database handle.
- For try-before-signup, prove ownership of the local-first identity on the client, verify the proof
  on the server, and issue all future JWTs with the proven Jazz ID as `sub`.

## Cross into adjacent work deliberately

- Load `jazz-schema-evolution` when the task authors or changes `permissions.ts`, merges generated
  auth tables into an established schema, or deploys those changes.
- Load `jazz-backend` when the task creates request-scoped database handles, auth middleware, API
  routes, or a self-hosted sync server.
- Load `jazz-testing` only when the requested work includes auth test code.

## Verify the change

1. Exercise first load, token refresh, sign-in, sign-out, and principal replacement separately.
2. Confirm the session user ID is stable across refresh and account upgrade.
3. Test expired, invalid, missing, and disabled credentials when the integration can produce them.
4. Test recovery before shipping a flow that can clear the local-first secret.
5. Test permission-relevant claims as signed sessions rather than unsigned client state.

## Avoid these failure modes

- Do not silently create a new local-first secret on every render or startup.
- Do not change JWT `sub` for an existing account.
- Do not assume a non-null last-known session means authenticated sync is healthy.
- Do not use browser database deletion as a substitute for signing out, or signing out as a
  substitute for deleting local data.
- Do not expose backend or admin credentials to the browser.
- Do not implement authorization solely with UI state or client-side filters.
