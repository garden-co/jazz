# auth-simple-chat

A small React + Vite example that shows how to integrate an external JWT auth server with Jazz.

What it demonstrates:

- A local Express auth server that issues ES256 JWTs and exposes a JWKS endpoint
- Resolving provider JWTs into opaque account handles before opening Jazz contexts
- Linking fresh provider identities outside contexts after ordinary graceful shutdown
- Falling back to local-first auth when no token is present
- Role-based UI gating (`admin` can post to Announcements; `member` can post to the general chat). Permissions are defined in [permissions.ts](./permissions.ts), with generic-chat message ownership enforced via `$createdBy.account`.

Passwords are stored in plain text in memory for example simplicity only.
One default account is seeded on startup: `admin@example.com / admin` with `role = "admin"`.
New sign-ups are auto-created as `role = "member"`.

The Jazz sync server validates the JWT's signature against the JWKS on every connection.
The `claims` object inside the payload is forwarded to the client as `session.claims`, which
is how the UI reads `session.claims.role` for role-based gating.

## Setup

### 1. Start the auth server

```bash
pnpm dev:auth
```

Starts the web server on port 3001 that serves:

- `POST /api/auth/sign-in` — verify credentials, return JWT
- `POST /api/auth/sign-up` — create account, return JWT
- `GET  /.well-known/jwks.json` — public key set used by the sync server

### 2. Start the Jazz sync server

```bash
pnpm sync-server
```

Starts a local sync server on port 1625 pointed at the
auth server JWKS URL, and pushes the schema catalogue in one step.

### 3. Start the Vite app

```bash
pnpm dev
```

Open `http://127.0.0.1:5173`.

## Tests

Run the Jazz + permissions integration tests with:

```bash
pnpm test
```

Vitest browser mode (chromium) mints ES256 JWTs for `admin` / `member`
roles against a local JWKS server, then asserts the policy outcomes
for posting to Announcements vs the general chat. The runtime auth
server (`server/auth-server.ts`) and the sign-in UI are covered by
the example itself when run via `pnpm dev` + `pnpm dev:auth` — they
aren't exercised by `pnpm test`.

### Account lifecycle

This example demonstrates manual hybrid auth, without an automatic provider
connection. Provider sign-in uses `loginOrRegisterJWT` to atomically resolve or
create the provider identity's Jazz account. Sign-up creates
an ordinary provider identity, gracefully shuts down the local Jazz client,
then links the new JWT to its account using `linkJWT`. The provider does not
rewrite user IDs or mint special proof claims. New contexts receive only the
returned account handle.

If linking fails, the existing account is reopened. The retry action preserves
the original intent: signup retries linking, while login retries login-or-register. These operations never merge existing accounts. Signing out
closes the client before clearing provider credentials and choosing a new local
account.
