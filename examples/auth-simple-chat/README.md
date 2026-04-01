# auth-simple-chat

A small React + Vite example that shows how to integrate an external JWT auth server with Jazz.

What it demonstrates:

- A local Express auth server that issues ES256 JWTs and exposes a JWKS endpoint
- Passing a JWT token directly to `JazzProvider` to authenticate as a named user
- Falling back to anonymous `localAuth` when no token is present
- Role-based UI gating (`admin` can post to Announcements; `member` can post to the general chat). Permissions are defined in [permissions.ts](./permissions.ts).

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

## Playwright

Run the full end-to-end setup and flow tests with:

```bash
pnpm test:e2e
```
