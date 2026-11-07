# auth-simple-chat

A small React + Vite example that shows how to run Jazz inside your own auth server.

What it demonstrates:

- A local Express server with its own email/password sign-in and an HttpOnly session cookie
- Jazz running inside that server with `createJazz`: `authenticate` reads the same cookie, so the browser never holds a Jazz credential
- Starting every visitor as a local-first guest, and keeping the guest's data on sign-up with `linkWithBackend()`
- Role-based UI gating (`admin` can post to Announcements; `member` can post to the general chat). Permissions are defined in [permissions.ts](./permissions.ts), with generic-chat message ownership enforced via `$createdBy.account`.

Passwords are stored in plain text in memory for example simplicity only.
One default account is seeded on startup: `admin@example.com / admin` with `role = "admin"`.
New sign-ups are auto-created as `role = "member"`.

`authenticate` returns `{ userId, claims: { role } }`. Jazz asks it on every sign-in and sync
connection, and the client reads `session.claims.role` for role-based gating.

## Setup

### 1. Start the auth server

```bash
pnpm dev:auth
```

Starts the server on port 3001, keeping Jazz's data in `.jazz`. It serves:

- `POST /api/auth/sign-in`, `POST /api/auth/sign-up`: verify or create the user, set the session cookie
- `POST /api/auth/sign-out`, `GET /api/auth/session`
- `/jazz/*`: Jazz sync and sign-in

### 2. Start the Vite app

```bash
pnpm dev
```

Open `http://127.0.0.1:5173`. Vite proxies `/api/auth` and `/jazz` to the auth server.

## Tests

Run the Jazz + permissions integration tests with:

```bash
pnpm test
```

Vitest browser mode (chromium) asserts the policy outcomes for `admin` / `member` roles posting to
Announcements vs the general chat, against a local test server.

### Account lifecycle

Every visitor starts as a local-first guest. Sign-in calls `loginWithBackend()`: the backend
names the signed-in user and Jazz opens that user's account. Sign-up calls `linkWithBackend()`:
the guest proves itself with its own local-first key, and the backend links it to the new user,
so the messages it wrote as a guest stay its own. Signing out flushes Jazz before clearing the
session cookie, then starts a new local-first guest.
