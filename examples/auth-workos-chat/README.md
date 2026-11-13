# auth-workos-chat

A small React + Vite example that shows how to integrate [WorkOS AuthKit](https://workos.com/docs/user-management) with Jazz.

What it demonstrates:

- Server-side AuthKit: a small Express server (`server/server.ts`) runs the OAuth redirect and keeps the WorkOS session in a sealed, HttpOnly cookie
- Jazz running inside that server with `createJazz`: its `authenticate` reads the AuthKit cookie, so the browser never holds a Jazz credential
- Refreshing the short-lived WorkOS access token once per stale cookie, shared by HTTP requests and sync sockets
- Role-based UI gating from the WorkOS organization role, passed to Jazz as the `role` claim (`admin` posts to Announcements; `member` posts to the general chat), with generic-chat message ownership enforced via `$createdBy` in `permissions.ts`

## Prerequisites

You need a [WorkOS account](https://workos.com/docs/authkit) with a configured AuthKit environment, its client ID and an API key.

## Setup

### 1. Configure WorkOS

1. Set `WORKOS_CLIENT_ID` in `constants.ts` to your WorkOS client ID.
2. Create `.env.local` with your API key and a cookie password of at least 32 characters:

   ```bash
   WORKOS_API_KEY=sk_test_...
   WORKOS_COOKIE_PASSWORD=$(openssl rand -base64 32)
   ```

3. Add the redirect URI `http://127.0.0.1:5173/auth/callback` in the WorkOS dashboard.

### 2. Start the server and the Vite app

```bash
pnpm dev:server
pnpm dev
```

`dev:server` starts WorkOS sign-in and Jazz on port 3001, keeping Jazz's data in `.jazz`. Vite
proxies `/auth`, `/api/auth` and `/jazz` to it, so the browser talks to one origin. Open
`http://127.0.0.1:5173` and click **Sign in with WorkOS**.

## How the WorkOS integration works

### Server — `server/server.ts`

`/auth/sign-in` redirects to AuthKit, and `/auth/callback` exchanges the code with
`authenticateWithCode({ session: { sealSession: true, cookiePassword } })` and sets the sealed
session cookie. The session's access token lasts minutes, and refreshing it spends a single-use
refresh token, so the server reads and refreshes it in separate places:

- `createJazz`'s `authenticate` only reads the cookie (`loadSealedSession(...).authenticate()`) and
  returns the WorkOS user id, the organization role as the `role` claim, and the access token's
  `exp` as `expiresAt`.
- `createJazz`'s `refresh` refreshes a session whose token expires within two minutes and appends
  the rotated cookie. Jazz calls it on the request a browser makes to ask who is signed in, before
  an open sync session reaches its `expiresAt`, and renews that session in place.
- The app's own routes (`/api`, `/auth`) refresh an expired session and write the new cookie, as
  an AuthKit app's routes do. Jazz's routes are left to `refresh`.
- Optional: with `WORKOS_WEBHOOK_SECRET` set and `/webhooks/workos` added in WorkOS, a
  `session.revoked` event calls `jazz.revokeUser(userId)`, so a sign-out elsewhere ends this user's
  open sync sessions at once instead of at their next renewal.

See [WorkOS](https://jazz.tools/docs/setup/auth/workos) in the docs for the same setup.

### Client — `src/App.tsx`

`useWorkOSSession` asks `/api/auth/session` who is signed in, for the UI. `JazzProvider` starts
signed out and asks the backend itself, so the identity always comes from `authenticate`;
the app calls `refresh()` (from `useJazzSession()`) when the WorkOS user changes and after
sign-out. Sign-out ends the session at WorkOS and clears the cookie without redirecting, so
`refresh()` signs this tab and the browser's other tabs out.

Ownership policies compare `$createdBy.account` with `session.user`. The WorkOS user id is
available under `session.identity.subject`.
