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
session cookie. `createJazz`'s `authenticate` loads that cookie with `loadSealedSession` and returns
the WorkOS user id and organization role:

```ts
authenticate: async (request) => {
  const session = await readSession(readCookie(request.headers.get("cookie")));
  return session ? { userId: session.user.id, claims: { role: session.role ?? "" } } : null;
},
```

### Client — `src/App.tsx`

`useWorkOSSession` asks `/api/auth/session` who is signed in. `JazzProvider` gets
`auth={backendAuth({ key, isPending, logout })}`: whenever the user changes, Jazz signs in again
through the backend, and the identity always comes from `authenticate`. Sign-out flushes Jazz,
clears the cookie and redirects to the WorkOS logout URL.

Ownership policies compare `$createdBy.account` with `session.user.account`. The WorkOS user id is
available under `session.user.identity.subject`.
