# auth-betterauth-chat

A small Next.js example that shows how to integrate [Better Auth](https://www.better-auth.com/) with Jazz.

What it demonstrates:

- A single Next.js app that serves both the UI and Better Auth routes
- Better Auth tables stored in Jazz through `jazz-tools/better-auth-adapter`
- A Jazz backend context using in-memory storage while syncing auth rows to the local sync server
- Better Auth's built-in `jwt` plugin to issue ES256 JWTs and expose a JWKS endpoint
- The `admin` plugin to assign roles (`admin` / `member`) to users
- Resolving ordinary provider JWTs into opaque Jazz account handles
- Linking a fresh provider identity to an offline account after graceful context shutdown
- Refreshing same-identity credentials through the session
- Creating local-first accounts when no provider session exists
- Role-based UI gating (`admin` can post to Announcements; `member` can post to the general chat). Permissions are defined in [permissions.ts](./permissions.ts), with generic-chat message ownership enforced via `$createdBy.account`.

One default account is seeded on startup: `admin@example.com / admin` with `role = "admin"`.
The seeded provider identity must be explicitly registered with Jazz on its first login;
the login error offers that action. Subsequent login resolves the existing account.
New sign-ups receive `role = "member"` by default (configured via the `admin` plugin).

## Setup

### 1. Create `.env`

```bash
cp .env.example .env
```

The `.env` file only needs the values already in `.env.example`:

- `NEXT_PUBLIC_APP_ORIGIN` — Next app origin used by Better Auth and Playwright
- `BACKEND_SECRET` — backend secret used by the Better Auth Jazz context
- `NEXT_PUBLIC_CHAT_ID` — general chat room id
- `NEXT_PUBLIC_ANNOUNCEMENTS_CHAT_ID` — announcements chat room id

`NEXT_PUBLIC_JAZZ_APP_ID`, `NEXT_PUBLIC_JAZZ_SERVER_URL`, and the sync server admin secret are
injected automatically by `withJazz` in `next.config.ts` — you do not need to add them.

`pnpm dev` reads `.env` for these values.

### 2. Start the Next app

```bash
pnpm dev
```

Starts Next.js at `NEXT_PUBLIC_APP_ORIGIN`. Better Auth is mounted under `/api/auth/*` via a Next
route handler.

Key routes exposed by Better Auth:

- `POST /api/auth/sign-in/email` — verify credentials, set session cookie
- `POST /api/auth/sign-up/email` — create account, set session cookie
- `GET  /api/auth/token` — exchange active session cookie for a JWT (bearer plugin)
- `GET  /api/auth/jwks` — public key set used by the Jazz sync server

Open `NEXT_PUBLIC_APP_ORIGIN`.

## How the Better Auth integration works

### Server — `src/lib/auth.ts`, `src/lib/auth-jazz-client.ts`, and `schema-better-auth/schema.ts`

`auth.ts` wires up the Better Auth instance with four plugins and points the adapter at the
root app schema, which includes both the generated Better Auth tables and the chat table:

```ts
import { jazzAdapter } from "jazz-tools/better-auth-adapter";
import { app } from "../../schema";

betterAuth({
  database: jazzAdapter({
    db: async () => (await authJazzClient()).db,
    schema: app.wasmSchema,
  }),
  emailAndPassword: { enabled: true, autoSignIn: true, minPasswordLength: 1 },
  plugins: [
    nextCookies(),
    admin({ adminRoles: ["admin"], defaultRole: "member" }),
    bearer(), // enables GET /api/auth/token → JWT exchange
    jwt({
      jwks: { keyPairConfig: { alg: "ES256" } },
      jwt: {
        issuer,
        expirationTime: "30d",
        definePayload: ({ user }) => ({
          claims: { role: user.role ?? "" },
          username: user.name,
        }),
        getSubject: ({ user }) => user.id, // becomes session.user.identity.subject in Jazz
      },
    }),
  ],
});
```

`schema-better-auth/schema.ts` is the generated Better Auth schema source file. Its deny-all
`permissions` export is spread into the root `permissions.ts` alongside the message policies, so
ordinary clients cannot read or mutate authentication rows. `authJazzClient()` uses
a cached session initialized with `initial: { backendSecret }` and its ready client’s `.db`, so the adapter can still access those rows.
It caches the context on `globalThis` so route modules don't instantiate it at import time (which
would fail during Next's build-time page data collection before env vars are available). That
keeps Better Auth state out of Better Auth's in-process memory adapter while still avoiding local
on-disk storage in the Next app.

- **`nextCookies` integration** — lets Better Auth session cookies participate in Next.js route
  handlers and server actions.
- **`admin` plugin** — tracks a `role` field on each user, defaults new accounts to `"member"`.
- **`bearer` plugin** — adds the `GET /api/auth/token` endpoint that turns a valid session cookie
  into a short-lived JWT signed by Better Auth's managed ES256 key pair.
- **`jwt` plugin** — manages JWKS key rotation and controls the JWT payload shape.
  `definePayload` injects `claims.role` and `username`; `getSubject` sets the JWT `sub` claim,
  which Jazz surfaces as `session.user.identity.subject` on the client.

The JWKS endpoint (`/api/auth/jwks`) is automatically provided by the `jwt` plugin and is what
the Jazz sync server polls to verify every incoming token. The same sync server also accepts the
backend secret used by the Better Auth Jazz context so auth rows can sync through Jazz too.

The root permission bundle composes the generated auth-table policies with the chat policies:

```ts
import { permissions as betterAuthPermissions } from "./schema-better-auth/schema";

const messagePermissions = definePermissions(app, ({ policy }) => {
  // Message policies...
});

export default {
  ...betterAuthPermissions,
  ...messagePermissions,
};
```

### Client — `src/lib/auth-client.ts`

```ts
import { jwtClient } from "better-auth/client/plugins";
import { createAuthClient } from "better-auth/react";

export const authClient = createAuthClient({ plugins: [jwtClient()] });
```

`jwtClient()` adds the `authClient.token()` method used to fetch the JWT after sign-in. No explicit
base URL is required; Better Auth defaults to `/api/auth` in the browser.

### Client — `app/page.tsx`

`Page` configures `JazzSessionProvider` once and uses `useJazzSession` commands.
Startup restores the retained local account and uses `loginJWT` for an existing
provider session. Login never implicitly registers or links an identity.

After Better Auth signup, `linkJWT({ getToken })` preserves the account that owns
local data. The session detaches the old data view, waits for sync, performs the
link, and opens the selected account. If sync fails, enrollment never starts and
the prior client remains usable. If linking fails, the session reopens the
retained account; the UI offers an explicit retry.

Sign-out calls `logout` before clearing the provider session, then explicitly
creates a new local-first account. Token refresh remains a provider callback
and cannot switch an existing client's identity.

## Tests

Run the Jazz + permissions integration tests with:

```bash
pnpm test
```

Vitest browser mode (chromium) generates an ES256 keypair, hosts a local
JWKS, and starts a local Jazz server pointed at it. Tests mint a
verified JWT and assert that authenticated callers can post to both
chats while anonymous callers are denied for Announcements. The Better
Auth sign-up / sign-in flow is covered by the example itself when run
via `pnpm dev` — it isn't exercised by `pnpm test`.
