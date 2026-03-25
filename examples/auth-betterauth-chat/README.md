# auth-betterauth-chat

A small Next.js example that shows how to integrate [Better Auth](https://www.better-auth.com/) with Jazz.

What it demonstrates:

- A single Next.js app that serves both the UI and Better Auth routes
- Better Auth's built-in `jwt` plugin to issue ES256 JWTs and expose a JWKS endpoint
- The `admin` plugin to assign roles (`admin` / `member`) to users
- Fetching the JWT from the Better Auth session and passing it to `JazzProvider`
- Falling back to anonymous `localAuth` when no session exists
- Role-based UI gating (`admin` can post to Announcements; `member` can post to the general chat). Permissions are defined in [permissions.ts](./schema/permissions.ts).

One default account is seeded on startup: `admin@example.com / admin` with `role = "admin"`.
New sign-ups receive `role = "member"` by default (configured via the `admin` plugin).

## Setup

### 1. Start the Next app

```bash
pnpm dev
```

Starts Next.js on port 3000. Better Auth is mounted under `/api/auth/*` via a Next route handler.

Key routes exposed by Better Auth:

- `POST /api/auth/sign-in/email` — verify credentials, set session cookie
- `POST /api/auth/sign-up/email` — create account, set session cookie
- `GET  /api/auth/token` — exchange active session cookie for a JWT (bearer plugin)
- `GET  /api/auth/jwks` — public key set used by the Jazz sync server

### 2. Start the Jazz sync server

```bash
pnpm sync-server
```

Builds the `jazz-tools` binary if needed, waits for the Better Auth JWKS endpoint from the Next
app, starts a local sync server on port 1625, and pushes the schema catalogue in one step.

Open `http://127.0.0.1:3000`.

## How the Better Auth integration works

### Server — `server/auth.ts` and `app/api/auth/[...all]/route.ts`

`createBetterAuth` wires up the Better Auth instance with four plugins:

```ts
import { nextCookies } from "better-auth/next-js";

betterAuth({
  database: memoryAdapter(authMemoryDb),
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
        getSubject: ({ user }) => user.id, // becomes session.user_id in Jazz
      },
    }),
  ],
});
```

- **`nextCookies` integration** — lets Better Auth session cookies participate in Next.js route
  handlers and server actions.
- **`admin` plugin** — tracks a `role` field on each user, defaults new accounts to `"member"`.
- **`bearer` plugin** — adds the `GET /api/auth/token` endpoint that turns a valid session cookie
  into a short-lived JWT signed by Better Auth's managed ES256 key pair.
- **`jwt` plugin** — manages JWKS key rotation and controls the JWT payload shape.
  `definePayload` injects `claims.role` and `username`; `getSubject` sets the JWT `sub` claim,
  which Jazz surfaces as `session.user_id` on the client.

The JWKS endpoint (`/api/auth/jwks`) is automatically provided by the `jwt` plugin and is what
the Jazz sync server polls to verify every incoming token.

### Client — `src/lib/auth-client.ts`

```ts
import { jwtClient } from "better-auth/client/plugins";
import { createAuthClient } from "better-auth/react";

export const authClient = createAuthClient({ plugins: [jwtClient()] });
```

`jwtClient()` adds the `authClient.token()` method used to fetch the JWT after sign-in. No explicit
base URL is required; Better Auth defaults to `/api/auth` in the browser.

### Client — `src/App.tsx`

`App` subscribes to the Better Auth session and, when a session exists, exchanges it for a JWT
before mounting `JazzProvider`:

```tsx
const { data: authSession } = authClient.useSession();
const [token, setToken] = React.useState<string | null>(null);

React.useEffect(() => {
  if (!authSession?.session) {
    setToken(null);
    return;
  }
  authClient.token().then(({ data }) => setToken(data.token));
}, [authSession?.session?.id]);
```

While the JWT is being fetched the app renders a loading state. Once the token arrives,
`JazzProvider` is mounted in JWT mode. On sign-out Better Auth clears the session cookie and
the effect resets `token` to `null`, reverting Jazz to anonymous mode.

```tsx
const config: DbConfig = token
  ? { appId, jwtToken: token, serverUrl, ... }
  : { appId, ...getActiveSyntheticAuth(appId, { defaultMode: "anonymous" }), ... };

<JazzProvider key={token ? "jwt" : "local"} config={config}>
  <ChatShell />
</JazzProvider>
```

## Playwright

Run the full end-to-end setup and flow tests with:

```bash
pnpm test:e2e
```

The Playwright `webServer` starts Next on port 4179. Global setup waits for the Next-hosted JWKS
endpoint, spins up a local Jazz sync server pointed at that JWKS URL, and pushes the schema
catalogue before the browser test runs.
