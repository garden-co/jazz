# auth-betterauth-chat

A small Next.js example that shows how to integrate [Better Auth](https://www.better-auth.com/) with Jazz.

What it demonstrates:

- A single Next.js app that serves the UI, Better Auth routes and Jazz itself (`createJazz` in `src/lib/jazz.server.ts`, mounted at `/jazz`)
- Better Auth tables stored in Jazz through `jazz-tools/better-auth-adapter`
- `authenticate` reading the Better Auth session cookie, so the browser never holds a Jazz credential
- The `admin` plugin to assign roles (`admin` / `member`) to users, passed to Jazz as the `role` claim
- Starting as a local-first guest and keeping the guest's data on sign-up with `linkWithBackend()`
- Role-based UI gating (`admin` can post to Announcements; `member` can post to the general chat). Permissions are defined in [permissions.ts](./permissions.ts), with generic-chat message ownership enforced via `$createdBy.account`.

One default account is seeded on startup: `admin@example.com / admin` with `role = "admin"`.
New sign-ups receive `role = "member"` by default (configured via the `admin` plugin).

## Setup

### 1. Create `.env`

```bash
cp .env.example .env
```

The `.env` file only needs the values already in `.env.example`:

- `NEXT_PUBLIC_APP_ORIGIN`: Next app origin used by Better Auth and Playwright
- `NEXT_PUBLIC_JAZZ_APP_ID`: the Jazz app id
- `NEXT_PUBLIC_CHAT_ID`: general chat room id
- `NEXT_PUBLIC_ANNOUNCEMENTS_CHAT_ID`: announcements chat room id

There is no separate Jazz server and no secret to set: Jazz runs inside the Next app and keeps its
data in `.jazz`.

### 2. Start the Next app

```bash
pnpm dev
```

Starts Next.js at `NEXT_PUBLIC_APP_ORIGIN`. Better Auth is mounted under `/api/auth/*` and Jazz
under `/jazz/*`, each through a Next route handler. Open `NEXT_PUBLIC_APP_ORIGIN`.

## How the Better Auth integration works

### Server: `src/lib/jazz.server.ts`, `src/lib/auth.ts` and `schema-better-auth/schema.ts`

`jazz.server.ts` creates Jazz once per process. Its `authenticate` asks Better Auth who the request's
session cookie belongs to, and passes the `admin` plugin's role on as a claim:

```ts
authenticate: async (request) => {
  const session = await (await auth).api.getSession({ headers: request.headers });
  if (!session) return null;
  return { userId: session.user.id, claims: { role: session.user.role ?? "" } };
},
```

`auth.ts` stores Better Auth's own tables in Jazz with the adapter, through the backend's `jazz.db`:

```ts
betterAuth({
  database: jazzAdapter({ db: async () => (await jazz()).db, schema: app.wasmSchema }),
  emailAndPassword: { enabled: true, autoSignIn: true, minPasswordLength: 1 },
  plugins: [admin({ adminRoles: ["admin"], defaultRole: "member" }), nextCookies()],
});
```

`schema-better-auth/schema.ts` is the generated Better Auth schema source file. Its deny-all
`permissions` export is spread into the root `permissions.ts` alongside the message policies, so
ordinary clients cannot read or mutate authentication rows; `jazz.db` acts with backend authority.

### Client: `app/page.tsx`

The browser connects to `serverUrl: "/jazz"` and starts as a local-first guest. If Better Auth
already has a session, startup calls `loginWithBackend()`. Sign-in calls `loginWithBackend()` after
Better Auth's sign-in sets its cookie. Sign-up calls `linkWithBackend()`: the guest proves itself
with its own local-first key and the backend links it to the new Better Auth user, so the guest's
messages stay its own. If linking fails, the guest account stays open and the UI offers a retry.

Sign-out calls `logout` before clearing the Better Auth session, then creates a new local-first
guest.

## Tests

Run the Jazz + permissions integration tests with:

```bash
pnpm test
```

Vitest browser mode (chromium) asserts that admins and members get the right outcomes posting to
both chats, against a local test server. The server suite runs Better Auth's sign-up, sign-in and
sign-out through the application handler and checks that Jazz's backend sign-in follows the
Better Auth cookie.
