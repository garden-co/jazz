# Wequencer on InstantDB

## Versions and hosting

- `@instantdb/react` and `@instantdb/admin` 1.0.67, `instant-cli` 1.0.67; Next.js 16.2.11, React 19.2.4 (as in
  the Jazz copy).
- Data: a **hosted temporary Instant app** (no account). `harness.mjs` runs `pnpm install` on a fresh checkout,
  creates the app into `.env` (git-ignored) and pushes `instant.schema.ts` and `instant.perms.ts` on every
  `seed()`. Temporary apps expire; delete `.env` and the next run makes a new one.
- Region: `api.instantdb.com` via CloudFront IAD (us-east). The Next server (sign-in routes, profile bootstrap,
  pages) runs locally, like the Jazz copy's.

## How it maps

- **Auth**: email + password through Instant's documented custom-auth route. `app/api/auth/sign-in` and
  `sign-up` check scrypt hashes kept in an admin-only `credentials` namespace and mint an Instant token with
  `createToken`; the browser signs in with `signInWithToken`. The sign-in form is unchanged.
- **Pages**: Instant keeps the session in the browser (IndexedDB), not in a cookie. So the Jazz app's
  server-side redirects (`/` → `/dashboard` when signed in, `/dashboard` → `/` when not) happen in the client
  instead. Instant's cookie-based SSR is documented as experimental.
- **Profiles**: created on the server, as in the Jazz app (`/api/bootstrap`, called once per account and
  name). The route checks the caller's Instant token with `adminDb.auth.verifyToken`.
- **Schema**: the Jazz tables become namespaces linked to their session. A step links to its track and its
  pattern only (no `session_id` carrier); a track's removal cascades to its steps in the same transaction.
  `$createdBy` becomes `sessions.creator`, set on create and immutable under the rules. Pads keep their
  derived row ids (`lib/step-id.ts`), so pressing the same untouched pad from two clients upserts one row.
- **Permissions** (`instant.perms.ts`): the Jazz rules, default-deny elsewhere. I checked them against the
  hosted app with a creator, an editor, a viewer and an outsider: self-admission, a viewer marked as an
  editor, viewer pad and transport writes, a step pointing at another session's pattern, an editor deleting
  a track, and outsider reads and presence are all rejected. Profiles become visible through presence, and
  deleting a track removes its steps.
- **Presence** stays as rows with a 5-second heartbeat, as in the Jazz copy, rather than Instant's own
  ephemeral presence (`db.rooms`). That keeps the behaviour and the "name visible once they've shown
  presence" rule the same.
- **Seed** (`seed.ts`): users sign up through the app's own `signUp`, and profiles come from its bootstrap.
  Each session is written with the admin SDK in one transaction (~280 chunks), as `createSession` would.
  It takes about 37 s.

## Approximations

- **Edit rights need an extra attribute.** Instant rules can't filter a linked list (`data.ref`) by another
  attribute, so "is an owner or editor of this session" can't be read from `members.role`. Each membership
  carries `editor_author` (its `member_author` when the role is owner or editor, else null). The membership's
  create rule checks it matches the role, and memberships can't be updated, so the two can't drift.
- **Write receipts**: `reportWrite` waits on the `transact` promise. Instant rejects it and rolls back the
  optimistic write when the server refuses it, which stands in for Jazz's global-tier wait.
- **Offline**: Instant caches queries and queues writes, but sign-in needs the server.

## Harness notes

- `signIn` uses the app's sign-in form, which loads the app; the Jazz hooks sign in through an API without
  loading a page. So `signIn` then goes to a same-origin page that loads no app code (`/api/auth/sign-in`) and:
  - clears Instant's cached query results (the `querySubs` IndexedDB store), keeping the session;
  - clears the app's localStorage;
  - clears the context's HTTP cache over CDP.

  The timed first open then starts as cold as the Jazz one. This was added on 2026-11-14 after the first
  canonical runs; runs before it had a warm HTTP cache and Instant query cache on the first open.
