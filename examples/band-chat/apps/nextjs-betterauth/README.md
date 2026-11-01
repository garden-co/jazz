# BandChat (Next.js + Better Auth)

BandChat is the small reference app for private rooms, membership boundaries,
inline attachments, and local-first message creation. It is deliberately a
product slice, not another generic Todo tutorial.

## What it demonstrates

- **A chat app, not a demo form.** Rooms in a side nav ordered by recent
  activity, with unread counts; a room view built from the design system's chat
  components; inline image previews, audio players and downloadable file chips;
  emoji reactions; shared sketches; profiles with names and photos. The UI is
  Astryx with the Jazz theme from `@garden-co/design`, using design tokens only.
- **Jazz runs inside the Next app.** `src/lib/jazz.server.ts` calls
  `createJazz`, and `app/jazz/[...path]/route.ts` mounts it under `/jazz`, so
  browsers sync with BandChat's own origin. Better Auth owns the browser
  session; `authenticate` reads it, and the client follows Better Auth's
  session with `betterAuth(authClient)`. There are no JWTs, JWKS or token
  routes. Sign-up and sign-in only call Better Auth.
- Better Auth's generated tables are persisted through the backend's own Jazz
  database (`jazz.db`) and carry explicit deny-all client policies. Read hooks do not create
  accounts, profiles, rooms, or memberships: a profile is created on an explicit
  first-run step.
- **Creator-managed admission.** A room creator bootstraps their own membership
  and is the only identity that admits or removes others. A room link
  (`?join=<room id>`) does not grant anything: it lets a signed-in person _ask_ to
  join by writing a `joinRequests` row that only they and the room creator can
  read. The creator admits that request; there is no other way in. Every
  membership names the admitted account's own profile, and the policy requires
  either the creator's own account or a join request from the admitted account
  for that room, so knowing someone's account id is not enough to put them in
  a room. A guest cannot add themself. Members may leave on their own. Secure,
  revocable bearer invite capabilities belong to
  [#1954](https://github.com/garden-co/jazz/issues/1954).
- **Profile visibility follows relationships.** A profile is readable by its
  owner, by co-members, by anyone who can read a message it sent, and by a room
  creator reviewing its join request.
- A message must reference a profile owned by `session.user.account`. Profiles,
  memberships, and row provenance store that enrolled account UUID. The
  external issuer and subject remain account identity metadata, never
  membership values. Revocation rejects subsequent writes at the serving
  authority; it does not erase rows already retained locally.
- **Unread state.** `rooms.lastActivityAt` is a denormalized carrier that any
  member may bump (an `exists` check against the stored row keeps the name
  creator-only). The value is not bounded by the policy: a member can write any
  time, including a future one, which keeps the room at the top of everyone's
  list. Opening the room still clears it, because a read marker never lags the
  room's activity. Each reader keeps a private `readMarkers` row per room; a room
  is unread when its activity is newer than the marker, and only unread rooms pay
  for a bounded count query.
- **Attachments** stream into the message row with `db.insertStreaming`. The
  room timeline selects message metadata only. An image or audio attachment
  reads its bytes once it scrolls near the viewport, and any other file only
  when it is downloaded. The picker accepts images, audio, text
  and PDF up to 10 MB. That limit is client-side UX validation only, not a Jazz
  authorization, security, or storage limit: `s.bytes()` has no size
  constraint, so an actor otherwise allowed to insert a message can write a
  different-sized value directly.
- **Sketches.** A message can carry a canvas; every finished stroke is one row,
  so strokes from bandmates appear live and offline strokes sync later. Stroke
  inserts carry `roomId` and require current membership, so a removed member can
  no longer draw.
- Room creation, messages, reactions and strokes are ordinary local-first
  writes, so they appear before a reconnect.

## Setup

```sh
pnpm dev
```

`pnpm dev` needs no configuration. It serves http://127.0.0.1:3000, keeps
Jazz data in the git-ignored `.jazz/` directory, and generates a local
`BETTER_AUTH_SECRET` into the git-ignored `.env.development.local`. No secret is
checked in. If Better Auth reports that it cannot decrypt its private key after
the secret changed, delete `.jazz/`.

Configuration fails closed. Local defaults apply only to a non-production
process on a loopback origin; a production build or start must set every value
listed in `.env.example`, or it refuses to start (`src/lib/config.mjs`,
`tests/config.test.ts`).

By default the backend stores data in SQLite next to the app. To sync through
Jazz Cloud or your own sync server instead, set `JAZZ_SERVER_URL`,
`JAZZ_ADMIN_SECRET` and `BACKEND_SECRET`; the backend then uses `remote()`.
Browsers still talk only to `/jazz` and never see these values.

## Checks

```sh
pnpm typecheck
pnpm test:permissions
pnpm test:browser
pnpm build
```

The permission receipt covers the normal path (owner creates a room, bootstraps
membership, admits a guest, and the guest posts) and the important failures:
self-admission, selecting someone else's profile as sender, and posting after
removal, including a same-subject/different-issuer owner check. A second case
covers join requests (visible only to requester and creator, own profile only),
profile visibility, member-only admission denial, activity-only room updates,
private read markers, and sketch strokes before and after leaving. `test:browser`
deploys the real permissions to a test authority and drives the React UI through
profile setup, room creation, a guest opening the room link and asking to join,
the owner admitting the request, guest messaging, and removal. It also covers
the local create/send/react path and attachment-picker validation. The browser receipt
enrolls test accounts directly against a test authority; it does not exercise
Better Auth's HTTP session endpoints.

## Non-goals

This app intentionally does not restore the retired Todo app, a separate sync
server, app-local worker/WASM copies, or a compatibility path for pre-canonical
author identifiers. The room link is deliberately an "ask to join" link, not a
bearer capability; secure, revocable invite capabilities belong to
[#1954](https://github.com/garden-co/jazz/issues/1954).

## Known limits

- A creator cannot add someone they already share another room with unless
  that person asks to join. The policy for that would use
  `allowedTo.read("memberProfile")`, which on INSERT currently requires UPDATE
  authority on the profile rather than READ
  ([#1900](https://github.com/garden-co/jazz/issues/1900)).
