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
- Better Auth owns the browser session, signs an ES256 JWT, and exposes its
  JWKS route. Jazz enrolls that JWT into an account handle: sign-up uses
  `registerJWT`, sign-in uses `loginJWT`, and the provider receives the result.
- Better Auth's generated tables are persisted through a trusted backend Jazz
  context and carry explicit deny-all client policies. Read hooks do not create
  accounts, profiles, rooms, or memberships: a profile is created on an explicit
  first-run step.
- **Creator-managed admission.** A room creator bootstraps their own membership
  and is the only identity that admits or removes others. A room link
  (`?join=<room id>`) does not grant anything: it lets a signed-in person *ask* to
  join by writing a `joinRequests` row that only they and the room creator can
  read. The creator admits a request, or adds someone they already share a room
  with. A guest cannot add themself, and a membership may only name a profile
  owned by the admitted account. Members may leave on their own. Secure,
  revocable bearer invite capabilities belong to
  [#1954](https://github.com/garden-co/jazz/issues/1954).
- **Profile visibility follows relationships.** A profile is readable by its
  owner, by co-members, by anyone who can read a message it sent, and by a room
  creator reviewing its join request. The "people you know" picker is simply
  every readable profile.
- A message must reference a profile owned by `session.user.account`. Profiles,
  memberships, and row provenance store that enrolled account UUID. The
  external issuer and subject remain account identity metadata, never
  membership values. Revocation rejects subsequent writes at the serving
  authority; it does not erase rows already retained locally.
- **Unread state.** `rooms.lastActivityAt` is a denormalized carrier that any
  member may bump (an `exists` check against the stored row keeps the name
  creator-only). Each reader keeps a private `readMarkers` row per room; a room
  is unread when its activity is newer than the marker, and only unread rooms pay
  for a bounded count query.
- **Attachments** stream into the message row with `db.insertStreaming`. The
  room timeline selects message metadata only; each attachment reads its own
  bytes when it is shown or downloaded. The picker accepts images, audio, text
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
cp .env.example .env
pnpm dev
```

`withJazz` supplies public Jazz app/server configuration in development. Set a
real `BETTER_AUTH_SECRET` and `BACKEND_SECRET` before any shared deployment.

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
uses test-authority JWTs; it does not claim to exercise Better Auth's HTTP
session/JWKS endpoints.

## Non-goals

This app intentionally does not restore the retired Todo app, a separate app
backend, app-local worker/WASM copies, or a compatibility path for pre-canonical
author identifiers. The room link is deliberately an "ask to join" link, not a
bearer capability; secure, revocable invite capabilities belong to
[#1954](https://github.com/garden-co/jazz/issues/1954).
