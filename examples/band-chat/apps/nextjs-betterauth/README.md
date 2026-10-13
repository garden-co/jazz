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
- **Unread state.** The room list reads each room with its newest message
  (`messagesViaRoom`, newest first, limit 1), which also orders the list.
  Nobody writes to the room when they post, so no member can keep a room on
  top or hide new messages from others. Each reader keeps one `readMarkers` row
  per room holding the `$createdAt` of the newest message they have seen; it
  only moves forward. A room is unread when its newest message was sent by
  someone else after the marker, and only unread rooms pay for a count, capped
  at 100. `$createdAt` is the sender's clock, so a message sent from a clock
  far behind can land before the marker and not count as unread.
- **Read receipts.** Markers are readable by the room's members: under your own
  messages, one check mark means sent and two mean another member's marker has
  reached it. Every marker move also appends a `readProgress` row in the same
  transaction. "Read by" on your message lists the members whose journal
  reaches it, dated by the first entry that did. The journal grows with marker
  moves, not with messages times readers; a member who jumps to the end reads
  everything they skipped at once. BandChat does not limit who may see read
  dates by room size or message age; Telegram, for example, stops at 100
  participants and 7 days.
- **History.** A room opens on its newest 50 messages, each with its reactions.
  Older messages load 50 at a time before a cursor (`$createdAt < oldest
shown`), never by offset; once older pages are loaded, the live window keeps
  everything from its oldest message on, so new messages cannot open a gap.
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

`pnpm dev` needs no configuration. It serves http://127.0.0.1:3000, `withJazz`
supplies a local Jazz app and sync server, and local `BACKEND_SECRET` and
`BETTER_AUTH_SECRET` values are generated into the git-ignored
`.env.development.local`. No secret is checked in. The local sync server keeps
its data in `node_modules/.cache/jazz-dev-server`; if Better Auth reports that
it cannot decrypt its private key after the secret changed, delete that
directory.

Configuration fails closed. Local defaults apply only to a non-production
process on a loopback origin; a production build or start must set every value
listed in `.env.example`, or it refuses to start (`src/lib/config.mjs`,
`tests/config.test.ts`).

A deployed Jazz server must verify BandChat's tokens the way `withJazz` does
locally: JWKS at `<NEXT_PUBLIC_APP_ORIGIN>/api/auth/jwks`, JWT issuer equal to
`NEXT_PUBLIC_APP_ORIGIN`, and JWT audience `band-chat` (`--jwt-issuer` /
`--jwt-audience`, or `JAZZ_JWT_ISSUER` / `JAZZ_JWT_AUDIENCE`). Otherwise it
rejects every sign-in with 401. The browser and backend share one Jazz
environment (`NEXT_PUBLIC_JAZZ_ENV`, else `prod` in production, else `dev`).

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

## Known limits

- A creator cannot add someone they already share another room with unless
  that person asks to join. The policy for that would use
  `allowedTo.read("memberProfile")`, which on INSERT currently requires UPDATE
  authority on the profile rather than READ
  ([#1900](https://github.com/garden-co/jazz/issues/1900)).
