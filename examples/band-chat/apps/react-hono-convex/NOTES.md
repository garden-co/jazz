# BandChat on Convex: notes

## Versions

- `convex` 1.50.0, local backend `precompiled-2026-10-22-1447815`
- `@convex-dev/auth` 0.0.98 with `@auth/core` 0.37.4, **Password** provider (email and
  password, at least 8 characters). Why not 2.0.0: see `stage-plan/convex/NOTES.md`.
- React 19.2.4, Vite 8.0.1, same Astryx / design packages as the Jazz copy

## Backend and hooks

A local anonymous deployment (`CONVEX_AGENT_MODE=anonymous npx convex dev`, no account).
Its data lives in `.convex/`, and the JWT keys and `SITE_URL` come from
`scripts/setup-auth.mjs`. `harness.mjs` works from a fresh checkout: it runs
`pnpm install` if needed, configures the deployment, starts `convex dev`, seeds, builds
and serves `dist/` with `vite preview` on :4403. `rttUrl` is the backend's `/version`.

**Seed** (`scripts/seed.mjs`, `pnpm seed` with `pnpm backend` running): it mirrors
`band-chat/jazz/seed.ts`.
1. `seed:reset` wipes every table.
2. The seed user, a and b sign up through the Password provider (`auth:signIn`,
   `flow: "signUp"`) and each creates a profile through `profiles.create`.
3. The internal `seed:createRooms` writes the seed user's 256 rooms. Each gets its
   creator's membership (what `rooms.create` writes) and `lastActivityAt` set to the
   fixture's `createdAtMs`, like the Jazz seed.
4. a and b ask to join every room (`joinRequests.create`), and the seed user admits them
   (`members.admit`). These go through the app's own public functions and their checks.
5. The internal `seed:writeMessages` writes the 10,000 messages in fixture order as their
   senders' profiles. Like the Jazz seed's direct inserts, this doesn't bump room activity.

`signIn(page, key)` fills in the app's own sign-in form; it's untimed.

Set `HARNESS_CONVEX_LOGS=always` to write the backend's function logs (timeouts, auth
warnings) to `.convex/dev.log` during a run; by default they're off.

**Local backend limits**: the anonymous local deployment enforces Convex's per-deployment
limits. These include 4 MiB written per second, a 1 s query execution limit, and a cap on
bursts of new subscriptions. The seed retries a batch that hits the write limit.

## How the port maps

- **Permissions**: the Jazz `permissions.ts` rules are checks in every function
  (`convex/lib/access.ts`). Members read a room's messages, reactions, members and
  sketches. Only the creator renames the room and admits people, and only people who
  asked to join (or the creator), each with their own profile. Members leave on their
  own and the creator removes anyone. Senders delete their own messages, and reactions
  and strokes belong to their author. Join requests are visible to the requester and the
  creator, read markers to their reader. A reaction's room comes from its message, not
  the caller.
- **Transactions**: room + creator membership, admit + clear requests, leave + drop
  read markers, and sketch canvas + message + activity are each one mutation.
- **Ids and timestamps**: `_id` and `_creationTime` replace `id` and `$createdAt` in the
  copied components, and `rooms.creator` replaces `$createdBy`. Timestamps are numbers
  (ms), so the components wrap them in `new Date(…)` where they show them.
- **Unsent messages**: Jazz tracks a write until the server accepts or rejects it, and
  matches late rejections by transaction id. Convex mutations reject their promise (the
  client keeps retrying while offline), so `UnsentMessages.track` takes that promise. The
  notices, and putting the text back into the draft, work the same.

- **Unread badges**: the Jazz room nav holds one unread-count subscription per room.
  Here `RoomNav` asks one query, `messages.unreadCounts`, for every room that has
  activity past your marker (not the open one). The per-room version started about 255
  queries in the same moment a room opened. Under that burst the local backend timed
  some out ("Function execution timed out (maximum duration: 1s)"). A failed query makes
  `useQuery` throw while rendering, which unmounted the app mid-run (2026-11-14: the
  composer was gone in `bc_send`). One query per list is also the usual Convex shape.

## Optimistic updates

- **Sending a text message** (`Composer.tsx`) uses `withOptimisticUpdate`, which prepends
  the message to `messages.page` for the room. That's the example in Convex's
  "Optimistic updates" docs (a chat send), so `bc_send` measures the local echo, as it
  does for Jazz.
- Nothing else is optimistic: reactions, read markers, rooms, admission and strokes wait
  for the server.

## Approximations

- **Attachments** go to Convex file storage, through `generateUploadUrl` and a POST, with
  the message holding the storage id. Jazz streams the bytes into the row. Images and
  audio load from a storage URL once near the viewport, and files download through the
  same URL. Members get the URL through a checked query, but the URL itself is a bearer
  link once handed out. The harness doesn't touch attachments.
- **Avatars** stay small `v.bytes()` on the profile, as in Jazz.
- **Profile directory**: Jazz can read any profile behind a message you can read.
  `profiles.directory` returns your own profile plus those of co-members and people
  asking to join your rooms. The sender of an old message who has since left shows as
  "Bandmate".
- **Profiles**: Jazz lets two tabs create two profiles (the oldest wins). Here
  `profiles.create` is idempotent per account.
- **New room**: Jazz closes the dialog before the server confirms. Here it closes once
  the mutation returns the room id.
- **History**: the newest 200 messages (`HISTORY_PAGE`), as in the Jazz copy. Neither has
  scroll-back paging.
