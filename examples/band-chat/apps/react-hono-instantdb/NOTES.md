# BandChat on InstantDB

## Versions and hosting

- `@instantdb/react` and `@instantdb/admin` 1.0.67, `instant-cli` 1.0.67; React 19.2.4, Vite 8.0.1, Hono 4 (as in
  the Jazz copy).
- Data: a **hosted temporary Instant app** (no account). `harness.mjs` runs `pnpm install` on a fresh checkout,
  creates the app into `.env` (git-ignored) and pushes `instant.schema.ts` and `instant.perms.ts` on every
  `seed()`. Temporary apps expire; delete `.env` and the next run makes a new one.
- Region: `api.instantdb.com` via CloudFront IAD (us-east). The app's own Hono server (sign-in, static files)
  runs locally, like the Jazz copy's.

## How it maps

- **Auth**: email + password through Instant's documented custom-auth route. `server/auth.ts` keeps scrypt
  password hashes in an admin-only `credentials` namespace and mints an Instant token with
  `adminDb.auth.createToken({ email })`; the browser calls `db.auth.signInWithToken(token)`. The sign-in form,
  its labels and copy are unchanged. The name given at sign-up is stored on `$users.name` and pre-fills the
  profile setup. Unlike Better Auth in the Jazz copy, sign-ins are not rate-limited.
- **Schema**: the Jazz tables become namespaces; a row's room, sender, canvas and so on are **links**, so the
  rules can follow them with `data.ref`. Reactions link only to their message (no denormalized `roomId`) and
  load nested in the room's message query. `$createdAt` and `$createdBy` become explicit `createdAt` and
  `rooms.creator` attributes (checked on create).
- **Permissions** (`instant.perms.ts`): the Jazz rules, default-deny elsewhere. I checked them against the
  hosted app with a creator, a joiner and an outsider: self-admission, admitting someone without a request,
  posting as someone else, renaming as a member, outsiders reading or reacting, deleting someone else's message
  and posting after removal are all rejected.
- **Attachments**: Instant Storage (`$files`), uploaded into the sender's own folder, readable by members of a
  room whose message links them. The UI still loads an image or audio file only once it scrolls near the
  viewport.
- **Avatars**: a data-URL string on the profile (Jazz keeps the bytes in the row).
- **Seed** (`seed.ts`): users sign up through the app's own `signUp`. Rows are then written with the admin SDK,
  in the final state of the app's admission flow (every user a member of every room, no open requests). This
  is not replayed request by request as the Jazz seed does. Messages carry the fixture's `createdAtMs`. It
  takes about 30 s.

## Approximations

- **Admission is two transactions.** Instant evaluates a create rule against the state *after* the
  transaction, so a membership whose rule needs the join request can't commit together with deleting that
  request. The app admits first, then deletes the request.
- **Profiles of former members.** The Jazz rule also shows the profile of anyone who sent a message you can
  read. As an Instant rule (`data.ref('messages.room.members.memberAuthor')`) that made every profile check
  walk all of a profile's messages: opening the busy room took over 5 s on the server, and the profiles query
  3.6 s. I dropped that clause, so a sender who has left the room shows as "Bandmate".
- **Unsent messages**: tracked through the `transact` promise. Instant rejects it and rolls back the
  optimistic write when the server refuses it, which replaces Jazz's transaction-id matching.
- **Offline**: Instant caches queries and queues writes, but sign-in needs the server.

## Observations

- **Runs fail intermittently under BandChat's startup load.** Each client opens one unread-count query per room
  with unread activity: 255 for the bench users, as in the Jazz app, plus the room list and the open room. Each
  unread query checks up to 100 messages against the membership rule. Against the hosted temporary app this
  sometimes overwhelms the session: the server answers *every* query of that page load with
  `operation-timed-out` ("Operation timed out: handle-receive", status 500). The Instant client doesn't
  re-subscribe after such an error, so the page stays on "Loading rooms…" until it is reloaded, and the
  harness times out. In my sandbox, more than half of the runs after I fixed the profile rule hit a stall. I
  captured the timeout frames for the stall after the room switches; the stalls in `bc_cross_client` (a's
  message box disappears once b signs in) look the same but I didn't capture them. Runs that don't stall give
  the numbers in the PR. I
  didn't restructure the unread counts: that would be tuning the app for Instant.
- **Captured timeout frames** (one stalled page load: `a` reloading "Busy room" after the room switches in
  `bc_switch_room`, recorded with Playwright's WebSocket events). Frame counts for that load:

  ```
  out:init 1, in:init-ok 1, out:add-query 261, in:add-query-ok 1, in:error 260,
  out:remove-query 256, in:remove-query-ok 256
  ```

  One of the 260 error frames (the room-list query; truncated where the capture cut it, bench user id elided):

  ```json
  {"op":"error","status":500,"client-event-id":"2a186d48-8146-4f8c-8c16-50d26e66d700",
   "original-event":{"client-event-id":"2a186d48-8146-4f8c-8c16-50d26e66d700","op":"add-query",
     "q":{"rooms":{},"roomMembers":{"$":{"where":{"memberAuthor":"<bench user a>"}},"room":{"$":{"fields":["id"]}}},
          "readMarkers":{"$":{"where":{"reader":"<bench user a>"}},"room":{"$":{"fields":["id"]}}}},
     "session-id":"6af81db3-a6a0-41e6-a8cf-833489bb922c",
     "instant.grouped-queue/put-at":1794645428834,"total-delay-ms":3,"ws-ping-latency-ms":0.0,
     "trace-id":"df4a6a296a1d7b162d69a6116af81db4"},
   "type":"operation-timed-out","message":"Operation timed out: handle-receive","hint":{"timeo…
  ```

  No `add-query` was re-sent for the errored queries after this; the page stayed on "Loading rooms…" until
  the harness's 60 s timeout.
- Sending shows the bubble after about 0.5 s, not at once. Instant's client does run a leading `limit` query
  optimistically (`instaql.js`), so this isn't a wait for the server on that account. My unconfirmed guess is
  the client re-running its ~260 subscribed queries (room list, unread counts, the room) on every write.

## Harness notes

- `signIn` uses the app's sign-in form, which loads the app; the Jazz hooks sign in through an API without
  loading a page. So `signIn` then goes to a same-origin page that loads no app code (`/health`) and:
  - clears Instant's cached query results (the `querySubs` IndexedDB store), keeping the session;
  - clears the app's localStorage;
  - clears the context's HTTP cache over CDP.

  The timed first open then starts as cold as the Jazz one. This was added on 2026-11-14 after the first
  canonical runs; runs before it had a warm HTTP cache and Instant query cache on the first open.
