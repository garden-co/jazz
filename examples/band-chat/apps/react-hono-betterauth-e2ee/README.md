# BandChat, end-to-end encrypted (Vite React + Hono + Better Auth)

[`../react-hono-betterauth`](../react-hono-betterauth) with message text,
attachments (their bytes and their names) and read markers end-to-end
encrypted. Each room is an encrypted space, and only its members hold its key.
The server stores and relays those columns as ciphertext and holds no keys.

- `server/jazz.ts` calls `createJazz`, and `server/index.ts` mounts it under
  `/jazz` (plain requests through `jazz.handle`, sync sockets through
  `jazz.attach(server)`), next to Better Auth at `/api/auth/*`. The browser
  talks only to this app's own origin.
- Better Auth's tables live in BandChat's own Jazz (`jazz.db`), and
  `authenticate` reads the Better Auth session cookie. No JWTs.
- In production the Hono server also serves the built SPA from `dist/`. In
  development Vite serves the app on http://127.0.0.1:3000 and proxies `/api`
  and `/jazz` to the Hono server on port 3001.

## Run it

```sh
pnpm dev          # http://127.0.0.1:3000, data in .jazz/, local secrets generated
pnpm build        # checks the deployment config, then builds dist/
pnpm start        # production server on APP_ORIGIN's port (or PORT)
pnpm test         # permissions, server, deploy and screen tests
pnpm test:e2e     # Playwright, against `pnpm dev`
```

Deployments set `APP_ORIGIN`, `JAZZ_APP_ID` and `BETTER_AUTH_SECRET`, and
optionally `JAZZ_SERVER_URL` with the app's secrets (see `.env.example`).

This example runs its Hono backend on Node. A Cloudflare Worker can also
serve encrypted apps by connecting to a native Jazz core with `remote()`.

### The browser tests

`e2e/` drives real Chromium browsers, each with its own storage profile, so
each is its own device with its own keys: two people in a room, and up to
three browsers of one person. To run them against a production build, build
first and drive the production server with a fresh data directory:

```sh
pnpm build
APP_ORIGIN=http://localhost:3000 JAZZ_APP_ID=$(uuidgen) \
  BETTER_AUTH_SECRET=$(openssl rand -hex 32) JAZZ_DATA_PATH=$(mktemp -d)/jazz \
  JAZZ_E2E_PROD=1 pnpm test:e2e
```

The tests read the server's own data: they search its data directory for
text that must not be there, and open a copy of its store to count read
markers and key versions. Each browser reaches the app through its own small
recording proxy, so the tests see everything it sends and receives, including
the sync connection Jazz opens from a SharedWorker (which Playwright's page
events do not show). The passkey test uses Chromium's virtual
authenticator, which needs a host name (`localhost`), not an IP address.

## What changed against the plain BandChat

1. **The provider** passes `encryption: { app }` beside `client:
   loadLocalFirst`. BandChat keeps its thin start: rooms show at once, message
   text waits for the local-first client, which holds this browser's keys.
2. **Encrypted columns** (`schema.ts`): `messages.text`, `messages.attachment`,
   `messages.attachmentName` and `readMarkers.lastReadAt`, each in the space
   `room`. Room names, reactions and sketches stay plain.
3. **A room's read rule** is `isMember("rooms", room.id)`. The creator's member
   row is inserted in the transaction that makes the room.
4. **Only a room's creator updates it** (renames it). Whoever may update a
   room manages its members, so a member who could update it could add
   anyone. `rooms.lastActivityAt` is gone, with the three places that wrote it.
5. **Room order and unread state come from each room's newest message**, read
   through plain columns only (`id`, `senderId`, `$createdAt`), one small query
   per room, served by the `["roomId", "$createdAt"]` index. The room list
   needs no key, shows every room its read rule returns, and does not wait for
   read markers.
6. **Read markers** are sealed with the room's key and readable only by their
   reader. One is written only after this browser has decrypted the room's
   newest message: never because none was found, and never by a browser that
   is waiting for approval or for the room's key.
7. **Membership** is Jazz's member row. `roomMembers` stays for the member's
   profile and the join request. Admitting inserts both and clears the
   request in one transaction, then delivers the room's key ("Key not
   delivered yet" with a retry if that fails). Removing and leaving delete both
   in one serializable transaction and wait for the server's answer. When
   the creator removes someone, BandChat tries to renew the room's key
   immediately and offers a retry if it fails. A member who leaves cannot
   renew the key. Once the creator's browser learns that the member left,
   Jazz tries to renew the key before the creator's next encrypted write.
8. **The members dialog** lists from the member rows, joined to profiles for
   names. A member with no profile shows as Unknown.
9. **Devices and recovery**, in Your profile: this browser's status, approving
   a new browser by its six-digit code, "Remove lost device", "Leave this
   browser", and "Set up recovery" (a passkey, or a phrase shown once). A
   browser waiting for approval can restore with the phrase or the passkey.
10. **Its own first schema version and its own store.** No migration from the
    plain app: encrypting a column of existing data is a conversion.
11. **The server** is `server/index.ts` and `server/jazz.ts`, plus one route,
    `POST /api/end-other-sign-ins`, used before a lost device is removed.

## What the server still sees

Encrypted: the text of a message, the bytes and the name of an attachment,
and the time stored in each read marker. The server still sees:

- who is in which room;
- who wrote a message, and when;
- room names, reactions and sketches;
- each person's profile: their display name and avatar;
- the type and the size of an attachment;
- **when each read marker was written.** The stored time is encrypted; the
  moment of the write is not, and it shows about when that person read the
  room.

**Read markers are hidden from the server, not from the room's other
members.** Every member holds the room's key, so every member could open a
marker. The read rule keeps the server from sending one member another's
marker, so reading one takes a member and the server together. Deploying
BandChat prints exactly one warning, which says this:

```
jazz.permissions.json: table readMarkers: its read rule is its space table rooms's plus further conditions, so every member of a space can decrypt rows of readMarkers that Jazz doesn't send them
```

It is expected. Giving the markers the room's own read rule removes it, and
lets every member read every marker.

## Removing a lost device

"Remove lost device" sits on every other device of yours that is active (a
device waiting for approval cannot be removed). It runs two steps:

1. **The server step.** BandChat's route deletes every Better Auth session of
   yours except this browser's, page by page, then closes your open Jazz
   connections (`jazz.revokeUser`). The server cannot tell which session
   belongs to which device, so it ends them all.
2. **The device step.** Then the browser calls `devices.remove` for that
   device.

**Every other browser of yours is signed out and has to sign in again.** They
keep their keys and their device records, so they need no new approval;
changes they had not synced yet may be lost. The screen says so before you
confirm. If the page goes away between the steps, the device is still
listed: press the button again, and both steps run again.

The removed device keeps the keys it already holds. It receives no new keys.
A device that's connected and watching the room learns of the removal one
sync round trip after it commits. Other devices learn of it when they next
sync the room. Writes can still use the current key before the device learns
of the removal, while offline after learning of it, or when an online
rotation is refused or undecided by the write's deadline. The write goes
ahead and the room's key rotates at the next opportunity.

## Signing out, and leaving a browser

**Signing out keeps this browser's keys**, its local data and its device
record. The next sign-in on this browser reads at once, with no approval.

On a shared computer that matters: a later user of the same browser profile,
or a script on the app's origin, can ask the browser to use the kept keys.
There are two ways to remove this browser as a device:

- **before signing out**, "Leave this browser" in Your profile;
- **afterwards**, "Remove lost device" on it from another of your devices.

**Leaving** asks another of your devices to remove this browser. An only
device cannot leave: nothing else would hold your keys, and BandChat says so.
Otherwise, once Jazz has published the request, the row shows "Removal
pending" and the browser stays usable. If another of your devices is online,
it may complete the removal before that shows. Once another of your devices
completes the removal, Jazz erases this browser's
keys for your account and the page signs itself out. If erasing fails,
BandChat says leaving did not finish and that the keys may still be here, and
stays signed in; reloading checks again.

Two known behaviours of leaving:

- **It waits for another of your devices to be online.** Jazz erases nothing
  before that, so your account is never left without a device that holds its
  key.
- **It leaves one more device on your list.** Once the leaving browser has
  seen its removal while signed in, it has enrolled again, so your other
  devices list one more device as "waiting for approval". That entry cannot
  be removed. Ignore it, or approve it to let that browser back in.

Leaving erases the keys, not the browser's local copy of the plain data.

## Limits

- **A join request guards the profile row only.** A room's creator can give
  any account a member row without a request, so "you only join a room you
  asked to join" is not enforced by the server. Such a member shows as
  Unknown until they have a profile row, and can leave.
- **Removing a member cannot take back what they read**, and removal is not
  instant: until the key is renewed, a message can be sealed under the old
  key.
- **Being admitted is not having the key.** A new member's devices get it from
  a device that holds it, once that device is online. Until then the room says
  its messages are decrypting. After ten seconds, an active browser says they
  are waiting for the room's key, and keeps saying so: it cannot tell a key
  that is late from one that no online device holds any more. If the room
  still has no key record after those ten seconds, or this browser's status
  is still unknown, the messages show as locked instead. A browser waiting
  for approval, or with no access to the key, shows them as locked at once.
- **Losing every device without recovery** loses the encrypted messages for
  good. Clearing site data on the only browser does the same. Set up recovery.
- **The room list runs one live query per room.** Fine for a band's handful of
  rooms, not built for hundreds.
- **Unread dots arrive after the room list** on a fresh load, once the
  local-first client and the read markers are there.
