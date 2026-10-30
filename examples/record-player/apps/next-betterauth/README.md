# RecordPlayer (Next.js + Better Auth)

RecordPlayer is a music library with shared playlists. It is the Jazz example
for large binary values: audio files stream into Jazz as they upload, the
library browses metadata without touching audio bytes, and playback reads the
audio back in byte ranges.

## What it demonstrates

- **Streaming writes.** Uploading an album creates the album row, then streams
  each audio file into a `tracks.audio_bytes` column with `db.insertStreaming`.
  The file's `ReadableStream` is passed straight through; the progress bar
  counts bytes as Jazz consumes them (`src/upload.ts`).
- **Metadata-first reads.** The album shelf and track tables select only
  metadata columns. Cover images live in their own column and each cover is
  read on its own, so a large library renders before any art arrives
  (`app/cover-art.tsx`).
- **Range reads for playback.** The player reads audio in 512 KiB windows with
  typed large-value selections, `select({ audio_bytes: { from, to } })`
  (`src/audio-stream.ts`). MP3 and WebM windows are appended to a
  `MediaSource` so playback starts after the first window; WAV and other
  formats the browser can't append are assembled into a `Blob` first.
- **Ordered, collaborative playlists.** Entries use fractional `position`
  values. Adding appends after the last entry; moving picks the midpoint of the
  new neighbours, so concurrent edits from two editors converge without
  renumbering.
- **Sharing with roles.** The playlist owner invites another account as a
  listener or an editor. The invitation stays `pending` until the recipient
  accepts it; only then does the read (and, for editors, write) policy admit
  them. The owner can revoke. See `permissions.ts`.
- **Better Auth → Jazz.** Better Auth owns the browser session and signs an
  ES256 JWT; Jazz enrolls it (`registerJWT` on sign-up, `loginJWT` on sign-in)
  and the app opens a client for that account (`app/record-player-provider.tsx`).

The library is a shared catalogue: every signed-in account can add albums and
everyone can browse and play them. Playlists are private until shared.

## Run it

```sh
pnpm install          # at the repository root
pnpm build:ci         # builds jazz-tools and its native/WASM parts
cd examples/record-player/apps/next-betterauth
cp .env.example .env
pnpm dev
```

`withJazz` (in `next.config.ts`) starts a local Jazz server in development and
supplies the public app ID and server URL. Set real `BETTER_AUTH_SECRET` and
`BACKEND_SECRET` values before any shared deployment.

`NEXT_PUBLIC_APP_ORIGIN` is also the JWT issuer and audience. Better Auth signs
tokens with both, and the Jazz server is told to expect both (`jwtIssuer` and
`jwtAudience` next to `jwksUrl`). If they drift apart, sign-in still succeeds
but account login fails with `invalid account credential`.

Sign in with the pre-filled demo account (choose **Create account** the first
time). An empty library offers **Add demo library**: three albums of short
tones synthesised in the browser as WAV files (`src/demo-audio.ts`), so the
player works without any uploads. The bytes are identical on every run.

To try sharing, open a second browser profile, create another account, copy
its account ID from the **Invitations** tab, and invite it from the first
account's playlist **Share** dialog.

## Checks

```sh
pnpm typecheck
pnpm test            # unit, provider and test-selection receipts
pnpm test:topology   # browser + Jazz server: invitations, grants, offline editors
pnpm build
```

## Known limits

- Range reads currently materialise the whole stored value before slicing it
  ([#2090](https://github.com/garden-co/jazz/issues/2090)), so each window
  costs as much as a whole-value read. Windows are therefore large (512 KiB),
  and the short demo tracks fit in a single window; uploaded songs are read in
  several. The app reads in windows anyway, so it gets the benefit when exact
  chunk demand lands.
- Seeking ahead of the loaded range waits for the sequential reads to catch up;
  the player does not yet prioritise the window under the seek position.
- Invitations are addressed by Jazz account ID. There is no directory to look
  someone up by email, and the recipient cannot see a playlist's name until
  they accept, because the read policy admits accepted invitations only.
- Albums and tracks cannot be edited or deleted; the permissions only allow
  inserts into the shared catalogue. An upload that fails part-way leaves the
  album and the tracks written so far; retrying from the same dialog reuses
  that album.
