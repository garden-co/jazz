# PosterShop

PosterShop is a collaborative gig-poster designer. Everyone on a poster edits
the same SVG artboard live: rectangles, ellipses, text and uploaded images,
arranged on ordered layers, with each collaborator's cursor drawn on the
poster. It works offline and syncs when the connection returns.

The UI uses the Jazz design system (Astryx components with the Jazz theme);
the artboard itself is app code drawn on design tokens. Shape colours are
stored as palette keys (`ink`, `sun`, `blue`, …) that map to data tokens, so
a poster never carries raw colour values.

## What each surface reads

The studio is split into independently subscribed surfaces, so a high-rate
write in one never re-runs another's query:

| Surface          | Query                                                       |
| ---------------- | ----------------------------------------------------------- |
| Artboard         | `shapes` and `layers` of the canvas, ordered by `zIndex`    |
| Cursor layer     | `cursors` of the canvas where `active` — nothing else       |
| Inspector        | the one selected shape                                      |
| Asset shelf      | asset metadata columns only (never the `bytes` large value) |
| Asset thumbnails | `select({ bytes: { from, to } })` pages of one asset        |
| History          | checkpoint labels; the snapshot JSON only while previewing  |

Cursor moves are throttled to one write per 50 ms and drags to one write per
animation frame. Both are ordinary local-first updates.

## Authorization

`permissions.ts` gives every child table its own role predicate (#1926).
No client can create a canvas or promote itself: canvases and first admins
come only from the server bootstrap, and other memberships from an admin or
the server invite route.

- Members (viewers included) read the poster and publish only their own
  cursor row. Nobody can write, reassign or delete another member's cursor.
- Editors and admins create, rename, reorder, hide and lock layers, and
  create, move, resize, recolour, reorder and delete shapes.
- A shape must sit on an unlocked layer of its own canvas. Locking a layer is
  enforced by the policy, not only the UI, and it denies cross-canvas
  attachment.
- Editors and admins upload images. Asset bytes are immutable; a replacement
  is a new asset.
- Admins save checkpoints. Checkpoints are immutable and cannot be deleted.
- Admins issue, list and revoke invite links.

## Invite links

An invite link looks like `/dashboard#invite/<canvasId>/<token>`. The token
lives in the URL fragment, so it never reaches server logs, CDN logs or
`Referer` headers; the client posts it to `/api/join` in the request body.
The route follows the invite-links recipe: one exclusive transaction checks
for an existing membership (so reopening a link is idempotent and never
changes an existing role), reads the private invite, inserts the membership
and deletes a single-use invite, so two people cannot both redeem one.

## Images as large values

An upload streams the file into `assets.bytes` with `db.insertStreaming`, so
the app never holds the whole file as one array. Thumbnails and image shapes
read the bytes back in 256 KiB pages with typed large-value range selections
(#2088) and share one object URL per asset.

## Checkpoints

A checkpoint stores a JSON snapshot of the poster's layers and shapes. The
History panel previews any checkpoint on the artboard, read-only, and "Back
to live" returns to the shared poster. Jazz documents branch views with a
frozen base, but no public API yet produces the snapshot reference a frozen
base needs, so the snapshot is application data. Restoring a checkpoint into
the live poster is a follow-up.

## First open

`/api/bootstrap` verifies the caller with `client.forRequest(request)` and
writes through `client.withAttributionForRequest(request)`, so it keeps
backend authority while stamping the verified user as author. One exclusive
transaction checks for an existing membership and otherwise creates the
canvas, the admin membership and a deterministic demo poster (see
`src/lib/demo-poster.ts`) with a "First draft" checkpoint. Conflicting
first opens retry with bounded exponential backoff; persistent conflicts end
in a recoverable 503 instead of a hung request (#2615). Concurrent calls for
one account share a single run on the server, and the dashboard shares one
request between React StrictMode's double mount.

## Tests

- `pnpm test` runs the policy receipts and the server actions (bootstrap and
  invite redemption: idempotency, racing first opens, racing single-use
  redeems, no role downgrade) against a local Jazz server, plus unit tests
  for the poster model, the seed and the conflict retry.
- `pnpm test:browser` runs the browser → serving core topology receipt:
  concurrent ordered edits, an offline local shape across persistent reopen,
  replay to a peer after reconnect, a bounded shape-window query and a
  post-revocation write denial.

## Running the Next/Better Auth example

Copy `apps/nextjs-betterauth/.env.example` to `.env.local`, then replace the
local Jazz app id and server URL when connecting to a deployed backend. The
checked-in defaults deliberately let `pnpm --dir apps/nextjs-betterauth build`
evaluate auth routes without an unset configuration; they do not start a Jazz
server for you. Memberships and cursor identity use the canonical Jazz account
id, so equal provider subjects from different issuers cannot share access.
