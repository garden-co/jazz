# Wequencer (Next.js + Better Auth)

Wequencer is a collaborative step-sequencer example: a session has members,
instrument tracks, a list of patterns of up to 64 steps, shared transport
observations, and advisory presence. Every bandmate hears the pattern through
a small Web Audio drum machine synthesised in the browser (no samples).

## What it demonstrates

- Better Auth persists generated tables through a trusted Jazz backend. Those
  tables are deny-all to clients. Profiles and memberships store Jazz's
  canonical issuer-scoped author, not a raw provider user id.
- The authenticated dashboard performs idempotent profile bootstrap on the
  server. Queries never create profiles, sessions, or membership rows.
- The session creator manages membership through immutable `$createdBy`
  metadata. The `owner` role records the creator's initial collaboration
  membership but is neither transferable nor administrative; deleting or
  downgrading that row does not change creator authority. Richer ownership
  semantics are tracked in [#2100](https://github.com/garden-co/jazz/issues/2100).
  Editors change tracks, pads, and transport observations; viewers only read.
- Pads are sparse: a missing row means the pad is off. Each pad's row id is
  derived from a hash of its track, pattern and position, and the first press
  upserts it. Two bandmates pressing the same untouched pad therefore write
  the same row instead of creating duplicates, and adding a track or pattern
  writes no pad rows at all. Parent-scoped ordered queries keep a grid of up
  to 16 tracks × 64 steps locally responsive and converge independent edits
  after reconnect. A step's track and pattern must belong to its session, and
  the transport may only point at one of the session's own patterns.
- Creating a session, and removing a track with its pads, each commit as one
  transaction.
- Play, stop, tempo and the playing pattern are shared: each change appends a
  `transport_observations` row, and every client extrapolates the playhead
  from the newest one with its own wall clock. Bandmates hear roughly the
  same step; clock-accurate sync is out of scope. Mute, solo and volume are
  shared track columns, so the band hears one mix. Sound is opt-in per device
  because browsers only start audio after a gesture.
- A bandmate's display name becomes readable once their profile has shown
  presence in a session you can read.
- Presence heartbeats run every five seconds independently of subscription
  rerenders. Observations may remain stale; they are advisory and never authorize a write.

## Checks

```sh
pnpm exec tsc --noEmit
pnpm test
pnpm test:browser:focused -- tests/browser/topology.e2e.test.ts
cargo test -p jazz-example-wequencer-benchmark --tests
```

The policy unit tests (`lib/permissions.test.ts`) cover editor and viewer pad
writes, cross-session patterns in steps and transport, two bandmates adding a
track and a pattern concurrently and then pressing the same pad, and profile
visibility through presence.

The topology receipt, run through the current fingerprinted WASM/NAPI artifacts,
covers creator bootstrap, immutable creator administration after an `owner`
membership change, editor admission, ordered 64-pad projection, local offline
edits, reconnect convergence, viewer pad and transport rejection, cross-session
pattern rejection, revocation of a former editor, and advisory presence. The
native benchmark still models the single-pattern shape (pads queried by track,
without patterns); aligning it with this schema is separate work.

## Non-goals

`transport_observations` records convergent transport state only. It does not provide
sample-accurate clock synchronization or skew correction between clients, conflict
resolution for simultaneous edits to the same pad, presence expiry guarantees,
or a secure invite-capability product. Those require separate designs rather
than app-local assumptions.

## Setup

```sh
cp .env.example .env
pnpm dev
```

`withJazz` supplies public Jazz configuration during local development. The
same `NEXT_PUBLIC_APP_ORIGIN` is the Better Auth origin and the canonical author
issuer. Set stable `BETTER_AUTH_SECRET` and `BACKEND_SECRET` values before deploying.
