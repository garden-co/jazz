# Wequencer (Next.js + Better Auth)

Wequencer is a collaborative step-sequencer example: a session has members,
ordered tracks, ordered pads, transport observations, and advisory presence.
It is a product-shaped Jazz example, not an audio engine.

## What it demonstrates

- Better Auth persists generated tables through a trusted Jazz backend. Those
  tables are deny-all to clients. Profiles and memberships store Jazz's
  canonical issuer-scoped author, not a raw provider user id.
- The authenticated dashboard performs idempotent profile bootstrap on the
  server. Queries never create profiles, sessions, or membership rows.
- The session creator manages membership through creator-bound policy. The
  `owner` role records the creator's initial membership but is not transferable;
  richer ownership semantics are tracked in [#2100](https://github.com/garden-co/jazz/issues/2100).
  Editors change tracks, pads, and transport observations; viewers only read.
- Each pad is an ordinary indexed row. Parent-scoped ordered queries keep a
  4×16 grid locally responsive and converge independent edits after reconnect.
- Presence heartbeats run every five seconds independently of subscription
  rerenders. Observations may remain stale; they are advisory and never authorize a write.

## Checks

```sh
pnpm exec tsc --noEmit
pnpm test
pnpm test:browser:focused -- tests/browser/topology.e2e.test.ts
cargo test -p jazz-example-wequencer-benchmark --tests
```

The topology receipt covers creator bootstrap, editor admission, ordered 64-pad
projection, local offline edits, reconnect convergence, viewer denial, and
advisory presence. The benchmark uses the same session/track/pad query shapes
and asserts their ordering and subscription delivery contract.

## Non-goals

`transport_observations` records convergent UI state only. It does not provide
sample-accurate clock synchronization, audio scheduling authority, conflict
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
