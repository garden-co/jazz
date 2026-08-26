# BandChat (Next.js + Better Auth)

BandChat is the small reference app for private rooms, membership boundaries,
inline attachments, and local-first message creation. It is deliberately a
product slice, not another generic Todo tutorial.

## What it demonstrates

- Better Auth owns the browser session, signs an ES256 JWT, and exposes its
  JWKS route; Jazz recreates its provider after a principal change and only
  refreshes a JWT for the same principal.
- Better Auth's generated tables are persisted through a trusted backend Jazz
  context and carry explicit deny-all client policies. The dashboard performs
  the one idempotent profile bootstrap after authentication; read hooks do not
  create accounts, profiles, rooms, or memberships.
- A room creator may bootstrap their own membership and admit another profile.
  The room UI lists members and lets its issuer-scoped creator admit or remove
  them. A guest cannot add themself. A message must reference a profile owned by
  the authenticated raw Better Auth user id, while Jazz records row provenance
  from the issuer-scoped `session.author`. Revocation rejects subsequent writes
  at the serving authority; it does not erase rows already retained locally.
- The attachment picker accepts inline PNG, JPEG, WebP, text, and PDF files up
  to 256 KiB. This is client-side UX validation only, not a Jazz authorization,
  security, or storage limit: `s.bytes()` has no size constraint, so an actor
  otherwise allowed to insert a message can write a different-sized value
  directly. Larger media belongs in the file/blob pattern; enforce any
  authoritative content limit at a trusted application boundary.
- Room creation and messages are ordinary local-first writes, so they appear
  before a reconnect. The browser receipt exercises the real React form; the
  policy receipt exercises serving-authority admission and removal.

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
membership, invites a guest, and the guest posts) and the important failures:
self-admission, selecting someone else's profile as sender, and posting after
removal, including a same-subject/different-issuer owner check. `test:browser`
deploys the real permissions to a test authority and drives the React UI through
owner creation, guest admission, guest messaging, and removal. It also covers
the local create/send path and attachment-picker validation. The browser receipt
uses test-authority JWTs; it does not claim to exercise Better Auth's HTTP
session/JWKS endpoints.

## Non-goals

This app intentionally does not restore the retired Todo app, a separate app
backend, app-local worker/WASM copies, or a compatibility path for pre-canonical
author identifiers. It also does not treat direct room-membership writes as a
shareable invite-link product. Secure, revocable invite capabilities belong to
[#1954](https://github.com/garden-co/jazz/issues/1954).
