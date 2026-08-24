# BandChat — Next.js + Better Auth

A self-contained BandChat application based on the current `create-jazz` `next-betterauth` scaffold.

## Run it

```bash
pnpm install
pnpm --filter band-chat-nextjs-betterauth dev
```

Open `http://127.0.0.1:3000`, create an account, then create a demo room. `withJazz` starts the local Jazz authority in development. The scaffold-style `scripts/ensure-env.js` creates a local Better Auth secret when needed; cloud deployments supply `.env.example` variables through their secret manager.

## Recommended structure

- `app/page.tsx` is the public server-side session gate.
- `app/dashboard/layout.tsx` validates the session and completes idempotent profile provisioning before ordinary Jazz UI mounts.
- `components/jazz-provider.tsx` owns JWT acquisition and refresh.
- `src/App.tsx` keeps room list, selected conversation, message list, composer, and attachment input as narrow query-owning components. Reads remain declarative and side-effect free.
- `schema.ts` and `permissions.ts` are the application-owned data and authorization contract.

Rooms use Jazz's trusted `$createdBy` metadata for ownership. Messages project and order by trusted `$createdAt`; writes never accept client-declared provenance. Only a room creator can admit or remove members, and a message sender profile must belong to the authenticated external identity. Better Auth persistence and profile bootstrap are the only trusted-backend operations; ordinary behavior uses JWT/JWKS clients.

Attachments are inline bytes capped at 256 KiB and restricted to PNG, JPEG, WebP, text, or PDF before reading the file. A production large-file variant should use Jazz large values.

## Checks

```bash
pnpm --filter band-chat-nextjs-betterauth typecheck
pnpm --filter band-chat-nextjs-betterauth test:headless
pnpm --filter band-chat-nextjs-betterauth test:browser
pnpm --filter band-chat-nextjs-betterauth build
```

The deployed permission receipt proves legitimate owner creation/invite/send, rejects foreign self-admission and forged authorship, and exercises `$createdBy` at the edge. Browser coverage proves offline IndexedDB retention, reconnect, and delivery to a fresh persistent reader through the canonical Jazz test server.

`tests/browser/topology.e2e.test.tsx` is the adopter-owned public-API receipt for
two browser clients connected to one core: it verifies concurrent messages,
reactions, ordinary inline attachment bytes, offline replay, and ordered
subscription convergence. Its shared-harness receipt covers authorization
rejection plus browser disconnect/reconnect. True edge → core → peer-edge
coverage remains separate follow-up work. The focused expected-failure test is
intentionally linked to #1844 (reproducing PRs #1830 and #1838): indirect
Text/Bytea materialization at the receiving browser remains a known red
regression and must not be replaced with an inline-only fixture.
