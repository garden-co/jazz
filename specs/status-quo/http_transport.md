# HTTP/WebSocket Transport Protocol — Status Quo

Jazz uses a deliberately small transport surface:

- `GET /apps/:app_id/ws` for bidirectional sync over WebSocket
- `GET /apps/:app_id/schemas` and `GET /apps/:app_id/schema/:hash` for schema catalogue reads
- `GET /apps/:app_id/admin/permissions/head` plus `POST /apps/:app_id/admin/...` for admin publication and inspection flows
- `GET /health` at the server root

That is enough because the interesting structure lives inside the typed sync payloads, not in a sprawling list of special-purpose endpoints.

## The Main Channel

### `/apps/:app_id/ws`

Clients and server peers open one WebSocket and exchange framed sync messages
carrying payloads such as:

- `Connected`
- `SyncUpdate`
- `SyncUpdateBatch`
- `Error`
- `Heartbeat`

Every WebSocket message uses the same outer frame shape:

```text
[4 bytes: u32 big-endian payload length][N bytes: payload]
```

The initial auth handshake payload is JSON. That keeps protocol-version and
auth failures readable for older or mismatched peers. Once both sides confirm
`SYNC_PROTOCOL_VERSION`, post-handshake sync transport payloads are binary
postcard payloads:

- client-to-server frames carry either a single outbox entry payload or a
  `SyncBatchRequest`
- server-to-client frames carry `ServerEvent` payloads, including coalesced
  `SyncUpdateBatch` events

The connection is app-scoped, so every non-health server interaction uses the same `/apps/<app_id>/...`
prefix as the cloud server.

Self-hosted edge servers use the same route when they connect upstream to a
core server. `jazz-tools server` without an upstream URL is the core/global
node. With `--upstream-url` or `JAZZ_UPSTREAM_URL`, it is an edge node and
opens a Rust WebSocket transport to the upstream core. Base `http`, `https`,
`ws`, and `wss` URLs are normalized to the app-scoped
`/apps/<app_id>/ws` route; URLs with query strings or fragments are rejected.

## What Actually Travels

The transport does not invent a second data model. It carries the same sync payloads the runtime already understands:

- row batch entries
- row state changes
- catalogue entries
- query subscriptions and unsubscriptions
- query-settled signals
- errors and warnings

That means transport code can stay thin. It does not need to understand relational semantics beyond "deserialize this payload and hand it to the runtime".

## Connection Identity

Clients use a stable `ClientId` across reconnects.

That matters for two reasons:

- the server can continue reasoning about the same logical client
- reconnect can resume with better anti-entropy instead of pretending every reconnect is a brand-new peer with no prior state

The `Connected` event also carries stream bookkeeping such as the connection id
and, when available, the server's current catalogue digest. Peer connections use
that digest during reconnect and initial sync so unchanged catalogue state does
not have to be replayed.

## Auth

The current transport supports four main auth shapes:

- JWT bearer auth for normal client sessions
- backend-secret impersonation for trusted server-side callers
- admin-secret auth for administrative or catalogue-specific flows
- peer-secret auth for server-to-server WebSocket links

The important idea is that auth is checked at the HTTP boundary, while row-level visibility still lives in the runtime's query/policy machinery.

Peer auth is carried in the WebSocket auth handshake as `peer_secret`. A core
server validates it against `AuthConfig.peer_secret`; a valid handshake is
registered as `ClientRole::Peer`, not as `Backend` or `Admin`. Edges require a
peer secret because they must authenticate to their upstream core. Cores only
need a peer secret when they accept edge connections.

Catalogue admin writes remain core-only. Edge servers learn schemas,
permissions, and migrations through the sync channel from core; they reject
local admin catalogue publishes with an error that tells callers to publish to
the core server instead.

## Why There Is No Separate "Query Transport"

A query subscription is just another sync payload.

That is a very intentional design choice. It means:

- browser worker links
- native client/server links
- server/server links

can all use the same transport vocabulary instead of inventing a query-only side protocol.

## Current Route Surface

The in-repo server keeps a small route set:

- `/apps/:app_id/ws`
- `/apps/:app_id/schemas`
- `/apps/:app_id/schema/:hash`
- `/apps/:app_id/admin/schemas`
- `/apps/:app_id/admin/migrations`
- `/apps/:app_id/admin/schema-connectivity`
- `/apps/:app_id/admin/permissions/head`
- `/apps/:app_id/admin/permissions`
- `/health`

## Key Files

| File                                                | Purpose                                |
| --------------------------------------------------- | -------------------------------------- |
| `crates/jazz-tools/src/transport_protocol.rs`       | Shared request/event types and framing |
| `crates/jazz-tools/src/server/routes/`              | In-repo server routes                  |
| `crates/jazz-tools/src/middleware/auth.rs`          | HTTP auth handling                     |
| `crates/jazz-tools/src/transport_manager.rs`        | Rust WebSocket transport manager       |
| `crates/jazz-tools/src/ws_stream/`                  | Concrete WebSocket stream adapters     |
| `packages/jazz-tools/src/runtime/sync-transport.ts` | TypeScript transport helpers           |
| `packages/jazz-tools/src/runtime/worker-bridge.ts`  | Browser worker transport bridge        |
