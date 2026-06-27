# HTTP/WebSocket Direct Transport Protocol - Status Quo

Jazz uses a deliberately small transport surface:

- `GET /apps/:app_id/ws` for bidirectional sync over WebSocket
- `GET /apps/:app_id/schemas` and `GET /apps/:app_id/schema/:hash` for schema catalogue reads
- `GET /apps/:app_id/admin/permissions/head` plus `POST /apps/:app_id/admin/...` for admin publication and inspection flows
- `GET /health` at the server root

That is enough because the interesting structure lives inside the typed sync payloads, not in a sprawling list of special-purpose endpoints.

## The Main Channel

### `/apps/:app_id/ws`

Clients open one WebSocket and exchange core wire frames.

Every WebSocket message uses the same outer frame shape:

```text
[4 bytes: u32 big-endian compressed payload length][N bytes: LZ4-compressed payload]
```

The initial auth handshake payload is JSON before compression. Once both sides
confirm the core wire protocol, post-handshake messages carry core
`WireFrame` payloads rather than the old alpha `SyncPayload` WebSocket event
envelope.

The connection is app-scoped, so every non-health server interaction uses the same `/apps/<app_id>/...`
prefix as the cloud server.

Server-to-server upstream sync through the old alpha transport is paused while
the core engine path becomes canonical. Core client WebSocket files are
kept under the server module.

## What Actually Travels

The transport does not invent a second data model. It carries core wire
messages that the core database already understands.

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
- admin-secret auth for administrative and catalogue-specific flows

The important idea is that auth is checked at the HTTP boundary, while row-level visibility still lives in the runtime's query/policy machinery.

Catalogue authority remains core-owned.

## Why There Is No Separate "Query Transport"

A query subscription is just another sync payload.

That is a very intentional design choice. It means:

- browser worker links
- native client/server links

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

| File                                                | Purpose                         |
| --------------------------------------------------- | ------------------------------- |
| `crates/jazz-tools/src/server/websocket_client.rs`  | WebSocket client transport      |
| `crates/jazz-tools/src/server/routes/websocket.rs`  | WebSocket server route          |
| `crates/jazz-tools/src/server/routes/`              | In-repo HTTP server routes      |
| `crates/jazz-tools/src/middleware/auth.rs`          | HTTP auth handling              |
| `packages/jazz-tools/src/runtime/sync-transport.ts` | TypeScript transport helpers    |
| `packages/jazz-tools/src/runtime/worker-bridge.ts`  | Browser worker transport bridge |
