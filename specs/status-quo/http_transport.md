# HTTP/SSE Transport Protocol — Status Quo

Jazz uses a deliberately small HTTP transport surface:

- `POST /sync` for client-to-server batches
- `GET /events` for server-to-client streaming updates

That is enough because the interesting structure lives inside the typed sync payloads, not in a sprawling list of special-purpose endpoints.

## The Two Channels

### `/sync`

Clients POST a `SyncBatchRequest`:

- one `client_id`
- an ordered list of `SyncPayload`s

The server applies them in order and returns a `SyncBatchResponse` with one success/error result per payload.

### `/events`

Clients open a long-lived stream and receive `ServerEvent`s such as:

- `Connected`
- `SyncUpdate`
- `Error`
- `Heartbeat`

The stream uses length-prefixed binary frames containing JSON payloads. That keeps parsing simple without turning the SSE body into newline-escaped JSON soup.

## What Actually Travels

The transport does not invent a second data model. It carries the same sync payloads the runtime already understands:

- row versions
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

The `Connected` event also carries stream bookkeeping such as the connection id and, when available, the server's current catalogue digest.

## Auth

The current transport supports three main auth shapes:

- JWT bearer auth for normal client sessions
- backend-secret impersonation for trusted server-side callers
- admin-secret auth for administrative or catalogue-specific flows

The important idea is that auth is checked at the HTTP boundary, while row-level visibility still lives in the runtime's query/policy machinery.

## Why There Is No Separate "Query Transport"

A query subscription is just another sync payload.

That is a very intentional design choice. It means:

- browser worker links
- native client/server links
- server/server links

can all use the same transport vocabulary instead of inventing a query-only side protocol.

## Current Route Surface

The in-repo server keeps a small route set:

- `/events`
- `/sync`
- `/schemas`
- `/schema/:hash`
- `/health`

The cloud server exposes equivalent app-scoped routes under `/apps/:app_id/...` while preserving the same transport semantics.

## Key Files

| File | Purpose |
| --- | --- |
| `crates/jazz-tools/src/transport_protocol.rs` | Shared request/event types and framing |
| `crates/jazz-tools/src/routes.rs` | In-repo server routes |
| `crates/jazz-tools/src/middleware/auth.rs` | HTTP auth handling |
| `crates/jazz-tools/src/transport.rs` | Rust client-side transport |
| `packages/jazz-tools/src/runtime/sync-transport.ts` | TypeScript transport helpers |
| `crates/jazz-cloud-server/src/server.rs` | Cloud server transport wiring |
