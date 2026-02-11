# HTTP/SSE Transport Protocol — Status Quo

This is the concrete wire protocol that carries the abstract sync messages defined by the [Sync Manager](sync_manager.md) over the network. The design prioritizes simplicity: SSE (Server-Sent Events) for server→client push, a single POST endpoint for client→server mutations. No WebSocket complexity, no custom protocol — just HTTP with binary framing for efficiency.

The client opens a persistent `/events` SSE connection for receiving updates, and sends mutations via `POST /sync`. This asymmetry matches the sync model: the server pushes data matching the client's subscriptions, while the client pushes mutations and subscription changes.

## Architecture

```
┌────────────────────────────────────────────────────────────┐
│                         CLIENT                              │
│  ┌──────────────┐  ┌───────────────┐  ┌─────────────────┐  │
│  │ JazzClient   │  │ HTTP POST     │  │ Binary Stream   │  │
│  │ (jazz-rs)    │──▶│ (/sync)       │  │ (/events)       │  │
│  └──────────────┘  └───────────────┘  └─────────────────┘  │
└────────────────────┬────────────────────┬──────────────────┘
                     │                    │
┌────────────────────┴────────────────────┴──────────────────┐
│                         SERVER                              │
│  ┌──────────────┐  ┌───────────────┐  ┌─────────────────┐  │
│  │ JazzRuntime  │◀─│  Axum Router  │◀─│ Broadcast       │  │
│  │(groove-tokio)│  │  (jazz-cli)   │  │ Channel         │  │
│  └──────────────┘  └───────────────┘  └─────────────────┘  │
└────────────────────────────────────────────────────────────┘
```

## Endpoints

| Route | Method | Description |
|-------|--------|-------------|
| `/events` | GET | Binary streaming for push updates |
| `/sync` | POST | Unified sync endpoint (all mutations, subscriptions, blobs) |
| `/health` | GET | Health check |

Note: The original spec described separate endpoints (`/sync/subscribe`, `/sync/object`, etc.). These were consolidated into a single `/sync` endpoint that accepts polymorphic `SyncPayload` variants — simpler routing, unified auth/logging.

> `crates/jazz-cli/src/routes.rs:62-287`

## Wire Format

**Binary length-prefixed frames** instead of raw JSON over SSE. Standard SSE sends newline-delimited text, which means every message needs escaping and parsing overhead. Binary framing is simpler: read 4 bytes for length, read that many bytes for the JSON payload.

```
[4 bytes u32 BE length][JSON payload]
```

Both the `/events` stream and individual sync messages use this format.

> `crates/jazz-transport/src/lib.rs:174-202` (encode_frame, decode_frame)

## Client Identity

Clients generate a persistent `ClientId` (UUIDv7) on first connection, stored locally at `data_dir/client_id`. The same ID is used for both `/events` stream (`?client_id=<uuid>` query param) and `/sync` requests (`client_id` field in body).

This persistence matters for reconnection efficiency: when a client reconnects with the same ID, the server's `sent_tips` tracking means it only sends new data since the last connection, not everything from scratch.

> `crates/jazz-rs/src/client.rs:65-84`

## SSE Events Endpoint

`GET /events?client_id=<uuid>` — requires valid session (JWT or backend secret).

Server calls `ensure_client_with_session(client_id, session)` on connect.

### Event Types

| Event | Purpose |
|-------|---------|
| `Connected` | Confirms connection, returns `connection_id` and `client_id` |
| `SyncUpdate` | Push sync data (wraps `SyncPayload`) |
| `Heartbeat` | Keep-alive every 30s |

> `crates/jazz-cli/src/routes.rs:132-166`

### Reconnection

Fixed 5s retry delay. Same client_id preserves server state. Server resumes from `sent_tips`.

> `crates/jazz-rs/src/client.rs:167-257`

## Authentication

Three independent mechanisms, resolved in priority order:

| Priority | Mechanism | Headers | Purpose |
|----------|-----------|---------|---------|
| 1 | Backend impersonation | `X-Jazz-Backend-Secret` + `X-Jazz-Session` | Backend apps impersonate users |
| 2 | JWT | `Authorization: Bearer <JWT>` | Frontend/mobile clients |
| 3 | No session | — | Anonymous (limited) |

Admin auth (`X-Jazz-Admin-Secret`) required separately for catalogue sync operations.

> `crates/jazz-cli/src/middleware/auth.rs:226-350` — extensive test coverage at lines 352-583

### Client-Side Auth

`jazz-rs` transport detects catalogue objects by metadata type and automatically sends with admin headers.

> `crates/jazz-rs/src/transport.rs:66-181`

## Broadcast Channel

Server uses `tokio::sync::broadcast` for SSE routing:
1. Runtime produces SyncOutbox entries
2. Event processor sends `(client_id, payload)` to broadcast channel
3. Each SSE stream filters for its `client_id`

## Sync Flow

**Client → Server**: CRUD → RuntimeCore commits locally → SyncManager outbox → SyncSender callback → HTTP POST `/sync` → server inbox → broadcast to other clients.

**Server → Client**: Server runtime outbox → broadcast channel → SSE stream filtered by client_id → client parses binary frames → local runtime inbox → indices update, subscriptions react.

## Key Files

| File | Purpose |
|------|---------|
| `crates/jazz-cli/src/routes.rs` | Server endpoints (events, sync, health) |
| `crates/jazz-cli/src/middleware/auth.rs` | Authentication middleware |
| `crates/jazz-transport/src/lib.rs` | Shared types, frame encoding |
| `crates/jazz-rs/src/client.rs` | Rust client (streaming, reconnection) |
| `crates/jazz-rs/src/transport.rs` | Client-side HTTP transport |
