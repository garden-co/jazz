# HTTP/SSE Transport Protocol

Wire protocol for client-server communication in Jazz. Clients connect via SSE for push updates and use REST endpoints for mutations.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                           CLIENT                                     │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐  │
│  │  JazzClient     │    │ ServerConnection│    │  SSE Listener   │  │
│  │  (jazz-rs)      │───▶│  (HTTP POST)    │    │  (EventSource)  │  │
│  └─────────────────┘    └─────────────────┘    └─────────────────┘  │
│           │                     │                       │           │
│           │              mutations                 events           │
└───────────┼─────────────────────┼───────────────────────┼───────────┘
            │                     │                       │
            ▼                     ▼                       ▼
┌─────────────────────────────────────────────────────────────────────┐
│                           SERVER                                     │
│           │                     │                       │           │
│           │              REST API                 SSE endpoint      │
│           │                     │                       │           │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐  │
│  │ JazzRuntime     │◀───│   Axum Router   │◀───│  Broadcast      │  │
│  │ (groove-runtime)│    │  (jazz-cli)     │    │  Channel        │  │
│  └─────────────────┘    └─────────────────┘    └─────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

## Endpoint Reference

| Route                 | Method | Description                 |
| --------------------- | ------ | --------------------------- |
| `/events`             | GET    | SSE stream for push updates |
| `/sync`               | POST   | Push sync payload to server |
| `/sync/subscribe`     | POST   | Subscribe to a query        |
| `/sync/unsubscribe`   | POST   | Unsubscribe from a query    |
| `/sync/object`        | POST   | Create new object           |
| `/sync/object`        | PUT    | Update existing object      |
| `/sync/object/delete` | POST   | Delete object               |
| `/health`             | GET    | Health check                |

## Client Identity

### Client ID Persistence

Clients generate a persistent `ClientId` (UUIDv7) on first connection:

```
data_dir/
├── rocksdb/          # Local storage
└── client_id         # Persistent client ID (UUID string)
```

**Lifecycle:**

1. On `connect()`, check for `data_dir/client_id` file
2. If exists: load and parse UUID
3. If missing or corrupt: generate new UUIDv7, persist to file
4. Use this ID for all server communication

**Why persistence matters:**

- Server maintains per-client state (`sent_tips`, query subscriptions)
- Reconnecting with same ID preserves state, avoids re-sending all data
- Different clients (different data_dir) get unique IDs

### Client ID in Requests

The same `ClientId` must be used for both:

1. **SSE connection**: Query parameter `?client_id=<uuid>`
2. **HTTP requests**: `client_id` field in `SyncPayloadRequest`

**Critical:** Server's `process_from_client()` silently drops payloads where `client_id` isn't registered. Mismatch between SSE and HTTP client IDs causes data loss.

## SSE Events Endpoint

### Connection

```
GET /events?client_id=<uuid>
```

**Query parameters:**

- `client_id` (optional): Client's persistent UUID
  - If valid UUID: server uses this ID
  - If malformed: server returns `400 Bad Request`
  - If missing: server generates new ID (backwards compatible)

**Response:** `text/event-stream`

### Server Registration

When SSE connects, server:

1. Parses `client_id` from query param (or generates new)
2. Calls `add_client_with_full_sync(client_id, session)`
3. Sends all existing data to new client
4. Subscribes client to sync broadcast channel

### Event Format

Events are JSON-encoded `ServerEvent`:

```
data: {"type":"Connected","connection_id":1,"client_id":"550e8400-e29b-41d4-a716-446655440000"}

data: {"type":"SyncUpdate","payload":{...}}

data: {"type":"Heartbeat"}
```

### ServerEvent Types

```rust
pub enum ServerEvent {
    /// Connection established
    Connected {
        connection_id: ConnectionId,
        client_id: String,  // Confirms which ID server is using
    },

    /// Subscription acknowledged
    Subscribed { query_id: QueryId },

    /// Sync data push
    SyncUpdate { payload: SyncPayload },

    /// Error notification
    Error { message: String, code: ErrorCode },

    /// Keep-alive (every 30s)
    Heartbeat,
}
```

### Reconnection

Client maintains reconnection loop:

```rust
loop {
    let mut es = EventSource::get(&format!("{}/events?client_id={}", base_url, client_id));

    while let Some(event) = es.next().await {
        match event {
            Ok(Event::Message(msg)) => handle_server_event(msg),
            Err(_) => break,  // Disconnect
        }
    }

    // Reconnect after delay
    tokio::time::sleep(Duration::from_secs(5)).await;
}
```

**Current behavior:**

- Fixed 5-second retry delay
- Same `client_id` on reconnect (preserves server state)
- Server resumes sending from `sent_tips` (no duplicate data)

**Future enhancement:** Exponential backoff.

## REST Endpoints

### Push Sync Payload

```
POST /sync
Content-Type: application/json

{
    "payload": { ... SyncPayload ... },
    "client_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

**Important:** `client_id` must match the ID used for SSE connection.

### Subscribe to Query

```
POST /sync/subscribe
Content-Type: application/json

{
    "query": { ... Query ... },
    "schema_context": {
        "env": "client",
        "schema_hash": "a1b2c3d4...",
        "user_branch": "main"
    },
    "session": null
}
```

**Response:**

```json
{ "query_id": 42 }
```

### Create Object

```
POST /sync/object
Content-Type: application/json

{
    "table": "todos",
    "values": [{"Text": "Buy milk"}, {"Boolean": false}],
    "schema_context": { ... },
    "session": null
}
```

**Response:**

```json
{ "object_id": "..." }
```

### Update Object

```
PUT /sync/object
Content-Type: application/json

{
    "object_id": "...",
    "updates": [["completed", {"Boolean": true}]],
    "schema_context": { ... },
    "session": null
}
```

### Delete Object

```
POST /sync/object/delete
Content-Type: application/json

{
    "object_id": "...",
    "schema_context": { ... },
    "session": null
}
```

## Server-Side Architecture

### State Management

```rust
pub struct ServerState {
    pub runtime_handle: RuntimeHandle,
    pub connections: RwLock<HashMap<u64, ConnectionState>>,
    pub next_connection_id: AtomicU64,
    pub sync_broadcast: broadcast::Sender<(ClientId, SyncPayload)>,
}

pub struct ConnectionState {
    pub client_id: ClientId,
}
```

### Broadcast Channel

Server uses `tokio::sync::broadcast` for SSE routing:

1. Runtime produces `SyncOutbox` entries
2. Event processor sends `(client_id, payload)` to broadcast channel
3. Each SSE stream filters for its `client_id`

```rust
// Event processor (spawned on server start)
while let Some(event) = events.recv().await {
    if let RuntimeEvent::SyncOutbox(entry) = event {
        let _ = sync_tx.send((entry.client_id, entry.payload));
    }
}

// SSE stream (per-client)
while let Ok((target_client_id, payload)) = sync_rx.recv().await {
    if target_client_id == my_client_id {
        yield ServerEvent::SyncUpdate { payload };
    }
}
```

## Client-Side Architecture

### JazzClient Structure

```rust
pub struct JazzClient {
    runtime_handle: RuntimeHandle,
    server_connection: Option<Arc<ServerConnection>>,
    client_id: ClientId,
    // ... subscription tracking ...
}
```

### Event Processing

Client spawns two background tasks:

**1. Runtime Event Processor:**

```rust
while let Some(event) = events.recv().await {
    match event {
        RuntimeEvent::SyncOutbox(entry) => {
            // Forward to server via HTTP POST
            conn.push_sync(entry.payload, client_id).await;
        }
        RuntimeEvent::SubscriptionUpdate { handle, delta } => {
            // Route to subscription channel
            senders.get(&handle)?.send(delta).await;
        }
    }
}
```

**2. SSE Listener:**

```rust
loop {
    let url = format!("{}/events?client_id={}", base_url, client_id);
    let mut es = EventSource::get(&url);

    while let Some(event) = es.next().await {
        if let Ok(Event::Message(msg)) = event {
            let server_event: ServerEvent = serde_json::from_str(&msg.data)?;
            handle_server_event(server_event, &runtime_handle).await;
        }
    }

    // Reconnect
    tokio::time::sleep(Duration::from_secs(5)).await;
}
```

### Server Event Handling

```rust
async fn handle_server_event(event: ServerEvent, runtime: &RuntimeHandle) {
    match event {
        ServerEvent::Connected { connection_id, client_id } => {
            tracing::info!("Connected: {:?}, client: {}", connection_id, client_id);
        }
        ServerEvent::SyncUpdate { payload } => {
            // Push to local runtime inbox
            let entry = InboxEntry {
                source: Source::Server(ServerId::default()),
                payload,
            };
            runtime.push_sync_inbox(entry).await?;
        }
        ServerEvent::Heartbeat => { /* ignore */ }
        ServerEvent::Error { message, code } => {
            tracing::error!("Server error {:?}: {}", code, message);
        }
        _ => {}
    }
}
```

## Sync Flow

### Client → Server (Mutations)

```
1. JazzClient.create("todos", values)
     ↓
2. RuntimeHandle.insert() → creates object, commits
     ↓
3. tick() → processes outbox → emits RuntimeEvent::SyncOutbox
     ↓
4. Event processor → ServerConnection.push_sync(payload, client_id)
     ↓
5. HTTP POST /sync with SyncPayloadRequest
     ↓
6. Server runtime processes inbox
     ↓
7. Broadcasts to other connected clients
```

### Server → Client (Push Updates)

```
1. Server runtime has new data (from other client or local)
     ↓
2. SyncManager queues outbox entry for client
     ↓
3. tick() → takes outbox → sends via broadcast channel
     ↓
4. SSE stream receives (filters by client_id)
     ↓
5. Sends ServerEvent::SyncUpdate over SSE
     ↓
6. Client SSE listener receives, parses
     ↓
7. Pushes InboxEntry to local runtime
     ↓
8. Local indices update, subscriptions react
```

### New Client Full Sync

```
1. Client connects: GET /events?client_id=<new-uuid>
     ↓
2. Server: add_client_with_full_sync(client_id, None)
     ↓
3. SyncManager queues all existing objects for this client
     ↓
4. Server sends SyncUpdate events for everything
     ↓
5. Client receives, indexes all data
     ↓
6. Client queries return complete results
```

## Error Handling

### HTTP Errors

| Status | Meaning                                         |
| ------ | ----------------------------------------------- |
| 200    | Success                                         |
| 201    | Created (for POST /sync/object)                 |
| 400    | Bad Request (malformed client_id, invalid JSON) |
| 500    | Internal Server Error                           |

### SSE Errors

Server sends `ServerEvent::Error`:

```rust
pub enum ErrorCode {
    BadRequest,
    Unauthorized,
    Forbidden,
    NotFound,
    Internal,
    RateLimited,
}
```

## Security Considerations

> **⚠️ SECURITY TODO: App Admin Token for Schema Pushes**
>
> Currently, any client can push schema/lens objects to the server. This requires a separate **app admin token**:
>
> - Normal session tokens: authorize data operations (CRUD on rows)
> - App admin tokens: authorize schema operations (push schemas/lenses)
>
> App admin tokens should be:
>
> - Issued to developers/operators only
> - Required in HTTP headers for catalogue object sync
> - Validated server-side before accepting schema changes
>
> See `schema_manager.md` for full details on the security gap.

### Client ID as Session Token

Current model treats `ClientId` as a bearer token:

- 122 bits of entropy (UUIDv7 random portion)
- Computationally infeasible to guess
- Persisted client-side only

**Limitations:**

- No server-side session validation
- No expiration
- Stolen client_id grants full access to that client's session

**Future work:** Proper authentication layer with sessions, tokens, and policies.

### Transport Security

- All communication should use HTTPS in production
- CORS configured permissively for development (`CorsLayer::permissive()`)

## Implementation Status

### Complete

- [x] SSE endpoint with client_id parameter
- [x] Client ID persistence in data_dir
- [x] HTTP POST for sync payloads
- [x] Broadcast channel routing
- [x] Automatic reconnection (fixed delay)
- [x] Full sync on client connect
- [x] ServerEvent types with client_id confirmation

### Partial

- [ ] `/sync/subscribe` and `/sync/unsubscribe` are stubs (return hardcoded values)
- [ ] No exponential backoff for reconnection
- [ ] No connection state tracking API (`is_connected()`)

### Future

- [ ] Authentication/authorization layer
- [ ] WebSocket alternative to SSE
- [ ] Binary protocol option (MessagePack/CBOR)
- [ ] Request signing/verification
