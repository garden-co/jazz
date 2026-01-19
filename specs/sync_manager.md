# Sync Manager Architecture

The sync manager sits atop the ObjectManager to coordinate network synchronization. It manages connections to upstream servers (trusted, receive everything) and downstream clients (untrusted, query-filtered).

## Core Concepts

### Connection Types

| | Upstream Servers | Downstream Clients |
|---|---|---|
| Trust | Trusted | Untrusted |
| Scope | All objects | Query-filtered |
| Direction | Bidirectional | We push to them, they can push back |
| Permissions | Full access | Per-object: Readable or ReadableAndWritable |

### Server Interaction Model

**Upward (us → server):**
- We push ALL our objects to the server
- We forward client queries to the server (so server knows what we're interested in)

**Downward (server → us):**
- Server sends updates matching any forwarded query
- This enables multi-tier: client queries us → we forward to server → server sends data we don't have → we forward to client

### Permission Model

Clients access objects through queries with per-branch permissions:

```
Permission
├── Readable           # Can receive updates, cannot push
└── ReadableAndWritable  # Can receive AND push updates
```

The effective scope merges all active queries, taking the most permissive permission when overlapping.

### Pending Updates

When clients push updates for objects NOT in their effective scope:
1. Update queued to `pending_updates`
2. Upper layer evaluates (policy decision)
3. Upper layer calls `approve_update()` or `reject_update()`
4. Approved: applied and distributed; Rejected: Error sent to client

This enables custom authorization logic (e.g., "client can create objects of type X").

## Identifiers

| Type | Format | Purpose |
|------|--------|---------|
| ServerId | UUIDv7 | Identifies server connection |
| ClientId | UUIDv7 | Identifies client connection |
| QueryId | u64 | Identifies a query subscription |
| PendingUpdateId | u64 | Identifies a pending update awaiting approval |

## State Structures

### SyncManager

```rust
SyncManager {
    object_manager: ObjectManager,
    servers: HashMap<ServerId, ServerState>,
    clients: HashMap<ClientId, ClientState>,
    inbox: Vec<InboxEntry>,
    outbox: Vec<OutboxEntry>,
    pending_updates: Vec<PendingUpdate>,
}
```

### ServerState

```rust
ServerState {
    sent_tips: HashMap<(ObjectId, BranchName), HashSet<CommitId>>,
    sent_metadata: HashSet<ObjectId>,
    forwarded_queries: HashMap<QueryId, HashMap<(ObjectId, BranchName), Permission>>,
}
```

Tracks what we've sent to the server and which queries we've forwarded.

### ClientState

```rust
ClientState {
    queries: HashMap<QueryId, HashMap<(ObjectId, BranchName), Permission>>,
    effective_scope: HashMap<(ObjectId, BranchName), Permission>,
    sent_tips: HashMap<(ObjectId, BranchName), HashSet<CommitId>>,
    sent_metadata: HashSet<ObjectId>,
}
```

Tracks client queries, derived effective scope, and what we've sent.

## Message Protocol

### SyncError

Strongly typed errors for sync operations:

```rust
enum SyncError {
    /// Operation denied due to insufficient permission.
    PermissionDenied { object_id: ObjectId, branch_name: BranchName, reason: String },
    /// Blob request denied due to insufficient permission.
    BlobAccessDenied { blob_id: BlobId },
    /// Blob not found in storage.
    BlobNotFound { blob_id: BlobId },
}
```

### SyncPayload

```rust
enum SyncPayload {
    ObjectUpdated {
        object_id: ObjectId,
        metadata: Option<ObjectMetadata>,  // Included on first send
        branch_name: BranchName,
        commits: Vec<Commit>,              // Topologically sorted
    },
    ObjectTruncated {
        object_id: ObjectId,
        branch_name: BranchName,
        tails: HashSet<CommitId>,
    },
    BlobRequest { blob_id: BlobId },
    BlobResponse { blob_id: BlobId, data: Vec<u8> },
    QueryRegistration { query_id: QueryId, scope: ... },
    QueryUnregistration { query_id: QueryId },
    Error(SyncError),
}
```

### Inbox/Outbox

```rust
struct InboxEntry {
    source: Source,     // Server(ServerId) or Client(ClientId)
    payload: SyncPayload,
}

struct OutboxEntry {
    destination: Destination,  // Server(ServerId) or Client(ClientId)
    payload: SyncPayload,
}
```

## Public API

### Connection Management

```rust
fn add_server(&mut self, server_id: ServerId)
fn remove_server(&mut self, server_id: ServerId)
fn add_client(&mut self, client_id: ClientId)
fn remove_client(&mut self, client_id: ClientId)
```

Adding a server triggers full sync of all existing objects.

### Query Management (Clients)

```rust
fn add_or_update_query(
    &mut self,
    client_id: ClientId,
    query_id: QueryId,
    scope: HashMap<(ObjectId, BranchName), Permission>,
)
fn unsubscribe_from_query(&mut self, client_id: ClientId, query_id: QueryId)
```

Query changes trigger:
- **Scope expansion**: Initial sync for newly-visible objects
- **Scope contraction**: Stop sending future updates (no "unsend")

### Query Forwarding (Servers)

```rust
fn forward_query_to_server(
    &mut self,
    server_id: ServerId,
    query_id: QueryId,
    scope: HashMap<(ObjectId, BranchName), Permission>,
)
fn unforward_query_from_server(&mut self, server_id: ServerId, query_id: QueryId)
```

Used for multi-tier sync: forward client queries upstream.

### Message Handling

```rust
fn push_inbox(&mut self, entry: InboxEntry)
fn take_outbox(&mut self) -> Vec<OutboxEntry>
fn process_inbox(&mut self)
```

### Pending Updates

```rust
fn take_pending_updates(&mut self) -> Vec<PendingUpdate>
fn approve_update(&mut self, pending_id: PendingUpdateId)
fn reject_update(&mut self, pending_id: PendingUpdateId, reason: String)
```

## Processing Flow

### Local Change → Outbox

1. Local operation modifies ObjectManager
2. Call `forward_update_to_servers(object_id, branch_name)`
3. For each server: compute diff vs `sent_tips`, queue `ObjectUpdated`
4. For each client with (object, branch) in scope: compute diff, queue update

### Inbox from Server → Apply + Forward

1. Validate and apply to ObjectManager
2. For each client whose scope includes (object, branch): queue to outbox
3. Update tracking state

### Inbox from Client → Permission Check

**In scope with ReadableAndWritable:**
- Apply to ObjectManager
- Forward to servers and other relevant clients

**In scope with Readable (read-only):**
- Queue `Error` response

**Out of scope:**
- Queue to `pending_updates` for upper layer evaluation

### Blob Handling

**Request:**
1. Check requester has read permission for associated object
2. Authorized: queue `BlobResponse` with data
3. Not authorized: queue `BlobResponse` with `data: None`

## Invariants

### Server Sync (INV-S)
1. **Completeness**: All local objects eventually synced to all servers
2. **Causal order**: Commits sent parent-before-child

### Client Scope (INV-C)
1. **No leakage**: Clients only receive updates for objects in their effective_scope
2. **Initial sync**: Query additions trigger current state for newly-visible objects
3. **Scope removal**: Query removals stop future updates (no unsend)

### Permission Enforcement (INV-P)
1. **Read-only enforcement**: Clients with `Readable` permission cannot push updates
2. **Truncation control**: Truncations require `ReadableAndWritable`

### Consistency (INV-X)
1. **Metadata once**: `ObjectMetadata` sent exactly once per destination per object
2. **Tip tracking accuracy**: `sent_tips` accurately reflects what destination has seen

## Usage Pattern

```rust
// Create manager
let mut sm = SyncManager::new();

// Add connections
let server_id = ServerId::new();
sm.add_server(server_id);

let client_id = ClientId::new();
sm.add_client(client_id);

// Set up client query
let mut scope = HashMap::new();
scope.insert((obj_id, "main".into()), Permission::ReadableAndWritable);
sm.add_or_update_query(client_id, QueryId(1), scope);

// Local operations (via object_manager)
let obj_id = sm.object_manager.create(None);
let commit_id = sm.object_manager.add_commit(obj_id, "main", vec![], content, author, None)?;

// Trigger sync
sm.forward_update_to_servers(obj_id, "main".into());
sm.forward_update_to_clients(obj_id, "main".into());

// Process network messages
// ... network layer delivers to inbox ...
sm.process_inbox();
let outbound = sm.take_outbox();
// ... network layer sends outbound ...

// Handle pending updates
for pending in sm.take_pending_updates() {
    if policy_allows(&pending) {
        sm.approve_update(pending.id);
    } else {
        sm.reject_update(pending.id, "Not authorized".into());
    }
}
```

## Design Decisions

1. **Query-based client scope**: Clients declare interest via queries; SyncManager enforces scope. Queries are opaque to SyncManager (evaluated by upper layer).

2. **Explicit sync triggers**: Local changes don't auto-sync. Caller must invoke `forward_update_to_*`. Enables batching and control.

3. **Pending update queue**: Out-of-scope client updates aren't rejected immediately. Upper layer has policy flexibility.

4. **Metadata deduplication**: Object metadata sent exactly once per destination, reducing bandwidth.

5. **Topological commit ordering**: Commits always sent parent-before-child for causal consistency.

6. **Permission merge semantics**: Multiple queries for same (object, branch) take most permissive. Simplifies reasoning.

7. **Query forwarding for multi-tier**: Enables hub-and-spoke topologies where intermediate nodes can request subsets from upstream.
