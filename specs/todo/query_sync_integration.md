# Query/Sync Integration — TODO

Remaining work items and known gaps.

> Status quo: [specs/status-quo/query_sync_integration.md](../status-quo/query_sync_integration.md)

## Error Handling for Failed Server-Side Query Compilation

**Priority: Medium**

When query compilation fails server-side (invalid query, schema mismatch), the client is NOT notified. The subscription is silently dropped. Should send `SyncPayload::Error` with schema hash and error message.

> `crates/groove/src/query_manager/manager.rs:2110-2122` — `continue` on compilation failure with TODO comment

## Cleanup on Client Disconnect

**Priority: Medium**

When a client disconnects, SyncManager cleans up `clients`, `commit_interest`, and `query_origin` — but QueryManager does NOT clean up `server_subscriptions`. Need `QueryManager::remove_client()` that filters out entries matching the disconnected client_id.

> `crates/groove/src/sync_manager.rs:498-510` (SyncManager cleanup — works)
> `crates/groove/src/query_manager/manager.rs` (server_subscriptions — no cleanup)

## Reconnection Re-subscription

**Priority: Medium**

When reconnect or dynamic server add/remove is implemented, client should re-send all active subscriptions to newly connected servers.

## Test Coverage Gaps

**Priority: Low**

- No tests for query compilation failure scenarios
- No tests for scope shrinking (deletes/updates that remove matches from result set)
- No tests for client disconnect cleanup
- No tests for complex join queries with contributing IDs tracking
