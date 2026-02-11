# Query Subscription Disconnect Cleanup — TODO (MVP)

When a client disconnects, SyncManager cleans up `clients`, `commit_interest`, and `query_origin` — but QueryManager does NOT clean up `server_subscriptions`. Need `QueryManager::remove_client()` that filters out entries matching the disconnected client_id.

Related: `client_state_cleanup.md`

> `crates/groove/src/sync_manager.rs:498-510` (SyncManager cleanup — works)
> `crates/groove/src/query_manager/manager.rs` (server_subscriptions — no cleanup)
