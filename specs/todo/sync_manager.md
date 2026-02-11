# Sync Manager — TODO

Remaining work items and known limitations.

> Status quo: [specs/status-quo/sync_manager.md](../status-quo/sync_manager.md)

## Query Forwarding Tracking

**Priority: Medium**

The spec described `ServerState.forwarded_queries` for tracking which queries are forwarded to each server. The implementation sends queries upstream (`send_query_subscription_to_servers()`, `send_query_unsubscription_to_servers()`) but does not track per-server forwarded query state.

`query_origin: HashMap<QueryId, HashSet<ClientId>>` tracks which clients originated a query, but not which servers received it.

> `crates/groove/src/sync_manager.rs:390` (query_origin), `675-696` (send methods)

## Query Settlement from Upstream

**Priority: Medium**

The infrastructure exists (`PersistenceAck` with tier, `QuerySettled` with tier) but the full end-to-end flow — "subscription callback only fires when local query graph is settled on data confirmed by all upstream tiers" — needs verification of completeness.

**Use cases:**
- Initial data load requiring authoritative server state
- Consistency-critical operations requiring server confirmation

> `crates/groove/src/sync_manager.rs:239-254` (PersistenceAck, QuerySettled)
> See also: [specs/status-quo/sync_manager.md](../status-quo/sync_manager.md) — PersistenceTier enum

## Scope-Based Contraction

**Priority: Low**

When a query is removed and an object falls out of scope, the spec notes "no unsend" — the client keeps what it already received. This is correct behavior, but there's no mechanism to inform the client that certain objects are no longer being tracked (useful for client-side GC).

> `crates/groove/src/sync_manager.rs:1415` (query removal)

## Blob Permission Checking

**Priority: Low**

Blob requests check read permission for the associated object. The implementation exists but may need review against the role-based model (User/Admin/Peer) vs the original scope-based model.

> `crates/groove/src/sync_manager.rs` (blob handling section)
