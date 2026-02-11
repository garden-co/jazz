# Sync Manager — TODO

Remaining work items and known limitations.

> Status quo: [specs/status-quo/sync_manager.md](../../status-quo/sync_manager.md)

## Phasing

- **MVP**: Query forwarding tracking, query settlement 3-tier verification
- **Launch**: Scope-based contraction
- **Superseded**: Blob permission checking (replaced by `binary_columns_and_fk_refs.md`)

## MVP: Query Forwarding Tracking

**Status: NOT implemented**

The spec described `ServerState.forwarded_queries` for tracking which queries are forwarded to each server. The implementation sends queries upstream but does not track per-server forwarded query state.

`query_origin: HashMap<QueryId, HashSet<ClientId>>` tracks which clients originated a query, but not which servers received it. This means:

- No deduplication of forwarded queries
- No cleanup of server-side subscriptions when queries are removed

> `crates/groove/src/sync_manager.rs:390` (query_origin), `675-696` (send methods)

## MVP: Query Settlement 3-Tier Verification

**Status: 2-tier works, 3-tier untested**

The infrastructure exists (PersistenceAck + QuerySettled with tier). 2-tier tests pass (direct, tier-constraint, multiple-servers, one-shot). Missing: a 3-tier test where QuerySettled cascades through an intermediate node back to the originating client.

> `crates/groove/src/schema_manager/integration_tests.rs:2651-3045` (2-tier tests)

## Launch: Scope-Based Contraction

When a query is removed and an object falls out of scope, the client keeps what it received ("no unsend"). There's no mechanism to inform the client that certain objects are no longer being tracked. Useful for client-side GC — related to `client_state_cleanup.md`.

> `crates/groove/src/sync_manager.rs:1415` (query removal)
