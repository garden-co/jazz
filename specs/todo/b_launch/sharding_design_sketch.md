# Sharding Design Sketch — TODO

Full design for distributing data across storage shards. Nothing here is implemented yet.

> Status quo: no sharding implementation exists yet.

## Architecture: Smart Shards with Local Indices

**Priority: Future (when single-node hits scaling limits)**

Core servers become stateless query coordinators. Each shard owns objects + indices for its data subset. Core fans out queries to all shards; empty responses are O(1).

```
Core Servers (stateless, auto-scaling)
        ↕ fan-out queries / receive deltas
Shard-1   Shard-2   Shard-3   Shard-4
(objects) (objects) (objects) (objects)
(indices) (indices) (indices) (indices)
```

## Adaptive Sharding: Slot-Based Table Growth

Each `(app, table)` starts on one "home" shard. When full, core updates shard map and routes new writes to next slot. Properties: small tables stay on 1 shard, large tables spread proportionally, no object movement.

### Rendezvous Hashing

Deterministic shard assignment via highest-random-weight hashing. Adding/removing a shard only moves ~1/N of keys. No central coordinator needed.

### Home Shard Coordination

Slot 0's shard acts as coordinator: maintains the shard map for `(app, table)`, notifies cores of slot transitions.

## Replication: Top-2 Rendezvous

Each slot gets primary + replica (top-2 scoring shards). Write path: primary write → async replica. Read path: primary preferred, fallback to replica.

### Failure Handling

- Primary fails → replica serves reads/writes, primary syncs on recovery
- Replica fails → primary continues, degraded mode
- Both fail → slot unavailable, partial results

## Subscription Model: Two-Level

### Gap vs. Current Implementation

Current system uses object-level `QueryScope` subscriptions. Sharding needs table-level "interests":

- **Level 1** (all shards): "Core C is interested in table T" — just a `(core_id, table_id)` pair
- **Level 2** (shards with data): Full query predicates/projections

This requires refactoring from current object-level scoping to table-level awareness.

> Current: `crates/groove/src/sync_manager.rs:124-129` (QueryScope — object-level)

## Shard Migration: Epoch-Based State Machine

When cluster membership changes (add/remove shard):

1. **Copying** — new shard receives data from old via replication stream
2. **DualWrite** — cores write to both old and new shard
3. **Draining** — old shard rejects writes (redirects), still serves reads
4. **Complete** — old shard deletes data after observing silence

Safety: old shard only deletes after T1 (no old-epoch writes, ~5min) + T2 (no requests at all, ~1hr). Late requests get redirected.

## Open Questions

1. "Full" threshold — row count? Storage bytes? Both?
2. Hash collision — same shard for consecutive slots?
3. Query pushdown — how much query logic in shards vs cores?
4. Cross-slot queries — ordering guarantees when merging across slot boundaries?
5. Home shard failure — replica must take over coordinator role
6. Subscription consistency during migration — dual-subscribe during DualWrite phase?
