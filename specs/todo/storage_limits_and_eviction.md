# Storage Limits & Eviction — TODO

Design for bounded storage on clients and cache tiers, with eviction of cold data.

## Overview

Client devices (browsers, phones) and edge servers have limited storage. The system must:

- Enforce configurable storage budgets per tier
- Evict cold objects and their associated index entries when limits are reached
- Re-fetch evicted data on demand from upstream (the upstream server always has a covering index)
- Handle eviction gracefully without corrupting reactive query state

This was identified as critical to design early — retrofitting eviction onto indices is much harder than building it in from the start.

## Eviction Strategy

- Objects are the unit of eviction (not individual columns or index entries)
- When an object is evicted, its index entries across all indices are also removed
- Whole index pages can be discarded if all their referenced objects are evicted
- LRU or access-frequency based eviction policy
- Queries against evicted data trigger upstream fetch (lazy reload)

## Per-Tier Behavior

- **Browser (OPFS)**: tight budget, aggressive eviction, rely on worker → server for cold data
- **Edge server**: larger budget, cache hot data per region, evict per-app
- **Core/shard servers**: authoritative storage, no eviction (or archival only)

## Open Questions

- How does eviction interact with reactive query subscriptions? (If a subscribed row is evicted, re-fetch immediately?)
- Budget units: bytes? row count? per-table limits?
- Can developers pin certain tables/queries as "never evict"?
- OPFS quota: how much can we reliably use before the browser pushes back?
- Eviction ordering: per-table LRU, global LRU, or query-driven (keep data for active subscriptions)?
