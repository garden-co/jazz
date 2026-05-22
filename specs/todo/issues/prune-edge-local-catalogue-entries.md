# Prune Edge Local Catalogue Entries

## What

When an edge reconnects with a stale catalogue hash, core replay currently upserts core catalogue entries but does not remove same-app catalogue entries that exist only on the edge. Add an authoritative reconnect path that prunes edge-local catalogue entries absent from core, so edge catalogue state converges exactly to core.

## Priority

high

## Notes

- Core remains the source of truth for catalogue state.
- A full pull is acceptable, but it must replace the edge catalogue set, not only fill missing entries.
- Cover storage pruning and in-memory SchemaManager state so pruned entries cannot keep being used after reconnect.
