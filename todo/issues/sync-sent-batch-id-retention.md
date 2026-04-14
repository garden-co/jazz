# Sync sent batch id retention

## What

Per-peer sync state retains every sent batch id in memory, which can grow with history size and may keep more tracking state than replay actually needs.

## Priority

medium

## Notes

- Where:
  - `crates/jazz-tools/src/sync_manager/types.rs`
  - `crates/jazz-tools/src/sync_manager/sync_logic.rs`
- The current sync state uses `HashSet<BatchId>` to remember which batch ids have already been sent to each peer.
- Initial sync and long-lived peers can accumulate large historical sets.
- Direction: revisit the retention policy and consider tracking only the active visible/replay window instead of all historical batch ids.
