# Intentional index staleness fallback

## What

Update paths tolerate stale indexing when old row content is missing, making query correctness probabilistic under some sync histories.

## Priority

medium

## Notes

- Where: `crates/jazz-tools/src/query_manager/manager.rs`
- This triggers under specific sync histories where old row content is unavailable during index updates.
- Expected: an explicit reindex or recovery workflow when old row content is missing.
- Actual: stale index entries are silently tolerated.
- Direction: replace the fallback with explicit reindex or recovery handling.
