# Intentional index staleness fallback

## What

Update paths tolerate stale indexing when old row content is missing, making query correctness probabilistic under some sync histories.

## Where

`crates/jazz-tools/src/query_manager/manager.rs`

## Steps to reproduce

N/A — triggers under specific sync histories where old row content is unavailable during index updates.

## Expected

Explicit reindex/recovery workflow when old row content is missing.

## Actual

Stale index entries are silently tolerated.

## Priority

high

## Notes

Direction: replace with explicit reindex/recovery workflow.
