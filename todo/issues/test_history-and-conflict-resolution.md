# History and conflict resolution integration tests

## What

Missing integration tests for history tracking and conflict resolution across concurrent writers.

## Where

RuntimeCore / SyncManager integration tests

## Steps to reproduce

N/A — missing test coverage.

## Expected

Tests covering: concurrent writes from multiple clients producing conflicts, conflict resolution producing deterministic winners, history traversal after merges, branching and merging flows.

## Actual

No dedicated integration tests for these scenarios.

## Priority

high

## Notes

Should use realistic fixtures with human actor names (alice, bob) and include ASCII flow sketches for multi-client scenarios.
