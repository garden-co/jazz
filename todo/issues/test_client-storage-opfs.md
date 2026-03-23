# Client storage (OPFS B-tree) integration tests

## What

Missing integration tests for the OPFS B-tree storage backend in client/browser context.

## Where

`crates/opfs-btree/src/db.rs`

## Steps to reproduce

N/A — missing test coverage.

## Expected

Tests covering: read/write round-trips, checkpoint and recovery, crash-recovery scenarios (interrupted writes), large dataset behavior, ordered index scans.

## Actual

No integration tests exercising the OPFS B-tree through the full storage interface in realistic scenarios.

## Priority

medium

## Notes

Related to opfs-btree-corruption-on-interrupted-write issue — crash-recovery tests would help prevent regressions.
