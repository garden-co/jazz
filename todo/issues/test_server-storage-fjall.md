# Server storage (fjall) integration tests

## What

Missing integration tests for the fjall storage backend in server context.

## Where

`crates/jazz-tools/src/storage/fjall.rs`

## Steps to reproduce

N/A — missing test coverage.

## Expected

Tests covering: read/write round-trips, key encoding correctness, persistence across restarts, concurrent access patterns, large dataset behavior.

## Actual

No dedicated integration tests for fjall as a server storage backend.

## Priority

medium

## Notes

fjall doesn't use the shared key_codec module yet (see storage-backend-key-layout-duplication issue) — tests should verify key encoding matches expectations regardless.
