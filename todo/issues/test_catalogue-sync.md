# Catalogue sync integration test

## What

Catalogue tests call `process_catalogue_update()` directly rather than pumping through SyncManager — missing a full end-to-end flow.

## Where

`crates/groove/src/schema_manager/integration_tests.rs`

## Steps to reproduce

N/A — missing test coverage, not a runtime bug.

## Expected

An integration test using `wire_up_sync()` / `pump_sync()` helpers that exercises the complete catalogue sync flow through SyncManager.

## Actual

Tests bypass SyncManager and call `process_catalogue_update()` directly.

## Priority

high
