# Schema traveling integration tests

## What

Missing integration tests for schema evolution and lens-based data migration across syncing clients.

## Where

RuntimeCore / SchemaManager / SyncManager integration tests

## Steps to reproduce

N/A — missing test coverage.

## Expected

Tests covering: client A writes under schema v1, schema evolves to v2 with lenses, client B reads under v2 and sees transformed data, bidirectional lens application during sync, catalogue updates propagating schema changes.

## Actual

No end-to-end tests exercising schema evolution through sync.

## Priority

high

## Notes

Related to existing issue: cross-schema-evolution-e2e-test. This is broader — covers lens sync and catalogue propagation, not just local evolution.
