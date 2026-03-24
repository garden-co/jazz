# Lens transform failures degrade silently

## What

Failed lens transforms fall back to original data and continue, silently propagating schema mismatches.

## Where

`crates/jazz-tools/src/query_manager/manager.rs`

## Steps to reproduce

N/A — requires a lens transform failure during row processing.

## Expected

Fail closed for that row/subscription and surface deterministic errors.

## Actual

Falls back to original (potentially mismatched) data silently.

## Priority

medium

## Notes

Direction: fail closed and surface deterministic errors.
