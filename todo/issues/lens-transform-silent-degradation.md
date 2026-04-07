# Lens transform failures degrade silently

## What

Failed lens transforms fall back to original data and continue, silently propagating schema mismatches.

## Priority

medium

## Notes

- Where: `crates/jazz-tools/src/query_manager/manager.rs`
- This requires a lens transform failure during row processing.
- Expected: fail closed for the affected row or subscription and surface deterministic errors.
- Actual: processing falls back to the original, potentially mismatched data silently.
- Direction: fail closed and surface deterministic errors.
