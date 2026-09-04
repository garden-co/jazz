# INV-STORAGE-23

- Status: now
- Coverage: ✓

## Invariant

Durable unique-index ownership MUST be resolved from the consolidated final deltas of one storage-atomic batch. A different record may replace the durable owner only when that batch fully retracts the old owner and leaves at most one positive owner; competing positive owners MUST be rejected.

## Enforced by (tests)

`groove::db::tests::{durable_unique_indices_reject_positive_delta_for_existing_different_record,durable_unique_indices_allow_atomic_replacement_within_one_batch}`

## Implementation

`ivm/runtime/persist.rs::{apply_persist_delta,resolve_unique_owner}`
