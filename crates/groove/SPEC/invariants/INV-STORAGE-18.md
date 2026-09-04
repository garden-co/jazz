# INV-STORAGE-18

- Status: now
- Coverage: ✓

## Invariant

Base table writes MUST be staged before the tick and flushed together with durable tick writes only after the tick succeeds.

## Enforced by (tests)

`groove::db::tests::final_atomic_commit_failure_leaves_base_rows_unwritten_and_poisons_database`; `groove::db::tests::atomic_commit_path_supports_indexed_join_and_recursive_workloads`

## Implementation

`db/mod.rs::Database::commit_pending_writes`; `ivm/runtime/mod.rs::IvmRuntime::tick_staged`
