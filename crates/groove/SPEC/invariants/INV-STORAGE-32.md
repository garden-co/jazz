# INV-STORAGE-32

- Status: now
- Coverage: ✓

## Invariant

An atomic batch acknowledgement MUST distinguish committed, definitely-uncommitted, and possibly-committed outcomes. Only a definitely-uncommitted result permits retry or runtime rollback. Cancellation before an ordered write starts may retry; cancellation after submission starts and an explicit possibly-committed result MUST poison the resident database before any retry or rollback can occur. Cancelling an in-flight publication MUST wake every later ordered waiter with the terminal order failure. The poison is instance-local: reopening observes the prior durable state, does not retry the abandoned resident publication, and permits fresh writes.

## Enforced by (tests)

`groove::storage::tests::{write_many_outcome_default_is_conservative_after_an_error,memory_write_many_outcome_proves_prevalidation_errors_uncommitted,layout_storage_preserves_backend_commit_classification}`; `async_hydration_session::{cancelled_started_persistence_poisoned_database_cannot_retry_or_roll_back,cancelled_started_persistence_wakes_queued_publication_with_order_failure,possibly_committed_receipt_poisoned_database_before_settlement}`; `groove::db::tests::suspended_resident_chunk_install_joins_assigned_publication`

## Implementation

`groove/src/storage/mod.rs::{OrderedKvStorage::write_many_outcome,WriteManyOutcome,LayoutStorage::write_many_outcome}`; `groove/src/storage/memory.rs::MemoryStorage::write_many_outcome`; `groove/src/db/mod.rs::{AppliedBatch::persist,PersistenceAttempt}`
