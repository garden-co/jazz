# INV-LARGE-9

- Status: now
- Coverage: ✓

## Invariant

Physical-record mutation and descriptor-reference deltas MUST be one atomic Groove transaction; persisted root/node counts and resumable reclamation MUST preserve shared and retained chunks without a Jazz history walk.

## Enforced by (tests)

`groove::db::tests::{staged_large_value_is_consumed_atomically_with_its_referencing_row,incomplete_push_upload_is_restart_persistent_and_reclaimable,shared_durable_root_is_reclaimed_only_after_its_last_physical_record,orphan_reclamation_defers_for_active_chunk_requests_and_leases,repeated_child_dag_finalizes_once_per_node_and_reclaims_without_leaks,shared_child_dag_counts_distinct_parent_edges_and_reclaims_once,resolver_installed_shared_dag_recursively_activates_and_reclaims_descendants}`

## Implementation

`groove/src/db/commit.rs`; `groove/src/db/facade.rs::{evict_staged_large_value,reclaim_orphaned_large_value_chunks}`
