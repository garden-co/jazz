# INV-LARGE-13

- Status: now
- Coverage: ✓

## Invariant

Accepted resident removal of a staging receipt MUST veto eviction before durability. While a resident publication owns lifecycle serialization, eviction of any receipt MUST defer without waiting on that publication's own guard or changing durable state; retry after the durable frontier advances performs the maintenance. Physical chunk deletion MUST be authorised by durable metadata and MUST be vetoed by resident references, so unpublished activation protects bytes while unpublished deactivation cannot authorise deletion. A stale queue entry MUST NOT elevate resident deactivation into authority, and failed eviction or reclamation metadata persistence MUST leave retryable work.

## Enforced by (tests)

`groove::db::tests::{resident_large_value_acceptance_blocks_stale_eviction_and_reclamation,cross_receipt_eviction_defers_until_resident_publication_is_durable,reclamation_uses_durable_zero_and_resident_references_as_a_veto,reclaim_without_a_staged_override_fetches_node_metadata_once,large_value_eviction_and_reclamation_retry_failed_metadata_persistence}`

## Implementation

`groove/src/db/facade.rs::{evict_staged_large_value,reclaim_orphaned_large_value_chunks}`
