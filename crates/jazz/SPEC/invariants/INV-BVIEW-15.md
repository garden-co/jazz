# INV-BVIEW-15

- Status: now
- Coverage: ✓

## Invariant

Effective rows MUST distinguish the requested head branch key from the physical branch key that supplied each selected layer; ordinary branch columns project to head values while hidden provenance retains supplying branch keys.

## Enforced by (tests)

`jazz::tests::branch_views::db_exact_mutations_and_branch_view_reads_compose_head_over_base`; `jazz::node::tests::harness::branch_view_selects_head_then_base_and_keeps_unbranched_tables_shared`

## Implementation

`node/query_eval/read_sources.rs::branch_view_storage_source_fields`; `node/query_eval/materialization.rs`
