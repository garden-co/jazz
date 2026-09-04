# INV-BVIEW-6

- Status: now
- Coverage: ✓

## Invariant

Secondary indices MUST be physically prefixed by the exact branch key; a composed branch view MUST apply head/base masking before consulting or publishing index results.

## Enforced by (tests)

`jazz::tests::branch_views::indexed_branch_view_masks_base_before_applying_the_predicate`

## Implementation

`schema.rs::TableSchema::global_current_storage_tables`; `node/query_eval/read_sources.rs::CurrentQuerySourceResolver::resolve_source`
