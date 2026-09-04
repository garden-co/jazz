# INV-HIST-18

- Status: now
- Coverage: ✓

## Invariant

A version parent MUST identify an exact prior version of the same physical table, branch key, row, and content/deletion layer; it MUST NOT encode a cross-row transaction dependency or a dependency between the content and deletion layers.

## Enforced by (tests)

`jazz::node::tests::harness::known_parent_must_match_exact_row_coordinate_and_layer`; `jazz::node::tests::harness::parent_validation_scopes_same_table_transactions_to_the_physical_row`; `jazz::db::tests::mutations::delete_starts_a_deletion_history_without_parenting_content`

## Implementation

`jazz/src/node/ingest/validation.rs::NodeState::validate_known_parent_coordinate`; `jazz/src/db/mutations.rs::Db::delete`
