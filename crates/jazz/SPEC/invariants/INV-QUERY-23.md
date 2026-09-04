# INV-QUERY-23

- Status: now
- Coverage: ✓

## Invariant

A flat joined output occurrence MUST be identified by its ordered contributing source-row ids, not by its root row id; maintained delivery MUST address additions, removals, and replacements by that composite occurrence identity.

## Enforced by (tests)

`crates/jazz/tests/output_occurrence_id.rs::flat_join_output_occurrence_identity_addresses_additions_removals_and_replacements`

## Implementation

`query.rs::FlatJoin`; `node/query_engine/lowering.rs::flat_join_occurrence_id_fields`; `node/query_eval.rs::NodeState::current_row_from_result_payload`; `db.rs::subscription_row_occurrence_id`
