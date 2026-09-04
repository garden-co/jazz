# INV-QUERY-20

- Status: now
- Coverage: ✓

## Invariant

`ArgMaxByOp` and `ArgMinByOp` MUST accept arbitrary upstream graph inputs. Base-table inputs MUST have primary-key columns exactly `group_cols + order_cols`; every plan shape MUST compare only that declared key under the operator direction, then encoded full-record bytes ascending as the deterministic final tie-breaker. Full-record bytes MUST independently key multiplicity so distinct records tied on the comparison key remain independently retractable; arbitrary payload field order MUST NOT be appended to the declared comparison key.

## Enforced by (tests)

`groove::db::tests::arg_max_by_tracks_union_of_filtered_sources`; `groove::db::tests::arg_max_by_tracks_join_filter_input`; `groove::db::tests::arg_by_rejects_bad_primary_keys`; `groove::db::tests::arg_max_by_projection_reorder_preserves_tied_winner_and_retraction`; `groove::db::tests::arg_max_by_direct_table_and_noop_filter_publish_same_payload_replacement`; `groove::db::tests::arg_by_snapshot_hydration_tie_breaker_is_independent_of_reversed_input_order`; `groove::db::tests::arg_min_by_reordered_projection_preserves_declared_order_on_retraction`; `groove::db::tests::arg_min_by_hydrates_initial_snapshot_winner`

## Implementation

`groove/src/ivm/runtime/compilation.rs::arg_by_comparison_field_indices`; `groove/src/ivm/runtime/windows.rs::arg_by_winner_from_records`; `groove/src/ivm/runtime/windows.rs::arg_by_winner_before_from_deltas`
