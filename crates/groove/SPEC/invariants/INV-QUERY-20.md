# INV-QUERY-20

- Status: now
- Coverage: ✓

## Invariant

`ArgMaxByOp` and `ArgMinByOp` MUST accept arbitrary upstream graph inputs. Base-table inputs MUST have primary-key columns exactly `group_cols + order_cols`; non-table inputs MUST use `group_cols + order_cols` as the comparison key.

## Enforced by (tests)

`groove::db::tests::arg_max_by_tracks_union_of_filtered_sources`; `groove::db::tests::arg_max_by_tracks_join_filter_input`; `groove::db::tests::arg_max_by_rejects_unsupported_inputs_and_bad_primary_keys`; `groove::db::tests::arg_min_by_hydrates_initial_snapshot_winner`

## Implementation

`groove/src/ivm/runtime/mod.rs::IvmRuntime::add_dedup_graph`; `groove/src/ivm/runtime/mod.rs::validate_arg_max_by_primary_key_indices`; `groove/src/ivm/runtime/mod.rs::TickEvaluator::update_arg_min_by`
