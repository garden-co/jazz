# INV-LARGE-4

- Status: now
- Coverage: ✓

## Invariant

Large-value tree construction, metrics, validation, edit interpretation, and consolidation MUST be deterministic, bounded, and integrity checked. Every branch edge MUST contribute a positive byte length; only a root leaf MAY represent an empty logical value, and historical branches violating that rule MUST fail closed.

## Enforced by (tests)

`groove::large_values::tests::{construction_is_deterministic_and_text_metrics_are_exact,localized_single_edit_matches_fresh_tree_across_seeded_ranges,multi_edit_continuation_keeps_completed_local_splices_across_suspension,object_hash_authenticates_locator_bearing_branch_bytes,reconstruction_reuses_exact_locators_for_unchanged_nodes,zero_byte_branch_child_is_rejected_before_descendant_discovery_or_materialization,canonical_empty_value_is_one_empty_root_leaf,full_deletion_consolidates_to_one_empty_root_leaf,complete_suffix_deletion_collapses_singleton_root_to_fresh_leaf,shared_dag_physical_walks_deduplicate_and_logical_materialization_preserves_occurrences}`

## Implementation

`groove/src/large_values.rs`
