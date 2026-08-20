# Branch-view test porting ledger

This temporary ledger preserves the behavioral intent of tests that exercise
the retired core-owned branch model. A row leaves this file only when its named
replacement is active, or when the retired requirement has an explicit receipt.

| Old test | Old guarantee | New invariant | Disposition | Replacement test |
| --- | --- | --- | --- | --- |
| `branch_read_is_base_snapshot_plus_overlay_writes` | frozen base plus overriding writes | `INV-BVIEW-9`, `INV-BVIEW-11` | port | `branch_view_frozen_base_reduces_layers_head_first` |
| `branch_target_commit_unit_is_visible_after_global_acceptance` | branch-local fate/publication | `INV-BVIEW-16` | port | `cross_branch_key_commit_publishes_atomically_after_fate` |
| `branch_read_filter_shape_uses_shared_branch_source_lowering` | mask before filter | `INV-BVIEW-9` | port | `branch_view_masks_base_before_filter` |
| `branch_read_join_uses_shared_branch_sources` | joins use effective sources | `INV-BVIEW-8`, `INV-BVIEW-9` | port | `branch_view_join_projects_table_dimension_subsets` |
| `branch_read_reachable_uses_shared_branch_sources` | reachability uses effective sources | `INV-BVIEW-9`, `INV-BVIEW-13` | port | `branch_view_reachability_resolves_effective_references` |
| `branches_do_not_observe_sibling_overlays_and_recover_metadata` | sibling isolation and recovery | `INV-BVIEW-5` | replace | `branch_keys_recover_independent_winners` |
| `branch_exclusive_returns_v1_error` | exclusive branch API is unsupported | retired API | retire | removal of branch-specific exclusive API |
| `branch_creation_does_not_scale_with_base_row_count` | branch creation is metadata-only | userland lifecycle | retire | application branch-row benchmark, outside core |
| `branch_read_requires_branch_row_read_then_branch_local_row_policy` | branch-row read gate | `INV-BVIEW-18` | replace | `branch_dimension_reference_policy_controls_view_read` |
| `branch_write_requires_branch_row_write_then_branch_local_write_policy` | branch-row write gate | `INV-BVIEW-18` | replace | `branch_dimension_reference_policy_controls_exact_write` |
| `branch_creation_persists_no_overlay_partition_until_first_write` | sparse branch storage | `INV-BVIEW-3` | replace | `unused_branch_key_persists_no_history` |
| `branch_overlay_partition_creation_rebuilds_live_database_without_storage_reopen` | new coordinate becomes queryable live | `INV-BVIEW-5` | port | `first_branch_key_write_updates_live_current_state` |
| `branch_overlay_spans_schema_renames_and_merge_back_after_restart` | rename-stable coordinate and merge | `INV-BVIEW-1`, `INV-MERGE-2` | replace | `branch_dimension_binding_survives_column_rename_and_reopen` |
| `branch_writes_reject_unknown_and_closed_branches` | core validates branch existence/lifecycle | userland lifecycle | retire | ordinary reference policy covers missing/closed app rows |
| `discard_branch_closes_branch_for_writes_and_merge_back` | core discard transition | userland lifecycle | retire | application policy test, outside core branch mechanism |
| `merge_back_branch_emits_ordinary_target_transaction_and_leaves_branch_open` | merge is ordinary transaction | `INV-MERGE-1` | port | `contribution_merge_emits_ordinary_target_transaction` |
| `merge_back_parents_every_concurrent_target_head` | target-only causal parents | `INV-MERGE-4` | port | `contribution_merge_parents_current_target_heads` |
| `merge_back_accumulates_authored_columns_across_successive_branch_patches` | field-grained contributions | `INV-MERGE-2` | port | `contribution_merge_accumulates_authored_components` |
| `legacy_target_version_without_authored_columns_contributes_present_cells` | legacy provenance fallback | no persisted users | retire | no legacy branch encoding compatibility |
| `merge_back_deduplicates_shared_transaction_parent_edges` | parent edge deduplication | `INV-MERGE-4` | port | `contribution_merge_deduplicates_target_parent_edges` |
| `branch_target_is_canonical_atomic_transaction_state_across_reopen` | branch transaction recovery | `INV-BVIEW-16` | port | `cross_branch_key_commit_recovers_atomically` |
| `root_branch_transitive_merge_expands_provenance_without_echo` | recursive no-echo merge | `INV-MERGE-3` | port | `contribution_merge_cycle_does_not_echo` |
| `ordinary_commit_unit_routes_to_branch_target_without_touching_root` | per-version coordinate routing | `INV-BVIEW-3`, `INV-BVIEW-5` | replace | `commit_versions_route_by_branch_key` |
| `invalid_branch_targets_do_not_persist_poison_partitions` | invalid target is atomic | `INV-BVIEW-16` | replace | `malformed_branch_key_rejects_commit_without_residue` |
| `ordinary_branch_target_ingest_applies_target_authorization` | target authorization | `INV-BVIEW-18` | replace | `branch_key_write_uses_ordinary_dimension_policy` |
| `merge_back_fails_whole_calculation_when_source_row_is_not_readable` | source-read authorization | `INV-MERGE-5` | port | `contribution_merge_fails_when_source_is_unreadable` |
| `identity_merge_back_of_public_deleted_row_preserves_initiator` | initiator provenance | `INV-MERGE-2` | port | `contribution_merge_preserves_initiator` |
| `deleted_merge_witness_uses_inherited_select_not_update_permission` | deletion contribution authorization | `INV-MERGE-5` | replace | `contribution_merge_authorizes_source_delete_as_read` |
| `merge_back_fails_closed_for_strategy_without_contribution_capabilities` | unsupported strategy fails | `INV-MERGE-5` | port | `contribution_merge_rejects_unsupported_strategy` |
| `merge_back_seeded_net_effect_oracle_matches_direct_parent_rows_and_deletions` | net-effect oracle | `INV-MERGE-1..6` | port | `contribution_merge_seeded_net_effect_oracle` |
| `merge_back_seeded_strict_oracle_matches_direct_parent_versions` | strict version oracle | `INV-MERGE-1..6` | port | `contribution_merge_seeded_strict_oracle` |
| `merge_back_seeded_restore_oracle_matches_direct_parent_rows_and_deletions` | restore net-effect oracle | `INV-MERGE-1..6` | port | `contribution_merge_seeded_restore_net_effect_oracle` |
| `merge_back_seeded_restore_oracle_matches_direct_parent_versions` | restore strict oracle | `INV-MERGE-1..6` | port | `contribution_merge_seeded_restore_strict_oracle` |
| `offline_branch_creation_and_commit_sync_metadata_before_data` | metadata precedes target data | no core metadata | retire | branch keys are self-routing version metadata |
| `fixed_schema_db_branch_and_bootstrap_writes_retain_authored_schema` | authored schema survives branch sync | `INV-BVIEW-3` | replace | `branch_key_commit_retains_authored_schema` |
| `session_branch_metadata_rejects_creator_mismatch` | creator-owned branch metadata | userland lifecycle | retire | ordinary app-row write policy |
| `session_branch_metadata_rejects_malformed_initial_shapes` | validates core metadata shape | no core metadata | retire | schema validation covers branch dimensions |
| `empty_branch_metadata_retries_after_unacked_reopen` | metadata outbox retry | no core metadata | retire | ordinary row synchronization |
| `acknowledged_open_accepts_remote_discard_and_recovers_it` | discard metadata recovery | userland lifecycle | retire | ordinary app-row synchronization |
| `edge_durably_relays_empty_branch_creation_and_discard_after_reopen` | metadata relay durability | userland lifecycle | retire | ordinary row relay coverage |
| `session_branch_data_parks_until_authenticated_metadata_arrives` | data waits for metadata | no core metadata | retire | policy evidence uses ordinary closure repair |
| `session_branch_metadata_parks_until_snapshot_base_arrives` | metadata waits for base | app resolves base | retire | application supplies concrete base source |
| `locally_created_branch_and_commit_survive_rocks_reopen` | branch storage recovery | `INV-BVIEW-3`, `INV-BVIEW-5` | replace | `branch_key_history_and_current_survive_reopen` |
| `trusted_branch_snapshot_round_trips_without_receiver_reauthoring` | frozen base round-trip | `INV-BVIEW-11` | port | `frozen_branch_source_round_trips_canonical_cut` |
| `trusted_backend_replays_branch_metadata_over_transport` | metadata replay | no core metadata | retire | ordinary application-row replay |
| `trusted_backend_discards_branch_metadata_once_and_recovers_it` | discard idempotency | userland lifecycle | retire | ordinary application-row idempotency |
| `subscriber_connection_serves_branch_subscription_with_known_state_and_unsubscribe` | branch subscription identity | `INV-BVIEW-12` | port | `branch_view_subscription_known_state_and_unsubscribe` |
| `subscriber_connection_serves_branch_subscription_alongside_root_subscription` | shared and branched subscription coexist | `INV-BVIEW-7`, `INV-BVIEW-12` | replace | `branch_view_and_shared_subscription_coexist` |

## Active tests that will be ported in-place

These remain compiled until their production dependency is removed:

| Old test | New invariant | Disposition | Replacement test |
| --- | --- | --- | --- |
| `branch_read_view_relation_snapshot_uses_query_engine_relation_edges` | `INV-BVIEW-9` | port | same scenario using `BranchView` |
| `single_branch_read_view_uses_query_engine_branch_source_for_one_shot_reads` | `INV-BVIEW-9` | port | `branch_view_one_shot_uses_effective_source` |
| `branch_commit_rejects_unadmitted_authored_schema_without_persistence` | `INV-BVIEW-16` | replace | `branch_key_commit_rejects_unadmitted_schema_atomically` |
| `dynamic_edge_bootstrap_rejects_branch_creation_without_residue` | no core lifecycle | retire | remove branch-creation case from bootstrap surface |
| `fallback_replay_of_preselection_branch_view_cannot_settle` | legacy metadata parking | retire | branch views carry no metadata admission stream |
| `parked_branch_opening_is_not_cleared_by_unrelated_applied_view` | legacy metadata parking | retire | branch views carry no metadata admission stream |
| wire fixtures `branch_metadata_root_open` / `fetch_branch_metadata` | legacy metadata protocol | retire | branch selectors travel only in ordinary read options |
| `catalogue_arrival_drains_branch_relay_into_branch_partition` | `INV-BVIEW-3` | replace | `catalogue_arrival_drains_branch_keyed_history` |
| `parked_branch_ingress_role_keeps_authority_precedence_in_both_orders` | `INV-BVIEW-16` | replace | `parked_branch_key_ingress_preserves_authority_precedence` |
| `lowered_write_policy_covers_branch_metadata_gate` | no core metadata | retire | ordinary reference-policy lowering coverage |
| `lowered_write_policy_covers_branch_overlay_table_operations` | `INV-BVIEW-18` | replace | `lowered_write_policy_covers_branch_key_operations` |
| `client_local_branch_subscription_survives_sparse_first_write_delete_and_restore` | `INV-BVIEW-5`, `INV-BVIEW-12` | port | `branch_view_subscription_tracks_first_write_delete_restore` |
| `denied_branch_subscription_does_not_allocate_sparse_source` | `INV-BVIEW-18` | replace | `denied_branch_view_allocates_no_maintained_source` |
| `branch_subscription_reconnects_and_re_settles_after_a_fresh_view_receipt` | `INV-BVIEW-12` | port | `branch_view_subscription_reconnects_and_resettles` |
| `branch_one_shot_waits_for_metadata_and_keeps_sibling_result_identity` | `INV-BVIEW-12` | replace | `branch_view_one_shot_keeps_sibling_result_identity` |
| `empty_branch_subscription_reconnects_with_a_settlement_only_refresh` | `INV-BVIEW-12` | replace | `empty_branch_view_reconnects_with_settlement_only_refresh` |
| `dropping_a_branch_subscription_releases_its_upstream_coverage` | `INV-BVIEW-12` | port | `dropping_branch_view_releases_upstream_coverage` |
| `branch_program_tier_filter_preserves_claim_policy_fields` | `INV-BVIEW-18` | replace | `branch_view_tier_filter_preserves_claim_policy_fields` |
| `prepared_relation_terminal_keeps_branch_discriminator_in_public_payload` | `INV-BVIEW-15` | replace | `prepared_relation_terminal_keeps_branch_source_provenance` |
| `lowered_groove_graph_differs_for_distinct_read_views` | `INV-BVIEW-12` | port | same scenario using distinct `BranchView` values |
| `branch_program_maintained_view_provides_branch_deletion_witness_source` | `INV-BVIEW-5` | port | `branch_view_maintains_deletion_witness_source` |
| `branch_program_maintained_view_tracks_local_overlay_replacement` | `INV-BVIEW-9` | port | `branch_view_maintains_head_replacement` |
| `branch_program_maintained_view_survives_first_overlay_partition_write` | `INV-BVIEW-5` | replace | `branch_view_maintains_first_branch_key_write` |
| `branch_program_maintained_views_isolate_sibling_first_writes` | `INV-BVIEW-12` | port | `branch_views_isolate_sibling_first_writes` |
| `branch_program_maintained_view_settles_overlay_fates_at_every_tier` | `INV-BVIEW-12` | port | `branch_view_settles_fates_at_every_tier` |
| `branch_program_maintained_view_retracts_rejected_pending_overlay_versions` | `INV-BVIEW-12` | port | `branch_view_retracts_rejected_pending_head_version` |
| `branch_relation_target_projects_old_renamed_witness` | `INV-BVIEW-1`, `INV-BVIEW-15` | replace | `branch_view_relation_projects_renamed_witness` |
| `renamed_branch_terminal_resolves_root_target_from_emitted_read_table` | `INV-BVIEW-1`, `INV-BVIEW-15` | replace | `renamed_branch_view_terminal_resolves_emitted_table` |
| `branch_relation_array_uses_frozen_root_and_overlay_target` | `INV-BVIEW-11`, `INV-BVIEW-15` | replace | `branch_view_relation_array_uses_frozen_base_and_head_target` |
| `read_view_key_canonicalizes_merged_branch_order` | multiple bases deferred | retire | multiple-base canonicalization is a later capability |
