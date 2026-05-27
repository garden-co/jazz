# Prototype Test Traceability

## Appendix E: Prototype Test Traceability

This appendix maps the current `crates/mini-jazz-sqlite/tests/whole_system`
suite to the invariant groups in Appendix D. It is intentionally coarser than a
formal coverage database: one test may exercise several invariants, and one
invariant may require several tests before it is convincing.

Coverage labels:

- **covered**: at least one whole-system test directly exercises the invariant
- **partial**: tests exercise a narrow prototype shape, but not the full product
  invariant
- **untested**: no obvious prototype test covers it yet

### E.1 Coverage Summary By Invariant Group

| Group                      | Current status        | Notes                                                                                                                                                                                                                                              |
| -------------------------- | --------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| D.1 Identity               | covered for prototype | Public row ids, physical id locality, replica-local physical ids, and one user writing from multiple nodes are tested.                                                                                                                             |
| D.2 Transactions           | partial               | Sealing, explicit transactions, edge/global receipts, rejection, idempotence, non-unique global epochs, and monotonic fate are tested. Awaiting-dependencies semantics and audit-grade receipt history are not.                                    |
| D.3 History/projection     | partial               | Append-only ordinary deletes, rebuild, rejection repair, global ordering, remote pending constraints, and broad repair are tested. Hard delete/truncate and full merge/conflict projection semantics remain partial.                               |
| D.4 Visibility/snapshots   | partial               | Global epoch and pinned branch snapshot behavior is tested. Full vector snapshots are not implemented/tested.                                                                                                                                      |
| D.5 Branches               | covered for prototype | Branch overlay/base reads, branch tombstones, rejected overlay fallback, provenance sync, multi-source conflict candidates, and branch policy contexts are tested. Full product branch backing rows and merge commits are not.                     |
| D.6 Queries/observed facts | partial               | Equality, contains, IN, not-equal, null-present, selected system fields, ordered pages, absence facts, recursive query scopes, policy dependencies, query-scope repair, and predicate serialization are tested. Range and catalogue facts are not. |
| D.7 Sync                   | partial               | Query-scoped sync, table-vs-query scope, idempotence, public id hydration, reordered fate, scope contraction, active query refresh, and reconnect-shaped repair are tested. Compact reconnect summaries are not.                                   |
| D.8 Subscriptions          | partial               | Rerun-and-diff, policy dependency diffs, branch checkout diffs, pinned branch stability, pagination, and reconnect-shaped observed subscription recovery are tested. Tier gating and settled state are not.                                        |
| D.9 Policies               | covered for prototype | Read/write policies, ref-readable policies, recursive acyclic policies, cycle rejection, branch/pinned-base contexts, trusted bypass, and transitive policy read sets are tested. Full policy language and diagnostics are not.                    |
| D.10 Catalogue/lenses      | partial               | Narrow storage-name rename lenses, ref lenses, system prefix escaping, and index-only compatibility are tested. Catalogue revision graph, migrations directory semantics, inverse lenses, and copy-forward are not.                                |
| D.11 Authority validation  | partial               | Untrusted bundle rejection, atomic rejection, delete/update validation, branch-context validation, stale row/absence/policy/source read-set checks, exclusive same-row conflict, and repair are tested. Predicate/range validation is not.         |
| D.12 Conflicts             | partial               | Side APIs expose multi-base and pinned-base conflict candidates and policy-filtered candidates; conflict-aware row reads and resolution transactions are tested. Product metadata shape is not.                                                    |
| D.13 Errors/diagnostics    | partial               | Rejection codes, transaction info, rejection lists, rejection subscriptions, and detail enrichment are tested. Public error object shape, redaction, and diagnostics are not.                                                                      |
| D.14 Storage/lowering      | partial               | SQLite current/history tables, physical ids, system prefix escaping, integer-like enum behavior, and rebuild are exercised. `WITHOUT ROWID`, generated indexes, and query plans are not asserted.                                                  |
| D.15 Files/blobs           | untested              | No file/blob implementation in the prototype.                                                                                                                                                                                                      |
| D.16 Privacy/encryption    | untested              | No E2EE/encrypted-index implementation in the prototype.                                                                                                                                                                                           |
| D.17 Harness               | partial               | In-memory SQLite, file-backed durable nodes, multi-runtime local/edge/global tests, duplicate/reordered bundles, and durable reopen are tested. Drop/delay/reconnect protocol and deterministic clock APIs are not.                                |
| D.18 Tooling/admin         | untested              | Tooling, inspector, admin catalogue publication, and codegen workflows are not implemented in the prototype.                                                                                                                                       |

### E.2 Test Module Mapping

#### `storage_projection.rs`

- `memory_runtime_writes_through_sqlite_current_projection`: D.3, D.14, D.17
- `durable_nodes_survive_reopen_but_memory_nodes_start_empty`: D.17, D.3
- `rebuild_current_projection_from_history_matches_current_reads`: D.3, D.14
- `delete_is_history_not_removal`: D.3

#### `transactions.rs`

- `explicit_transaction_seals_multiple_mutations_atomically`: D.2
- `generic_transaction_seals_multiple_rows_atomically`: D.2
- `generic_transaction_can_seal_updates_atomically`: D.2, D.3
- `generic_update_records_previous_row_read_set`: D.9, D.11
- `generic_transaction_can_seal_delete_with_other_mutations`: D.2, D.3
- `exclusive_transaction_requires_global_epoch_and_commits_accepted`: D.2,
  D.11
- `exclusive_transaction_mode_survives_sync`: D.2, D.7
- `authority_acceptance_enriches_existing_transaction`: D.2
- `generic_transaction_delete_records_previous_row_read_set`: D.9, D.11
- `exclusive_transaction_rejects_same_row_conflict`: D.11, D.12
- `generic_transaction_delete_shadows_pinned_base_row`: D.5, D.3
- `global_epoch_can_accept_multiple_transactions`: D.2, D.3

#### `sync_fate.rs`

- `query_scoped_sync_converges_memory_and_durable_nodes`: D.7, D.17
- `rejected_transaction_remains_history_but_is_hidden_from_current`: D.2, D.3
- `rejected_fate_update_repairs_peer_current_projection`: D.2, D.3, D.7
- `durable_worker_reconciles_rejected_fate_after_restart`: D.17, D.2, D.3
- `rejecting_generic_transaction_repairs_schema_driven_projection`: D.3, D.7
- `table_scope_sync_exports_delete_so_peer_removes_row`: D.3, D.7
- `same_bundle_twice_is_idempotent`: D.7
- `replicas_may_use_different_physical_ids_for_same_public_ids`: D.1
- `query_scope_is_not_table_replication`: D.7, D.6
- `query_scope_excludes_rows_outside_current_result_set`: D.7, D.6
- `accepted_global_fate_update_reaches_peer_transaction_info`: D.2, D.7
- `stale_pending_bundle_does_not_downgrade_accepted_fate`: D.2, D.7
- `out_of_order_global_epochs_do_not_regress_current_projection`: D.3, D.7
- `rebuild_uses_global_epoch_order_not_local_tx_order`: D.3
- `direct_global_acceptance_repairs_current_projection_order`: D.3
- `remote_pending_update_does_not_override_global_current_on_peer`: D.3
- `accepted_remote_pending_update_repairs_peer_current_projection`: D.3, D.7
- `accepted_bundle_does_not_resurrect_rejected_fate`: D.2, D.7
- `direct_accept_after_reject_preserves_rejected_outcome_with_global_metadata`:
  D.2
- `direct_reject_after_accept_removes_current_but_preserves_global_metadata`:
  D.2, D.3

#### `branches.rs`

- `branch_local_write_is_invisible_on_main`: D.5
- `branch_scoped_export_excludes_unrelated_branch_rows`: D.5, D.7
- `branch_scoped_export_excludes_unrelated_deleted_rows`: D.5, D.7
- `branch_reads_main_base_with_sparse_overlay`: D.5, D.4
- `fixture_open_todos_reads_pinned_base_with_sparse_overlay`: D.5, D.4
- `branch_base_is_pinned_to_global_epoch`: D.5, D.4
- `branch_base_snapshot_chooses_latest_row_version_within_same_global_epoch`:
  D.4, D.5
- `branch_delete_shadows_pinned_base_row`: D.5, D.3
- `rejected_branch_update_reveals_pinned_base_row_again`: D.5, D.3
- `rejected_branch_delete_reveals_pinned_base_row_again`: D.5, D.3
- `branch_export_includes_pinned_main_base_rows_for_receiver_view`: D.5, D.7
- `branch_base_snapshot_respects_deletes_and_excludes_pending_main`: D.4, D.5
- `branch_base_snapshot_applies_row_policy`: D.4, D.5, D.9
- `branch_base_snapshot_ref_policy_uses_parent_at_base_epoch`: D.4, D.5, D.9
- `branch_ref_policy_uses_branch_local_parent_visibility`: D.5, D.9
- `branch_equality_query_uses_effective_branch_policy`: D.5, D.6, D.9
- `branch_base_export_preserves_ref_policy_at_base_epoch`: D.5, D.7, D.9
- `branch_multi_base_conflicts_expose_multiple_candidates`: D.5, D.12
- `branch_conflict_candidates_include_pinned_base_candidate`: D.5, D.12
- `branch_source_metadata_survives_sync`: D.5, D.7
- `branch_conflict_candidates_respect_effective_row_policy`: D.5, D.9, D.12
- `branch_conflict_candidates_survive_durable_sync_and_rejected_fate`: D.5,
  D.12, D.17
- `branch_sync_preserves_branch_provenance`: D.5, D.7
- `branch_transitive_conflict_resolution_survives_sync`: D.5, D.7, D.12
- `durable_reopen_preserves_branch_sync_and_dedupes_replay`: D.5, D.7, D.17

#### `generic_schema.rs`

- `runtime_can_install_and_write_a_non_todo_schema`: D.14
- `generic_schema_rows_rebuild_and_sync_by_public_ids`: D.1, D.3, D.7
- `generic_equality_query_scope_exports_matching_rows_and_policy_dependencies`:
  D.6, D.7, D.9
- `equality_query_scope_resync_removes_row_that_left_predicate`: D.6, D.7
- `equality_query_scope_resync_removes_deleted_matching_row`: D.6, D.7
- `branch_equality_query_scope_records_branch_predicate_read`: D.5, D.6
- `branch_equality_query_scope_resync_repairs_row_that_left_predicate`: D.5,
  D.6, D.7
- `query_predicate_reads_survive_bundle_serialization`: D.6, D.7
- `generic_equality_query_lowers_public_ref_ids_to_physical_row_ids`: D.1,
  D.6, D.14
- `generic_update_records_update_op_and_syncs_current_value`: D.2, D.3, D.7

#### `policies.rs`

- `policy_filters_reads_through_required_parent_ref`: D.6, D.9
- `policy_scoped_sync_includes_required_parent_rows_only`: D.6, D.7, D.9
- `trusted_peer_can_read_applied_policy_scoped_facts_without_user_user`:
  D.7, D.9
- `trusted_peer_generic_transaction_bypasses_user_write_policy`: D.9
- `trusted_edge_accepts_mergeable_tx_then_untrusted_peers_enforce_policy`:
  D.2, D.7, D.9
- `trusted_edge_acceptance_syncs_without_global_epoch`: D.2, D.7
- `edge_accepted_transaction_can_upgrade_to_global_epoch`: D.2
- `trusted_edge_rejects_policy_violating_tx_and_syncs_reason`: D.2, D.9,
  D.13
- `trusted_edge_authoritatively_rejects_untrusted_policy_violation_on_apply`:
  D.9, D.11
- `trusted_edge_rejects_untrusted_transaction_atomically`: D.2, D.9, D.11
- `trusted_edge_rejects_untrusted_update_to_unreadable_ref`: D.9, D.11
- `branch_write_policy_does_not_use_parent_from_different_branch`: D.5, D.9
- `branch_write_policy_uses_parent_visible_from_pinned_base`: D.5, D.9
- `branch_recursive_write_policy_uses_parent_state_from_pinned_base`: D.5,
  D.9
- `trusted_edge_validates_branch_recursive_write_policy_against_pinned_base`:
  D.5, D.9, D.11
- `trusted_edge_rejects_untrusted_delete_policy_violation`: D.9, D.11
- `created_by_write_policy_allows_self_create_but_rejects_other_writer`: D.9
- `untrusted_validation_error_does_not_leave_invalid_current_row_visible`:
  D.3, D.9, D.11
- `durable_edge_rejects_after_restart_and_repairs_memory_client`: D.9, D.17
- `policy_denied_write_is_rejected_history_not_current_state`: D.2, D.3, D.9
- `write_policy_parent_check_records_policy_read_set`: D.9, D.11
- `patch_update_uses_preserved_ref_for_write_policy_validation`: D.9
- `ref_retarget_update_validates_new_parent_policy`: D.9
- `policy_denied_delete_restores_previous_visible_row_and_subscription`: D.8,
  D.9
- `multi_row_transaction_rejects_atomically_when_one_policy_check_fails`: D.2,
  D.9
- `trusted_admin_write_bypasses_policy_but_preserves_author_provenance`: D.1,
  D.9
- `recursive_write_policy_records_transitive_policy_read_set`: D.9, D.11
- `policy_read_set_survives_sync`: D.7, D.9
- `bundle_read_sets_are_scoped_to_exported_history_transactions`: D.7, D.9

#### `recursive_queries.rs`

- `recursive_policy_filters_reads_through_grandparent_ref`: D.6, D.9
- `long_acyclic_ref_policy_chain_reads_visible_leaf`: D.9
- `schema_rejects_direct_recursive_policy_cycle`: D.9
- `schema_rejects_indirect_recursive_policy_cycle`: D.9
- `long_acyclic_recursive_policy_chain_is_sql_lowerable`: D.9, D.14
- `recursive_policy_scoped_sync_includes_transitive_parent_rows`: D.7, D.9
- `recursive_query_reads_policy_filtered_tree`: D.6, D.9
- `recursive_query_scope_sync_recreates_policy_filtered_tree`: D.6, D.7, D.9
- `recursive_query_scope_sync_exports_deleted_descendant_tombstone`: D.6, D.7
- `recursive_query_scope_sync_exports_deleted_descendant_subtree_tombstones`:
  D.6, D.7
- `recursive_query_scope_sync_includes_recursive_policy_ancestors`: D.6, D.7,
  D.9
- `recursive_query_reads_branch_base_and_sparse_overlay`: D.5, D.6
- `recursive_query_scope_sync_preserves_branch_base_and_overlay`: D.5, D.6,
  D.7
- `recursive_branch_query_export_includes_tombstone_for_deleted_base_descendant`:
  D.5, D.6, D.7
- `recursive_branch_query_export_includes_snapshot_policy_ancestors`: D.5,
  D.6, D.7, D.9

#### `schema_lenses.rs`

- `rename_lens_reads_old_storage_column_as_new_field_name`: D.10
- `rename_lens_writes_export_current_semantic_field_name`: D.10, D.7
- `renamed_ref_lens_participates_in_read_policy`: D.9, D.10
- `renamed_ref_lens_participates_in_untrusted_write_policy_validation`: D.9,
  D.10, D.11
- `user_columns_with_system_prefix_are_escaped_physically`: D.14
- `index_only_schema_changes_are_semantically_compatible`: D.10

#### `subscriptions.rs`

- `subscription_initial_snapshot_matches_query_then_diffs_semantic_rows`: D.8
- `subscription_removes_child_when_parent_policy_dependency_changes`: D.8,
  D.9
- `subscription_diffs_when_active_branch_changes`: D.5, D.8
- `subscription_on_pinned_branch_ignores_later_main_updates_until_overlay_changes`:
  D.5, D.8

#### `invariant_coverage.rs`

- `batched_refresh_matches_individual_refresh_for_mixed_predicates_and_pages`:
  D.6, D.7
- `refresh_planner_does_not_batch_across_descriptor_boundaries`: D.6, D.14
- `large_same_shape_page_refreshes_survive_multi_value_sql_chunking`: D.6,
  D.14
- `query_scope_refresh_is_idempotent_after_scope_contraction`: D.6, D.7
- `semantic_system_field_page_refresh_matches_individual_application`: D.1,
  D.6
- `transaction_causality_is_recorded_at_row_granularity`: D.2, D.11
- `rejected_fate_repairs_query_scope_and_survives_replay`: D.2, D.3, D.7,
  D.13
- `branch_observed_refreshes_are_scoped_to_checked_out_branch`: D.5, D.6,
  D.7
- `renamed_lens_query_refresh_keeps_observed_row_current_across_schema_versions`:
  D.6, D.7, D.10
- `observed_query_subscription_emits_deterministic_diff_after_batched_refresh`:
  D.6, D.8
- `recursive_batched_refresh_matches_individual_refresh_after_subtree_changes`:
  D.6, D.7
- `multi_hop_topology_refreshes_cold_client_query_after_upstream_change`:
  D.7, D.17
- `repeated_observed_query_descriptor_is_deduped`: D.6, D.7
- `forgotten_observed_query_descriptor_is_not_refreshed`: D.6, D.7
- `empty_observed_refresh_request_is_noop`: D.6, D.7
- `subscribing_to_observed_query_requires_checked_out_branch`: D.5, D.8
- `in_query_duplicate_values_are_semantically_idempotent`: D.6
- `not_equal_null_matches_present_optional_values_only`: D.6
- `repeated_bundle_replay_does_not_duplicate_history_or_current_rows`: D.3,
  D.7
- `projection_rebuild_is_semantically_identical_to_current_reads_after_mixed_fate`:
  D.2, D.3
- `durable_reopen_preserves_projection_without_rebuild`: D.3, D.17
- `accepting_same_tx_at_edge_and_global_is_monotonic`: D.2, D.7
- `rejection_then_stale_pending_replay_does_not_resurrect_current_row`: D.2,
  D.3, D.7
- `query_scope_retains_previous_row_as_local_fact_after_predicate_exit`: D.6,
  D.7
- `branch_source_metadata_updates_are_idempotent`: D.5
- `branch_backing_rows_match_branch_api_after_mutations`: D.5
- `branch_query_refresh_after_source_removal_removes_detached_source_rows`:
  D.5, D.6, D.7
- `trusted_admin_write_bypasses_policy_but_keeps_attributed_user`: D.1, D.9
- `untrusted_apply_policy_failure_is_atomic_for_multi_row_transaction`: D.2,
  D.9, D.11
- `declared_defaults_are_materialized_and_survive_sync_rebuild`: D.3, D.7,
  D.14
- `ordered_page_subscription_emits_deterministic_diff_for_order_only_change`:
  D.8
- `query_read_order_is_deterministic_after_mixed_descriptor_application`: D.6,
  D.7
- `exclusive_without_global_epoch_fails_without_writing_history`: D.2, D.11
- `mergeable_same_row_updates_can_follow_each_other`: D.2, D.3
- `update_preserves_omitted_fields_across_sync_and_rebuild`: D.2, D.3, D.7
- `deleting_invisible_row_fails_without_creating_transaction`: D.2, D.3
- `checked_out_unknown_branch_fails_without_changing_current_branch`: D.5
- `branch_base_epoch_mismatch_fails_idempotently`: D.5
- `direct_branch_source_cycle_fails_without_partial_source_change`: D.5
- `query_export_with_unknown_table_fails_without_recording_interest`: D.6,
  D.7
- `query_export_with_unknown_field_fails_without_recording_interest`: D.6,
  D.7
- `contains_query_is_case_sensitive_substring_match`: D.6
- `id_magic_field_query_is_not_confused_with_user_id_column`: D.1, D.6
- `created_by_filter_uses_authorship_not_mutable_user_column`: D.1, D.6
- `rejection_subscription_reports_detail_once_and_then_quiets`: D.8, D.13
- `same_global_epoch_tie_breaker_is_stable_after_rebuild`: D.3, D.14
- `accepted_global_fate_arriving_before_history_later_materializes_row`: D.2,
  D.7
- `durable_reopen_preserves_rejection_info_and_no_current_row`: D.2, D.3,
  D.13, D.17
- `empty_explicit_transaction_is_noop_without_history`: D.2
- `same_row_updates_in_one_transaction_normalize_to_one_history_version`: D.2,
  D.3
- `insert_then_update_same_row_in_one_transaction_seals_final_created_row`:
  D.2, D.3
- `awaiting_dependency_does_not_publish_subscription_until_dependency_arrives`:
  D.2, D.8, D.9, D.17
- `durable_node_recovers_when_fate_arrives_before_history`: D.2, D.7, D.17
- `duplicated_and_reordered_table_bundles_converge_across_topology`: D.7,
  D.17
- `missing_catalogue_state_fails_closed_without_partial_apply`: D.7, D.10,
  D.14
- `upsert_creates_missing_row_and_updates_existing_row`: D.2
- `transaction_upsert_normalizes_with_later_same_row_updates`: D.2, D.3

### E.3 Tests That Added Or Sharpened Invariants

The following behaviors are now represented in Appendix D because the tests made
them concrete:

- edge-accepted mergeables are accepted/visible without global epochs
- global epochs are not unique per transaction
- remote pending history cannot override durable current rows
- branch metadata must include base epoch/source ids, not only row branch ids
- branch-local tombstones over pinned-base rows are required
- rejected branch overlays fall back to pinned base
- query-scope repair must handle rows leaving predicates by update and delete
- query-scope export must dedupe history included for several reasons
- recursive query-scope export must include deleted descendant subtrees
- recursive write-policy read sets are transitive
- historical and branch policy evaluation must use the correct read context
- batched observed-query refresh must be semantically equivalent to refreshing
  each descriptor individually
- multi-value SQL chunking must not change page-refresh semantics
- observed query refreshes are scoped to the checked-out branch context
- query-scope refresh can keep previously observed rows current even after they
  leave the predicate, because local-first caches retain useful facts until
  explicit eviction
- `write_if_created_by_user` has distinct create and update ownership
  semantics
- generic schema installation must not be defined by the todo fixture
- trusted infrastructure peers may read applied policy-scoped facts without a
  user user
- transaction-info APIs must propagate receipts, global epochs, and rejection
  details consistently after sync
- observed query descriptors dedupe, forget, and refresh in deterministic order
- failed query export must not create observed-query interest
- magic `id` and `$createdBy` filters use semantic system fields even if a user
  column has the same surface name
- branch catalogue updates are idempotent and cycle-safe without partial source
  mutation
- ordered subscription diffs must be deterministic, while exact diff variant
  shape is still implementation-defined
- partial or policy-invalid untrusted applies are atomic: no subset of a
  rejected transaction may become current
- durable reopen preserves rejection outcome and current invisibility without
  relying on test-only projection rebuild
- empty explicit transactions create no transaction/history state
- multiple same-row mutations staged in one transaction normalize to one final
  row version
- upsert is an explicit create-or-update operation and participates in
  transaction normalization
- awaiting-dependency transactions do not publish subscription-visible rows
  until the dependency arrives
- fate-before-history delivery survives durable restart and materializes when
  history later arrives
- duplicated/reordered table bundles converge across a simple multi-tier
  topology

### E.4 Largest Untested Gaps

The largest gaps between Appendix D and the current prototype tests are:

- full vector snapshots and compact dotted-vector encoding
- exact one-simple-write transaction count, sealed transaction immutability, and
  rejection detail storage outside the hot transaction row
- explicit wait behavior for exclusive transactions at local, edge, and global
  tiers
- explicit upsert semantics across mergeable and exclusive transactions in
  multi-tier topologies
- implementation/tests for empty explicit transaction no-ops and same-row
  mutation normalization
- forwarded exclusive transaction retry/offline transport and proactive
  dependency request/subscription mechanics for mergeable `awaiting_deps`
- compact reconnect summaries and active query-descriptor replay protocol
- range and catalogue observed facts
- cache eviction policy for retained out-of-scope rows
- tier-gated query/subscription settlement semantics
- missing catalogue and missing permission fail-closed behavior
- admin-controlled catalogue publication and separate catalogue sync lane
- full authority predicate/range read-set validation beyond current row,
  absence, policy, and branch-source cases
- final product conflict metadata shape and resolved-candidate provenance
- production catalogue revision graph, migration files, inverse/cross-schema
  lenses, and copy-forward writes
- files/blobs, encryption/privacy, and encrypted indexes
- generated index/query-plan assertions and `WITHOUT ROWID` layout checks
- staged untrusted authority apply before publication
- public error object shape, global rejection callback, and redaction policy
- stable public error machine codes across write promises, queries,
  subscriptions, and sync errors
- as-of-time query/export API and timestamp-to-epoch mapping
- deterministic default ordering and subscription diff ordering
- deterministic clock/message harness for drop/delay/reconnect scenarios

This spec is serious but still evolving. New implementation results, review
comments, and experiments should be allowed to falsify parts of it. When they
do, the result should be a sharper spec and prototype, not hidden divergence.
