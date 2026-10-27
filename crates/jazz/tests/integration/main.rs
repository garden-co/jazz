//! Jazz's integration tests, built as one binary: each flat `tests/*.rs`
//! file is a module here (see dev/gates/test/rust-test-targets.test.mjs).
//! Tests that need a process of their own (durable_file re-executes and
//! measures its binary) stay separate targets.

#[allow(dead_code)]
#[path = "../common/mod.rs"]
mod common;

#[path = "../aggregate_nullable_keys.rs"]
mod aggregate_nullable_keys;
#[path = "../aggregate_ordering.rs"]
mod aggregate_ordering;
#[path = "../array_columns.rs"]
mod array_columns;
#[path = "../authorization_scope_reentry.rs"]
mod authorization_scope_reentry;
#[path = "../binding_host.rs"]
mod binding_host;
#[path = "../branch_views.rs"]
mod branch_views;
#[path = "../claims_change.rs"]
mod claims_change;
#[path = "../column_defaults.rs"]
mod column_defaults;
#[path = "../commit_atomicity_frames.rs"]
mod commit_atomicity_frames;
#[path = "../composite_indexes.rs"]
mod composite_indexes;
#[path = "../deleted_rows.rs"]
mod deleted_rows;
#[path = "../deployment_preparation.rs"]
mod deployment_preparation;
#[path = "../dynamic_schema_views.rs"]
mod dynamic_schema_views;
#[path = "../error_code_strings.rs"]
mod error_code_strings;
#[path = "../existential_joins.rs"]
mod existential_joins;
#[path = "../fanout_differential.rs"]
mod fanout_differential;
#[path = "../flat_joins.rs"]
mod flat_joins;
#[path = "../gather_policy_double_ref.rs"]
mod gather_policy_double_ref;
#[path = "../history_reads.rs"]
mod history_reads;
#[path = "../include_live.rs"]
mod include_live;
#[path = "../large_value_append.rs"]
mod large_value_append;
#[path = "../large_value_read_scaling.rs"]
mod large_value_read_scaling;
#[path = "../large_value_streaming_create.rs"]
mod large_value_streaming_create;
#[path = "../large_value_subscription_scaling.rs"]
mod large_value_subscription_scaling;
#[path = "../large_value_sync.rs"]
mod large_value_sync;
#[path = "../large_value_tx_update.rs"]
mod large_value_tx_update;
#[path = "../large_values.rs"]
mod large_values;
#[path = "../local_first_first_load_gate.rs"]
mod local_first_first_load_gate;
#[path = "../local_first_server_wait.rs"]
mod local_first_server_wait;
#[path = "../order_by_unselected_column.rs"]
mod order_by_unselected_column;
#[path = "../parameterized_subscription_routing.rs"]
mod parameterized_subscription_routing;
#[path = "../permission_advice.rs"]
mod permission_advice;
#[path = "../policy_activation.rs"]
mod policy_activation;
#[path = "../prepared_claim_routing.rs"]
mod prepared_claim_routing;
#[path = "../provenance.rs"]
mod provenance;
#[path = "../relation_policies.rs"]
mod relation_policies;
#[path = "../relation_queries.rs"]
mod relation_queries;
#[path = "../relation_results.rs"]
mod relation_results;
#[path = "../relay_policy_subscriptions.rs"]
mod relay_policy_subscriptions;
#[path = "../schema_activation.rs"]
mod schema_activation;
#[path = "../schema_renames.rs"]
mod schema_renames;
#[path = "../serializable_relay.rs"]
mod serializable_relay;
#[path = "../serializable_sqlite.rs"]
mod serializable_sqlite;
#[path = "../serializable_sync.rs"]
mod serializable_sync;
#[path = "../session_policy_values.rs"]
mod session_policy_values;
#[path = "../session_write_checks.rs"]
mod session_write_checks;
#[path = "../snapshots.rs"]
mod snapshots;
#[path = "../storage_corpus.rs"]
mod storage_corpus;
#[path = "../storage_errors.rs"]
mod storage_errors;
#[path = "../structured_result_tree.rs"]
mod structured_result_tree;
#[path = "../sync_aggregates.rs"]
mod sync_aggregates;
#[path = "../sync_differential.rs"]
mod sync_differential;
#[path = "../sync_echo.rs"]
mod sync_echo;
#[path = "../sync_gather.rs"]
mod sync_gather;
#[path = "../sync_resume.rs"]
mod sync_resume;
#[path = "../sync_sessions.rs"]
mod sync_sessions;
#[path = "../sync_support.rs"]
mod sync_support;
#[path = "../sync_support_access.rs"]
mod sync_support_access;
#[path = "../sync_untrusted_clients.rs"]
mod sync_untrusted_clients;
#[path = "../sync_wire_corpus.rs"]
mod sync_wire_corpus;
#[path = "../union_queries.rs"]
mod union_queries;
#[path = "../uuid_page_probe.rs"]
mod uuid_page_probe;
#[path = "../write_check_evidence.rs"]
mod write_check_evidence;

#[path = "../binding_identity_transactions.rs"]
mod binding_identity_transactions;

#[path = "../e2ee_sqlite_blockers.rs"]
mod e2ee_sqlite_blockers;

#[path = "../sqlite_accepted_overlay.rs"]
mod sqlite_accepted_overlay;

#[path = "../sqlite_accepted_union_reads.rs"]
mod sqlite_accepted_union_reads;

#[path = "../sqlite_accepted_wire.rs"]
mod sqlite_accepted_wire;

#[path = "../sqlite_array_inheritance.rs"]
mod sqlite_array_inheritance;

#[path = "../sqlite_auth_mode.rs"]
mod sqlite_auth_mode;

#[path = "../sqlite_authenticated_views.rs"]
mod sqlite_authenticated_views;

#[path = "../sqlite_candidate_relations.rs"]
mod sqlite_candidate_relations;

#[path = "../sqlite_candidate_uuid_correlations.rs"]
mod sqlite_candidate_uuid_correlations;

#[path = "../sqlite_candidates.rs"]
mod sqlite_candidates;

#[path = "../sqlite_catalogue.rs"]
mod sqlite_catalogue;

#[path = "../sqlite_catalogue_cache.rs"]
mod sqlite_catalogue_cache;

#[path = "../sqlite_catalogue_policies.rs"]
mod sqlite_catalogue_policies;

#[path = "../sqlite_catalogue_read_admission.rs"]
mod sqlite_catalogue_read_admission;

#[path = "../sqlite_catalogue_wire.rs"]
mod sqlite_catalogue_wire;

#[path = "../sqlite_content_origins.rs"]
mod sqlite_content_origins;

#[path = "../sqlite_differential_matrix.rs"]
mod sqlite_differential_matrix;

#[path = "../sqlite_fetch_transport_admission.rs"]
mod sqlite_fetch_transport_admission;

#[path = "../sqlite_foreground_initialization.rs"]
mod sqlite_foreground_initialization;

#[path = "../sqlite_foreground_leases.rs"]
mod sqlite_foreground_leases;

#[path = "../sqlite_foreground_reads.rs"]
mod sqlite_foreground_reads;

#[path = "../sqlite_initialization.rs"]
mod sqlite_initialization;

#[path = "../sqlite_large_trigger_groups.rs"]
mod sqlite_large_trigger_groups;

#[path = "../sqlite_owner_durability.rs"]
mod sqlite_owner_durability;

#[path = "../sqlite_owner_frozen_inputs.rs"]
mod sqlite_owner_frozen_inputs;

#[path = "../sqlite_owner_initialization_wire.rs"]
mod sqlite_owner_initialization_wire;

#[path = "../sqlite_owner_inputs.rs"]
mod sqlite_owner_inputs;

#[path = "../sqlite_owner_policy_inputs.rs"]
mod sqlite_owner_policy_inputs;

#[path = "../sqlite_owner_projection.rs"]
mod sqlite_owner_projection;

#[path = "../sqlite_owner_retirement.rs"]
mod sqlite_owner_retirement;

#[path = "../sqlite_owner_session_reads.rs"]
mod sqlite_owner_session_reads;

#[path = "../sqlite_owner_transactions.rs"]
mod sqlite_owner_transactions;

#[path = "../sqlite_owner_writes.rs"]
mod sqlite_owner_writes;

#[path = "../sqlite_paired_reads.rs"]
mod sqlite_paired_reads;

#[path = "../sqlite_policy_descriptor_wire.rs"]
mod sqlite_policy_descriptor_wire;

#[path = "../sqlite_private_owner_handoff.rs"]
mod sqlite_private_owner_handoff;

#[path = "../sqlite_query_inheritance.rs"]
mod sqlite_query_inheritance;

#[path = "../sqlite_read_hydration.rs"]
mod sqlite_read_hydration;

#[path = "../sqlite_readonly_transactions.rs"]
mod sqlite_readonly_transactions;

#[path = "../sqlite_relation_aliases.rs"]
mod sqlite_relation_aliases;

#[path = "../sqlite_relation_normalization.rs"]
mod sqlite_relation_normalization;

#[path = "../sqlite_relay_fetch.rs"]
mod sqlite_relay_fetch;

#[path = "../sqlite_relay_uploads.rs"]
mod sqlite_relay_uploads;

#[path = "../sqlite_remote_inheritance.rs"]
mod sqlite_remote_inheritance;

#[path = "../sqlite_remote_reachability.rs"]
mod sqlite_remote_reachability;

#[path = "../sqlite_remote_read_freshness.rs"]
mod sqlite_remote_read_freshness;

#[path = "../sqlite_remote_write_barrier.rs"]
mod sqlite_remote_write_barrier;

#[path = "../sqlite_scoped_evidence.rs"]
mod sqlite_scoped_evidence;

#[path = "../sqlite_scoped_fetch_wire.rs"]
mod sqlite_scoped_fetch_wire;

#[path = "../sqlite_scoped_relations.rs"]
mod sqlite_scoped_relations;

#[path = "../sqlite_scoped_union_reads.rs"]
mod sqlite_scoped_union_reads;

#[path = "../sqlite_scoped_union_subscriptions.rs"]
mod sqlite_scoped_union_subscriptions;

#[path = "../sqlite_shutdown.rs"]
mod sqlite_shutdown;

#[path = "../sqlite_transaction_read_tiers.rs"]
mod sqlite_transaction_read_tiers;

#[path = "../sqlite_union_queries.rs"]
mod sqlite_union_queries;

#[path = "../sqlite_union_remote_reads.rs"]
mod sqlite_union_remote_reads;

#[path = "../sqlite_union_transactions.rs"]
mod sqlite_union_transactions;

#[path = "../sqlite_write_state.rs"]
mod sqlite_write_state;

#[path = "../sqlite_writer_order_format.rs"]
mod sqlite_writer_order_format;

#[path = "../sqlite_writer_ordering.rs"]
mod sqlite_writer_ordering;

#[path = "../staged_streaming_sqlite.rs"]
mod staged_streaming_sqlite;

#[path = "../sqlite_sync_v2.rs"]
mod sqlite_sync_v2;

#[path = "../sqlite_upstream_read_wire.rs"]
mod sqlite_upstream_read_wire;
