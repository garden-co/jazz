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
#[path = "../host_tables.rs"]
mod host_tables;
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
#[path = "../relay_local_seqs.rs"]
mod relay_local_seqs;
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
#[path = "../transaction_handles.rs"]
mod transaction_handles;
#[path = "../union_queries.rs"]
mod union_queries;
#[path = "../uuid_page_probe.rs"]
mod uuid_page_probe;
#[path = "../write_check_evidence.rs"]
mod write_check_evidence;
