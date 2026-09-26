// Single integration-test binary (experiment).
#[path = "../admin_schema_api.rs"]
mod admin_schema_api;
#[path = "../aggregate_subscriptions.rs"]
mod aggregate_subscriptions;
#[path = "../array_reference_policies.rs"]
mod array_reference_policies;
#[path = "../auth_admission.rs"]
mod auth_admission;
#[path = "../bigint_integration.rs"]
mod bigint_integration;
#[path = "../branch_claims_integration.rs"]
mod branch_claims_integration;
#[path = "../catalogue_sync_integration.rs"]
mod catalogue_sync_integration;
#[path = "../claims_merge_integration.rs"]
mod claims_merge_integration;
#[path = "../client_storage_shutdown_integration.rs"]
mod client_storage_shutdown_integration;
#[path = "../clients_sync.rs"]
mod clients_sync;
#[path = "../durable_local_write_replay_integration.rs"]
mod durable_local_write_replay_integration;
#[path = "../flush_once_per_refresh.rs"]
mod flush_once_per_refresh;
#[path = "../gset_merge.rs"]
mod gset_merge;
#[path = "../history_conflict.rs"]
mod history_conflict;
#[path = "../inherited_policies.rs"]
mod inherited_policies;
#[path = "../json_storage.rs"]
mod json_storage;
#[path = "../large_json_permissions.rs"]
mod large_json_permissions;
#[path = "../large_value_subscriptions.rs"]
mod large_value_subscriptions;
#[path = "../local_first_auth_integration.rs"]
mod local_first_auth_integration;
#[path = "../local_first_unless_empty.rs"]
mod local_first_unless_empty;
#[path = "../merged_redelivery.rs"]
mod merged_redelivery;
#[path = "../mixed_generation_history.rs"]
mod mixed_generation_history;
#[path = "../native_account_admission.rs"]
mod native_account_admission;
#[path = "../native_client_channel_pump.rs"]
mod native_client_channel_pump;
#[path = "../nullable_writes.rs"]
mod nullable_writes;
#[path = "../output_occurrence_id.rs"]
mod output_occurrence_id;
#[path = "../policy_branch_closure.rs"]
mod policy_branch_closure;
#[path = "../policy_dependency_sync.rs"]
mod policy_dependency_sync;
#[path = "../query_membership_filters.rs"]
mod query_membership_filters;
#[path = "../readable_scope_exit.rs"]
mod readable_scope_exit;
#[path = "../reconnect_write_durability.rs"]
mod reconnect_write_durability;
#[path = "../rename_write_authorization.rs"]
mod rename_write_authorization;
#[path = "../replica_settlement.rs"]
mod replica_settlement;
#[path = "../schema_migration_policies.rs"]
mod schema_migration_policies;
#[path = "../scope_revocation.rs"]
mod scope_revocation;
#[path = "../server_subscriptions.rs"]
mod server_subscriptions;
#[path = "../subscription_initial_hydration.rs"]
mod subscription_initial_hydration;
#[path = "../subscription_lifecycle.rs"]
mod subscription_lifecycle;
#[path = "../transactions.rs"]
mod transactions;
