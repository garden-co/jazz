// One integration-test binary for Groove's flat test files.
//
// Every flat file used to link its own test executable against Groove and its
// storage stack. Building them as modules of one binary compiles and links
// that dependency graph once. Nextest still runs each test in its own
// process. The two allocation-counting files install a #[global_allocator]
// and stay separate binaries (see Cargo.toml).
#[path = "../aggregate_sql_semantics.rs"]
mod aggregate_sql_semantics;
#[path = "../anti_join_regressions.rs"]
mod anti_join_regressions;
#[path = "../arrangement_regressions.rs"]
mod arrangement_regressions;
#[path = "../async_hydration_session.rs"]
mod async_hydration_session;
#[path = "../chunk_provider.rs"]
mod chunk_provider;
#[path = "../direct_metadata_progress.rs"]
mod direct_metadata_progress;
#[path = "../inline_records_snapshot.rs"]
mod inline_records_snapshot;
#[path = "../large_value_leaf_codec.rs"]
mod large_value_leaf_codec;
#[path = "../large_value_query.rs"]
mod large_value_query;
#[path = "../multisink_subscription.rs"]
mod multisink_subscription;
#[path = "../operator_pipeline.rs"]
mod operator_pipeline;
#[path = "../plain_output_positions_unbounded.rs"]
mod plain_output_positions_unbounded;
#[path = "../plain_output_root_positions.rs"]
mod plain_output_root_positions;
#[path = "../prepared_batches.rs"]
mod prepared_batches;
#[path = "../prepared_binding_regressions.rs"]
mod prepared_binding_regressions;
#[path = "../prepared_binding_scale.rs"]
mod prepared_binding_scale;
#[path = "../primary_key_lookup.rs"]
mod primary_key_lookup;
#[path = "../query_templates.rs"]
mod query_templates;
#[path = "../recursive_cycle_regressions.rs"]
mod recursive_cycle_regressions;
#[path = "../root_rank_subscription.rs"]
mod root_rank_subscription;
#[path = "../route_barrier_parked_publication.rs"]
mod route_barrier_parked_publication;
#[path = "../snapshot_subscription_regressions.rs"]
mod snapshot_subscription_regressions;
#[path = "../storage_residency.rs"]
mod storage_residency;
#[path = "../terminal_occurrence_keys.rs"]
mod terminal_occurrence_keys;
#[path = "../top_by_terminal_identity.rs"]
mod top_by_terminal_identity;
#[path = "../variant_ab_benchmark.rs"]
mod variant_ab_benchmark;
#[path = "../variant_tables.rs"]
mod variant_tables;
#[path = "../versioned_rows.rs"]
mod versioned_rows;
