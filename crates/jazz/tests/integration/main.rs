// One integration-test binary for Jazz's flat test files.
//
// Every flat file used to be its own test executable, each compiling and
// linking the full Jazz dependency graph. Here they build as modules of one
// binary; Nextest still runs each test in its own process. A module whose file
// used to need `required-features` is gated by the same feature with `cfg`, so
// a build without that feature skips it exactly as Cargo skipped the binary.
// account_author (runtime) and the four files calling `*_for_test` helpers
// (testing) are gated too: they never compiled without those features, and
// ungated they would break the whole binary for a plain `cargo test -p jazz`.
//
// Two files stay separate binaries (see Cargo.toml): incremental_delivery_canary
// installs a #[global_allocator], and legacy_benchmark_smoke is selected by
// name in the CI and benchmark gates.

#[cfg(feature = "runtime")]
#[path = "../account_author.rs"]
mod account_author;
#[cfg(feature = "testing")]
#[path = "../async_open.rs"]
mod async_open;
#[cfg(feature = "runtime")]
#[path = "../auth_admission.rs"]
mod auth_admission;
#[cfg(feature = "testing")]
#[path = "../authorization_scope_reentry.rs"]
mod authorization_scope_reentry;
#[path = "../branch_views.rs"]
mod branch_views;
#[cfg(feature = "testing")]
#[path = "../browser_relay_durability.rs"]
mod browser_relay_durability;
#[path = "../column_defaults.rs"]
mod column_defaults;
#[cfg(feature = "testing")]
#[path = "../composite_indexes.rs"]
mod composite_indexes;
#[cfg(feature = "testing")]
#[path = "../core_client_topology.rs"]
mod core_client_topology;
#[cfg(feature = "runtime")]
#[path = "../core_fate_authority.rs"]
mod core_fate_authority;
#[path = "../coverage_group_flush_once.rs"]
mod coverage_group_flush_once;
#[path = "../deferred_local_persistence.rs"]
mod deferred_local_persistence;
#[path = "../deployment_preparation.rs"]
mod deployment_preparation;
#[path = "../dynamic_schema_views.rs"]
mod dynamic_schema_views;
#[path = "../error_code_strings.rs"]
mod error_code_strings;
#[cfg(feature = "runtime")]
#[path = "../exclusive_snapshot_coverage.rs"]
mod exclusive_snapshot_coverage;
#[path = "../fate_regressions.rs"]
mod fate_regressions;
#[cfg(feature = "testing")]
#[path = "../fate_replay.rs"]
mod fate_replay;
#[path = "../large_json_wire.rs"]
mod large_json_wire;
#[path = "../large_value_append.rs"]
mod large_value_append;
#[path = "../large_value_read_scaling.rs"]
mod large_value_read_scaling;
#[path = "../large_value_streaming_create.rs"]
mod large_value_streaming_create;
#[path = "../large_value_subscription_scaling.rs"]
mod large_value_subscription_scaling;
#[path = "../large_value_tx_update.rs"]
mod large_value_tx_update;
#[cfg(feature = "testing")]
#[path = "../local_first_unless_empty.rs"]
mod local_first_unless_empty;
#[path = "../order_by_unselected_column.rs"]
mod order_by_unselected_column;
#[cfg(feature = "testing")]
#[path = "../parameterized_subscription_routing.rs"]
mod parameterized_subscription_routing;
#[path = "../persistent_codec_family_registry.rs"]
mod persistent_codec_family_registry;
#[cfg(feature = "testing")]
#[path = "../prepared_claim_routing.rs"]
mod prepared_claim_routing;
#[path = "../public_transaction_id_api.rs"]
mod public_transaction_id_api;
#[path = "../route_subscription_benchmark_contract.rs"]
mod route_subscription_benchmark_contract;
#[path = "../row_provenance.rs"]
mod row_provenance;
#[path = "../shared_coverage_differential.rs"]
mod shared_coverage_differential;
#[cfg(feature = "testing")]
#[path = "../shared_query_hydration.rs"]
mod shared_query_hydration;
#[path = "../structured_result_tree.rs"]
mod structured_result_tree;
#[cfg(feature = "testing")]
#[path = "../threaded_client_relay.rs"]
mod threaded_client_relay;
#[path = "../uuid_page_probe.rs"]
mod uuid_page_probe;
#[path = "../warm_reopen_differential.rs"]
mod warm_reopen_differential;
#[path = "../wire_fixtures.rs"]
mod wire_fixtures;
