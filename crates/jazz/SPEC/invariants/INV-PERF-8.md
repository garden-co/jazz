# INV-PERF-8

- Status: now
- Coverage: ✓

## Invariant

INV-PERF-8 cold current-only subscription hydration and current-view version-witness payload sourcing should be O(current rows), not O(history depth), for degenerate whole-table global current-row shapes. Global current content/register rows carry canonical winner metadata and settle position, so version witnesses do not require a current-key → history join. Benchmark receipt comes from `benches/cold_subscription.rs`; byte-fidelity is enforced by the denormalized-current witness test.

## Enforced by (tests)

`jazz::node::query_eval::tests::denormalized_current_content_witness_projects_history_provenance_to_unix_milliseconds`

## Implementation

`schema.rs::TableSchema::global_current_storage_tables`; `schema.rs::TableSchema::ahead_current_storage_tables`; `node/codec.rs::global_current_values`; `node/query_eval.rs::content_version_current_source_graph`; `node/ingest.rs::NodeState::write_global_current_update`
