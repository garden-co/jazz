# node-core-a: napkin C port report

Unit: `jazz_node` (crates/jazz/layers/node/src). It covers node/codec, node/physical (+ submodules), node/ingest (+ submodules) and node/maintained_subscription_view.

Status: **every in-scope Rust file is ported and compiles.** Running `build/check.sh` over all 16 unit C files gives `OK (16 files)`, with no warnings for maintained_subscription_view.c.

## Files

The Rust line counts below exclude `#[cfg(test)]` code.

| Rust file | Rust lines (non-test) | C file | C lines |
|---|---:|---|---:|
| node/codec.rs | 3431 | node/codec.c + codec.h | 3293 + 315 |
| node/physical.rs + physical/names.rs | 39 + 57 | node/physical.c + physical.h | 239 + 276 |
| physical/bindings.rs | 629 | physical/bindings.c | 500 |
| physical/catalogue.rs | 509 | physical/catalogue.c | 455 |
| physical/descriptors.rs | 1160 | physical/descriptors.c | 1374 |
| physical/enum_lookup.rs | 330 | physical/enum_lookup.c | 337 |
| physical/projections.rs | 1466 | physical/projections.c | 1347 |
| physical/storage.rs | 685 | physical/storage.c | 717 |
| node/ingest.rs | 73 | node/ingest.c + ingest.h | 120 + 237 |
| ingest/catalogue.rs | 972 | ingest/catalogue.c | 856 |
| ingest/commit_bundles.rs | 961 | ingest/commit_bundles.c | 1149 |
| ingest/fates.rs | 905 | ingest/fates.c | 1101 |
| ingest/validation.rs | 666 | ingest/validation.c | 765 |
| ingest/view_updates.rs | 962 | ingest/view_updates.c | 1148 |
| ingest/write_base.rs | 487 | ingest/write_base.c | 419 |
| node/maintained_subscription_view.rs | ~3065 | maintained_subscription_view.c + .h | 3194 + 191 |
| (foreign mirrors) | | src/jazz_node/_ext_node-core-a.h | 1507 |

Totals: about 16.4k non-test Rust lines became about 18.0k lines of C (.c + .h), plus 1.5k lines of mirror header.

## Skipped (not ported)

These were skipped on purpose:

- **Everything under `#[cfg(test)]`**:
  - physical/tests.rs
  - codec test-only helpers
  - in fates.rs:
    - `shape_predicate_changed_after`
    - `commit_unit_satisfies_write_policies`
    - `transaction_ids`
    - test asserts
  - in view_updates.rs: the test-only functions
  - in maintained_subscription_view.rs:
    - `covered_input_for_version`
    - `apply_decoded_deltas`
    - `structured_app_row`
    - `structured_app_rows`
    - `assert_incremental_footprint_matches_full_scan`
    - `ReplacementIndex::assert_footprint_matches_full_scan`
    - `replacement_map_bytes`
    - the `result_member_reconcile_visits` and `root_order_insert_comparisons` counters
    - the `#[cfg(test)]` byte-equivalence oracle inside `decode_typed_version_witness`
- **The `testing` feature**:
  - `MaintainedSubscriptionView.compiled_authorization_mode`
  - `MaintainedTerminalSchemas::footprint` and `MaintainedTerminalSchemasFootprint`
- **`cold-settle-attribution`**: tracing spans and `record_conversion`.
- **Dead outside tests and absent from browser-coverage.csv**:
  - the write_base `WriteConflict`, `authored_overlap`, `write_conflict` and `row_write_conflicts`
  - `remap_authored_*` in descriptors and storage
  - the catalogue failpoint
  - the benchmark helper `ingest_captured_reset_bundles_for_benchmark`
- **`MaintainedSubscriptionView` / `MaintainedTerminalSchemas` derive(Clone)**: only tests clone them. `ClientLocalLiteralToken`'s custom Clone exists only to serve that clone.
- **Serde and Debug derives.** The covered-input trace `eprintln!`s are ported; they print through a mirrored `jz_eprint` with variant names instead of full `{:?}` dumps.
- `debug_assert_lowered_layouts` is a no-op. Other `debug_assert!`s are `JZ_ASSERT`s.

## Notes and approximations

- **async → sync**: Rust async functions are plain synchronous C functions.
- **Foreign mirrors**: all foreign types and functions are approximate mirrors in `_ext_node-core-a.h`. They follow other units' headers where those exist:
  - protocol result members, SupportingRow and ProgramFactEntry come from jazz_protocol/protocol.h
  - query_engine schemas come from jazz_engine/query_engine/schemas.h
  - SupportingFrontier, TerminalRootLayout, TerminalRecordState and descriptor_roles come from core-b's .c files
  - groove TerminalOperation, multisink and RecordDeltas follow core-b and qe-b
- **Mirror differences you may hit at link time**:
  - `groove_RecordDescriptor` is a copyable handle here. core-b uses a pointer.
  - `ProgramFactEntry` only carries the arms this unit builds.
  - qe-b's `replacement_for` takes a 4-argument form. This unit's takes 6 arguments and returns both winners.
- **Ownership choices**:
  - `Arc<[u8]>` / `Arc<VersionPayload>` / `Rc<ResultMemberEntry>` / `Rc<ProgramFactEntry>` use `jz_rc`.
  - `RetainedResultMap<V>` is one struct with a value-kind flag (i64 weights or payload entries).
  - The `HashMap<RecordDescriptor, CurrentPayloadEncodePlan>` is a `jz_hmap` keyed by descriptor hash.
- **Owned by core-b**: the `AheadCurrentKeys` insert and remove functions live in core-b's mod.c. ingest.h only mirrors them, with their real signatures (bool return plus `prev` / `removed` out-parameters).
- **Mirrors added during the port**:
  - `record_applied_global_time_collect` (the Rust function returns the advanced globals)
  - `jazz_model_BranchKey_cmp` / `_eq`
  - `DatabaseBatch::insert_variant`
  - the 3-argument `apply_compiled_lens_path`
  - `jz_blake3_derive_key`
  - `RuntimeSchema::decode_persisted_branch_key`
  - `NodeAliases::retarget_count` / `len`
- **codec additions**:
  - exports `jzn_desc_str_value_pub`, used by maintained_subscription_view for `VersionRowParts.cells`
  - `StoredTransaction` clone and drop, used by policy.c in another unit

## Blockers

None. Nothing in scope remains unported.
