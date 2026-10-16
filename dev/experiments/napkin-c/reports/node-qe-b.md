# Unit `node-qe-b`: jazz_node::node::query_eval (the remaining files) and node::state

All 21 C files compile with `build/check.sh`, with no errors:

- 15 files under `src/jazz_node/node/query_eval/`
- 6 files under `src/jazz_node/node/state/`

The state files are `include!`d into `node/mod.rs` in Rust, so their functions are named `jazz_node__node__NodeState__*`. The query_eval files use `jazz_node__node__query_eval__<file>__NodeState__*`.

Headers:

- `src/jazz_node/node/node_qe_b.h` (651 lines) holds this unit's prototypes and the AUTHZ, MAT and MV name macros.
- `src/jazz_node/_ext_node-qe-b.h` (2211 lines) mirrors every type and function used from other units: groove, model, engine, protocol, types, node core, and the qe-a files.

## Files (Rust non-test lines vs C lines)

"Non-test" means the lines before the trailing `mod tests`. Any `#[cfg(test)]` or testing-feature items inside that range are counted on the Rust side but not ported.

| Rust file (node/…) | Rust total | Rust non-test | C lines |
|---|---:|---:|---:|
| query_eval/authorization.rs | 2534 | ~1650 | 1479 |
| query_eval/bindings.rs | 1104 | 1103 | 1013 |
| query_eval/local_authority_reconciliation.rs | 76 | 34 | 25 |
| query_eval/local_availability_receipts.rs | 394 | 343 | 332 |
| query_eval/maintained_views.rs | 1576 | 1523 | 1237 |
| query_eval/materialization.rs | 1438 | 1437 | 1362 |
| query_eval/narrowed_reads.rs | 686 | 685 | 639 |
| query_eval/policy_factoring.rs | 277 | 173 | 248 |
| query_eval/prepared_bindings.rs | 191 | 190 | 260 |
| query_eval/query_read_sets.rs | 198 | 197 | 236 |
| query_eval/query_result_rows.rs | 241 | 240 | 281 |
| query_eval/route_discipline.rs | 1065 | ~974 | 1113 |
| query_eval/subscription_watermarks.rs | 224 | 189 | 223 |
| query_eval/subscriptions.rs | 1211 | 1210 | 1194 |
| query_eval/unavailable_inputs.rs | 324 | 323 | 332 |
| state/catalogue.rs | 1398 | 1397 | 1146 |
| state/commit.rs | 1570 | 1569 | 1439 |
| state/contribution_merge.rs | 93 | 92 | 76 |
| state/durable.rs | 866 | 865 | 725 |
| state/lifecycle.rs | 2642 | 2641 | 1932 |
| state/read_payload.rs | 563 | 562 | 636 |
| **total** | **18671** | **~17397** | **15928** (+2862 header lines) |

These files belong to node-qe-a and are not covered here: query_eval.rs, read_sources.rs, normalization.rs and lowering.rs. The `query_eval/tests` directory is not covered either.

## Skipped and why

Everything skipped is test-only (`#[cfg(test)]` or `#[cfg(any(test, feature = "testing"))]`):

- **authorization.rs:**
  - `PolicyProofCompilationPause` and its thread-local, with `pause_/wait_for_policy_proof_compilation_for_test`;
  - `test_content_current_with_version`;
  - the `authorization_scope_compiler_tests` module.
- **route_discipline.rs:** the `SCOPED_BINDING_SOURCES` thread-local and `take_scoped_binding_sources_for_test`.
- **maintained_views.rs:** `LocalMaintainedViewSubscriptionFootprint`, `postcard_len`, `compiled_authorization_mode`, `footprint`, and `terminal_transition_tests`.
- **subscriptions.rs:** `outbound_shape_owner_count_for_test`, `authority_result_state_for_binding_view`, and the snapshot-call counters.
- **catalogue.rs:** `activate_schema_for_test`.
- **commit.rs:** `rebuild_ahead_current_keys_with_receipt` and the receipt counter.
- **durable.rs:**
  - `set_catalogue_activation_failpoint`, `mutate_current_schema_for_testing`;
  - the `*_groove_runtime_for_test` functions;
  - `runtime_stats_for_test`, `history_class_bytes_for_test`, `encoded_storage_bytes_for_test`;
  - the delivery-diagnostics record.
- **lifecycle.rs:**
  - `pending_upload_count_for_test`, `new_with_shared_test_catalogue`;
  - `new_with_open_receipt_for_test` and the `NodeOpenReceipt` timing plumbing;
  - the `genesis_identities` fixture override;
  - `set_relay_authority_session_owner_for_test`, `set_test_provider_claims`, `admit_test_session_claims`, `attach_large_cell_for_test`;
  - the delivery-diagnostics record.

Some code is server-only (`#[cfg(feature = "runtime")]`). It is ported, but only compiles under `#ifdef JZ_FEATURE_RUNTIME`:

- `validate_schema_activation` (catalogue);
- `select_durable_reopen_schema` and `enable_authoritative_scalar_exit_refresh` (lifecycle).

No stubs were added. Every function from another unit is declared in the mirror header and not defined.

## How the Rust mapped to C

- **Async:** every `async fn` is synchronous. `Result<T, Error>` becomes a `jz_err` return plus an out-parameter.
- **Generic storage parameter:** the `S`/`T: ReopenableStorage` parameter on `NodeState<S>` is erased.
  - Storage is an opaque `jazz_node_BoxedStorage *` that moves through `Database::new_with_storage_layout` / `into_storage`, the same way the Rust constructors box it.
  - The groove `Database` sits behind the NodeState `database` pointer, which stands in for `DatabaseSlot`.
- **`PublicationOutcome<T>`** (commit.rs) holds an erased `void *value`. There is one C copy instead of one per instantiation.
- **Generic sort and route-discipline helpers** (route_discipline.rs) are each written once, with callback and context-pointer comparators, instead of once per monomorphised use.
- **policy_factoring.rs** uses the same u64-word bitsets as the Rust code. Its fixed-point factoring loop is translated as is.
- **GraphBuilder** chains from groove are flattened into sequential builder calls.
- **Opaque types:** `VersionRow`, `OutputOccurrenceId`, `LargeValueRef` and the chunk handles (`Rc<dyn MissingChunkResolver>`, `LocalChunkReader`, `OwnedChunkProvider`) are opaque, with accessor and release functions.
- **`ActiveSessionClaimsScope`** is a by-value guard struct. The Rust drop becomes an explicit `ActiveSessionClaimsScope__drop`.
- **`Rc<Cell<bool>>`** (`detach_covered_chunk_waits`) and `Rc<AtomicU64>` (the reservation clock) become `jz_rc` payloads.

## Notes and inconsistencies for integration

Some functions are named differently by different callers. These need reconciling at link time:

- The query_eval code calls `jazz_node__node__views__NodeState__cached_tx_versions` and `materialized_cells_for_version`. commit.c defines those helpers under `jazz_node__node__NodeState__*`, because they live in state/commit.rs.
- `query_transaction` is declared in the mirror under both `views__` and `currency__`.
- `translate_cells` is declared under both `lenses__` and `source_resolution__`.
- Some other units' headers disagree with each other on signatures. Examples:
  - `RecordDescriptor__create` and `bind` take the descriptor by value in some headers and by pointer in others;
  - `LargeValueRef__kind` returns u8 in some and u32 in others;
  - groove `Database` large-value functions sit under `facade__` in some and directly under `db__` in others.

  This unit's mirror picks one consistent pointer-based form and does not include those headers.

## Header fixes from the resumed run

- A duplicate `JZ_LVSK` enum in the mirror broke every query_eval file. The ones reported were narrowed_reads, local_availability_receipts, query_result_rows, subscription_watermarks, bindings and policy_factoring. The duplicate was removed, and all 15 files compile again.
- `NodeState` fields were tightened:
  - `rejections` is a named `struct jazz_node_RejectionTracking`;
  - `detach_covered_chunk_waits` is a `bool *`;
  - `large_value_staging_policy` is a `jazz_node_LargeValueStagingPolicy`;
  - `relay_authority_session_owner` is a `jazz_node_ClientRelayScope *`.
- `groove_StagedLargeValue` now has `accounting.encoded_bytes` and `created_at_ms`.
