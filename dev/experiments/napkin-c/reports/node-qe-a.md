# Unit `node-qe-a` — jazz_node::node::query_eval (core + read_sources, normalization, lowering)

All four C files compile with `build/check.sh` with no errors and no warnings:
`query_eval.c`, `query_eval/read_sources.c`, `query_eval/normalization.c` and `query_eval/lowering.c`.

- Unit headers: `query_eval.h` is generated from the non-static definitions in `query_eval.c`, and also defines `ClientSettledBindingView` and `SeededMaintainedView`. The other three files each have a `.h`.
- `src/jazz_node/_ext_node-qe-a.h` mirrors the declarations used from other units (groove, model, engine, protocol, types, and the sibling node modules).

## Files (Rust non-test lines vs C lines)

"Non-test" leaves out the trailing `mod tests` and every `#[cfg(test)]` item.

| Rust file | Rust non-test | C file | C lines |
|---|---:|---|---:|
| node/query_eval.rs | ~4320 | node/query_eval.c (+ .h 137) | 3461 |
| node/query_eval/read_sources.rs | ~5871 | node/query_eval/read_sources.c (+ .h 146) | 4244 |
| node/query_eval/normalization.rs | ~3440 | node/query_eval/normalization.c (+ .h 37) | 2677 |
| node/query_eval/lowering.rs | ~1423 | node/query_eval/lowering.c (+ .h 210) | 1380 |
| total | ~15054 | | 11762 (+530 headers) |
| (mirror) | | _ext_node-qe-a.h | 1185 |

## Skipped and why

All skipped items are test-only:

- the `mod tests` modules
- `#[cfg(test)]` functions in query_eval.rs:
  - `compile_current_query_program_for_read_view`, `settled_binding_view_key_for_query`
  - `query_uses_heterogeneous_physical_lineage`, `query_storage_read_tables`
  - `collect_include_read_tables`, `collect_join_read_tables`, `compare_values`
  - `prepare_query_binding_for_link_in_authorization_mode`, `prepare_query_binding_for_link_with_shared_claim_fragments`
  - `prepare_client_subscription_binding`, `prepare_trusted_subscription_binding`
  - `query_rows_for_link_forced_full_scan_for_test`, `query_relation_snapshot_for_serving`
- testing-feature items:
  - the counters `take_client_physical_row_query_calls_for_test` and `take_covered_input_source_discoveries_for_test`
  - `clear_prepared_query_plan_cache_for_test` and `prepared_query_plan_cache_is_empty_for_test`
  - the `JAZZ_COMPILE_SHAPES` test tracing block
- read_sources.rs: `inline_current_graph_with_source_metadata_for_test`
- lowering.rs: the test-only `subscribe_lowered_program` variant

There are no stubs. Each non-test function is translated with its branches, validations and error strings.

## Mapping notes

- **async**: async functions become sync, so `.await` is a plain call.
- **Generics**: the storage generic `S: OrderedKvStorage` gets one copy, through the `Database` vtable mirror. Other generics are also single copies.
- **Result and Option**: `Result<T, Error>` becomes `jz_err` plus an out parameter. `Option<T>` becomes a `has_` bool, a nullable pointer, or a `bool` return with an out parameter.
- **Errors**: `Error::InvalidStoredValue` uses `JZN_INVALID_STORED(msg)`; capability errors use `JZN_QUERY_CAPABILITY`; groove errors pass through `jzn_groove`.
- **Collections**:
  - BTreeMap and BTreeSet become `jz_btree` with per-type descriptors (StrSet, StrValue, CompiledScopeTables, …).
  - HashMap caches become `jz_hmap`.
  - The prepared query plan cache holds rc-counted plans (`jz_rc_new`/`jz_rc_clone`).
  - `Rc` and `Weak` liveness tokens use `jz_rc_strong`.
- **Tuples**: tuple returns become out-structs, for example `SeededMaintainedView` for the `open_seeded_*` family (subscription, maintained view, terminal schemas, transitions, tables, initial-received flag, covered inputs).
- **Closures**: closures passed to `project_with_column_binding` and sort comparators become callback-plus-context pairs (`jz_sort` is a stable merge sort, matching `sort_by`).
- **Profiling**: `QueryReadProfile` timings use `jz_now_ms()` in place of `Instant`.
- **Tracing**: the capability trace (env-gated, non-wasm) is a no-op on wasm32. The covered-input debug trace keeps its formatting through approximated `Debug` printers (`SourceId_debug`, `BindingViewKey_debug`).
- **Cross-unit symbols**: some symbols owned by this unit but called by node-qe-b are defined here with the signatures that qe-b's mirror header expects: `aggregate_current_row_from_record`, `local_maintained_view_content_witness`, `predicate_params`, `bounded_historical_current_rows`, `query_rows_for_link`, `query_readable_current_row_including_deleted`, `aggregate_result_member_row_uuid` and `take_required_sink_deltas`.
- **Schema type**: qe-b declares `JazzSchema` where this unit uses `RuntimeSchema`. Both refer to the same opaque runtime schema.

## Blockers

None. All four files compile clean.
