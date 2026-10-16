# groove-ivm-rt-b: napkin C port report

Scope: every file in `crates/groove/src/ivm/runtime/` except `subscriptions.rs`, `runtime_tick.rs`, `evaluator.rs`, `evaluator/` and `record_projection.rs`, which belong to rt-a. `tests.rs` and `retained_gc_tests.rs` contain only tests and are not ported.

Every ported file passes `build/check.sh` (wasm32, -Oz). In the final run, all 23 `.c` files of the unit were checked together: "OK (23 files)". The last three files written (schema.c, typed_template.c and mod.c) were also checked one at a time, with zero warnings.

## Files: Rust non-test LOC vs C LOC

The Rust figures come from `build/rust-loc.txt` (non-test lines). The C figures are `wc -l`.

| Rust file | Rust LOC | C file | C LOC |
|---|---:|---|---:|
| join.rs | 798 | ivm/runtime/join.c | 836 |
| persist.rs | 237 | ivm/runtime/persist.c | 210 |
| operator_updates.rs | 940 | ivm/runtime/operator_updates.c | 790 |
| compilation_cache.rs | 121 | ivm/runtime/compilation_cache.c | 137 |
| state.rs | 298 | ivm/runtime/state.c | 254 |
| evaluation_memo.rs | 165 | ivm/runtime/evaluation_memo.c | 203 |
| pipeline.rs | 410 | ivm/runtime/pipeline.c | 405 |
| pipeline/fields.rs | 182 | ivm/runtime/pipeline/fields.c | 215 |
| recursion.rs | 1764 | ivm/runtime/recursion.c | 1498 |
| index_ids.rs | 270 | ivm/runtime/index_ids.c | 282 |
| graph_lifecycle.rs | 305 | ivm/runtime/graph_lifecycle.c | 267 |
| aggregate.rs | 327 | ivm/runtime/aggregate.c | 308 |
| typed_template.rs | 475 | ivm/runtime/typed_template.c | 628 |
| mod.rs | 624 | ivm/runtime/mod.c | 576 |
| compilation.rs | 2060 | ivm/runtime/compilation.c | 1254 |
| counted_map.rs | 234 | ivm/runtime/counted_map.c | 265 |
| schema.rs | 903 | ivm/runtime/schema.c | 765 |
| typed_projection.rs | 90 | ivm/runtime/typed_projection.c | 131 |
| key_encoding.rs | 702 | ivm/runtime/key_encoding.c | 629 |
| windows.rs | 1342 | ivm/runtime/windows.c | 1386 |
| rank_index.rs | 180 | ivm/runtime/rank_index.c | 198 |
| terminal.rs | 294 | ivm/runtime/terminal.c | 294 |
| evaluation_session.rs | 701 | ivm/runtime/evaluation_session.c | 760 |
| (shared declarations) | - | ivm/runtime/mod.h | 1057 |
| (mirror header) | - | _ext_groove-ivm-rt-b.h | 759 |
| **Total** | **13422** | **.c files only: 12291** (with both headers: 14107) | |

Nothing in scope is left unported.

## Skipped, with reasons

- **Test-only code.** All `#[cfg(test)]` items and test modules are skipped, along with `tests.rs`, `retained_gc_tests.rs`, and the test-only helpers in mod.rs, state.rs and recursion.rs.
- **persist.rs `update_persist`.** It is under `cfg(any())`, so it is never compiled.
- **terminal.rs serde derives.** No browser path serializes these types.
- **Debug-only checks.** `debug_assert!` blocks and `cfg(debug_assertions)` validation are skipped, for example in graph_lifecycle.rs and compilation.rs. A release wasm build drops them too.
- **`JAZZ_CLOSURE_TRACE` env-trace blocks (recursion.rs).** On wasm the env var is always unset, so the branch is dead.
- **The `cold-settle-attribution` feature (recursion.rs and others).** The feature is off in the browser build.

## Stubs and extern mirrors added

The unit includes no rt-a or groove-ivm headers. Everything it calls across that boundary is mirrored in `mod.h` or `_ext_groove-ivm-rt-b.h`, using this unit's type names. The items below are declared here and must be provided by sibling units, or they are names chosen here that a sibling may spell differently:

- **IvmRuntime clone (mod.c, `#[derive(Clone)]` on IvmRuntime, used by `register_table_index`).** It calls these value clones, which this unit expects rt-a to provide:
  - `groove_MultisinkSubscriptionState_clone_v`
  - `groove_RoutedMultisinkShapeState_clone_v`
  - `groove_BindingSources_clone`
  - `groove_BindingDelta_clone`
  - `groove_OperatorState_clone_v`
  
  `CompilationCache::clone` is `Self::default()`, as in Rust.
- **typed_template.c mirrors of groove-ivm template/graph items.** These are declared with this unit's `...B` types:
  - `TemplateGraphInput__with_output_contract`
  - `TemplateGraphInputVec__drop`
  - `bind_template_graphs_inner`
  - `TypedGraphTemplate__bind_declarative_with_arguments`
  - `TypedGraphTemplate__drop`
  - `arguments__bind_project_expression`
  - `CompiledForest__from_vec`
  - `TemplateCacheEntry__drop_v`
  - `GraphBuilder__input_source`, `__clone`, `__drop` and `__map_inputs`
  - `IvmGraph__dedup_node_parts`
  - `subscriptions__graph_builders_equal`
  
  It also uses `groove__schema__DatabaseSchema__new_empty` for `DatabaseSchema::new([])`.
- **schema.c mirrors.**
  - From rt-a's runtime_tick: `flush_pending_binding_retractions`, `hydration_snapshot`, `hydration_snapshot_with_root_values` and `hydration_snapshots`.
  - From rt-a's subscriptions: `count_builder_nodes`, `builder_contains_binding_source` and `add_dedup_schema_indices`. This unit no longer defines the last one, because Rust defines it in subscriptions.rs.
  - From records, schema and graph: `ValueType__collect_variant_registries`, `ValueType__can_evolve_registry_to`, `TableVariant__payload_name_for_shared`, `ProjectField__renamed`, `EnumSchema__case`, `OwnedRecord__new` and `DatabaseSchema__direct_record_store`.
  - Drop functions: `groove_ColumnSchema_drop`, `groove_TableVariant_drop` and `groove_PendingIndexIds_drop`.
- **compilation.c, graph_lifecycle.c and recursion.c** mirror these rt-a items:
  - evaluator (GraphRuntimeView, `operator_state_for`, `validate_arg_by_primary_key_indices`)
  - record_projection (`collect_by_slots`, `collect_by_projections`, `validate_collect_by_terminality`)
  - subscriptions (`infer_builder_output_cached` and `infer_builder_output_uncached`, `record_store_for_table`, `SubscriptionId__retainer_key`)
  - runtime_tick (`PendingIncrementalEvaluation__is_pending` and `__registered_nodes`)

## Mapping notes

- **Async and closures.** Async functions are synchronous: storage is a `groove_Storage` vtable, `scan` includes `collect_scan`, and `write_many` maps to `write_batch`. Closures become a function plus a ctx pointer: the describe callback, visit_inputs and map_inputs.
- **One C copy for two generic instantiations.** JoinBucketMap and RankIndex each have two Rust instantiations but one C copy here. Every `S: OrderedKvStorage` instantiation of schema.rs and recursion.rs is one C function over the vtable.
- **Shared helpers that Rust inlines:**
  - **compilation.c:** the ArgMaxBy and ArgMinBy arms share `compile_arg_by`. Join, SemiJoin and AntiJoin share `add_dedup_join_graph`. The output_positions HashMap is a linear search.
  - **recursion.c:**
    - `group_stored_by_variant` replaces three inlined copies.
    - The `TableSnapshotSource` HashSet is a dedup Vec.
    - Hydration traversal is a Frame stack with a 32-node budget per poll. It is kept across `EvaluationBlocked`.
  - **schema.c:**
    - `build_map_project` replaces the MapProjectOp construction inlined in the Project and Enum case paths.
    - `persist_parts` replaces the Persist-node destructuring repeated in `rebuild_declared_indexes` and `register_table_index_staged`.
    - `variant_source` replaces the repeated source-descriptor lookup.
- **typed_template.c:**
  - Pointer-identity memos (`HashMap<usize, Arc<GraphBuilder>>`) are hmaps keyed by u64 (the pointer value).
  - `DefaultHasher` (SipHash) in `forest_fingerprint` is replaced by `jz_hash_u64` and `jz_hash_combine`.
  - The 128-entry VecDeque cache is a `jz_deque`.
  - `NEXT_TEMPLATE` is a plain static, because wasm has a single thread.
- **schema.c:**
  - `register_table_index` clones the runtime, stages the change on the clone, and then swaps it in or drops it, as `*self = staged` does.
  - `collect::<Option<Vec<_>>>` on schema-index fields becomes an "all mapped" flag.
- **ABI and layout caveats.** These are napkin-level mismatches, and the code still compiles alone:
  - The `add_dedup_schema_index` arity differs from rt-a's declaration. It has an extra nullable out NodeId.
  - The OperatorState mirror is approximate.
  - AsOfStamp in this unit's mod.h has an extra kind field.
  - StorageRequestKey differs from rt-a's.
  - The GraphBuilder mirror is one flat struct, where groove-ivm uses a tagged union. TypedGraphTemplate is reached through the `groove_TypedGraphTemplateB` layout mirror.
  - Storage is `groove_Storage {vt, self}`. rt-a uses `groove_OrderedKvStorage*` and the db mirror uses `groove_DynStorage {self, vt}`.
  - TickMetrics fields are u32 where rt-a uses usize. They are the same size on wasm32.
- **Error path.** One missing AggregateExpr drop on an error path in compilation.c is noted in the source. It leaks only on that path.
- **Error constructors.** Error strings match the Rust `thiserror` messages through the IvmRuntimeError constructors in mod.c (`with_str`, `table_target`, `table_target_version`, `table_target_field`).
