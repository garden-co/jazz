# Unit `engine`: jazz_engine (crates/jazz/layers/engine/src)

All 20 C files under `src/jazz_engine/` compile with `build/check.sh` (`OK (20 files)`). One pass over the unit with `-Wall -Wunused-function` reports no unused statics.

The objects were also compiled with `-c` and their symbols compared with `llvm-nm`. Every `jazz_engine_*` symbol the unit references is defined inside the unit. The only unresolved symbols are mirrored `groove_*`, `jazz_model_*`, `jazz_types_*`, `jazz_protocol_*` and `jz_*` items, which other units provide.

The headers are:
- `query_engine/mod.h`: the public vocabulary, which pulls in `input.h`, `output.h`, `read.h`, `policy.h`, `resolver.h` and `schemas.h`.
- `query_engine/lowering.h`: the lowering-internal items. In Rust these are `pub(super)` items across one module tree.
- `_ext_engine.h`: mirrors of the groove, jazz_model, jazz_types and jazz_protocol declarations this unit uses.

## Files (Rust non-test lines vs C lines)

"Non-test" excludes `#[cfg(test)]` modules, including the two in the middle of terminals.rs.

| Rust file | Rust non-test | C file | C lines |
|---|---:|---|---:|
| lib.rs | 19 | lib.c | 3 |
| query_engine/mod.rs | 35 | query_engine/mod.c (+ mod.h) | 224 (+146) |
| query_engine/fields.rs | 150 | query_engine/fields.c | 184 |
| query_engine/input.rs | 438 | query_engine/input.c (+ input.h) | 1551 (+305) |
| query_engine/output.rs | 87 | query_engine/output.c (+ output.h) | 453 (+104) |
| query_engine/policy.rs | 24 | query_engine/policy.c (+ policy.h) | 67 (+24) |
| query_engine/publication.rs | 76 | query_engine/publication.c | 95 |
| query_engine/read.rs | 240 | query_engine/read.c (+ read.h) | 406 (+75) |
| query_engine/resolver.rs | 210 | query_engine/resolver.c (+ resolver.h) | 428 (+174) |
| query_engine/schemas.rs | 309 | query_engine/schemas.c (+ schemas.h) | 589 (+218) |
| query_engine/binding_values.rs | 70 | query_engine/binding_values.c | 100 |
| query_engine/lowering.rs | 1021 | query_engine/lowering.c (+ lowering.h) | 1185 (+514) |
| lowering/arguments.rs | 241 | lowering/arguments.c | 305 |
| lowering/closure.rs | 691 | lowering/closure.c | 602 |
| lowering/collect_layout.rs | 633 | lowering/collect_layout.c | 626 |
| lowering/graph_lowering.rs | 3834 | lowering/graph_lowering.c | 3415 |
| lowering/planning.rs | 1278 | lowering/planning.c | 1523 |
| lowering/requirements.rs | 424 | lowering/requirements.c | 406 |
| lowering/templates.rs | 280 | lowering/templates.c | 326 |
| lowering/terminals.rs | 3767 | lowering/terminals.c | 3015 |
| **total** | **13827** | .c + .h (incl. `_ext_engine.h` 496) | **17559** |

The data-model files (input, output, schemas, resolver, read) grow most in C. Their Rust `#[derive(Clone, Debug, PartialEq, Ord)]` lines become hand-written clone, drop, eq, cmp and Debug functions. The lowering files are roughly the same size or smaller than the Rust, because iterator chains become loops with few temporaries.

## Skipped (and why)

- **Test code.** All of `query_engine/tests/*` (5,902 lines) is skipped, as are the `#[cfg(test)]` modules inside files (`binding_route_tests` and `publication_schema_tests` in terminals.rs, among others) and test-only hooks such as `binding_value_source_projection_fields_for_test`. PORTING.md asks for this.
- **Dead code in graph_lowering.rs.** These functions have no callers in the crate's non-test code, so they were not ported:
  - `lower_join_key_pairs` and `lower_join_key_pair`
  - `lower_union_relation_input` and `lower_union_relation_input_with_prefix`
  - `linear_root_fields`
- **`trace_template`.** This is debug-env tracing in templates.rs. Only its env-flag guard is mirrored (`jazz_types__debug_env__query_template_trace`); the trace output itself is not ported.
- **Re-exports in lib.rs.** `pub use groove` and `use` re-exports carry no code. lib.c is an include and a comment.

## Stubs

None. Every in-scope function is translated in full. There are no `TODO` bodies.

## Mapping observations

- **Errors.**
  - `CapabilityResult<T>` becomes `jazz_engine_CapErr`, a boxed `CapabilityReport*` where NULL means Ok. The value comes back through out-params.
  - `Result<T, UnsupportedReason>` becomes `jazz_engine_UrErr`, a boxed reason.
  - `.map_err(single_gap_report)?` becomes `JE_UR_CAP_TRY` or `jazz_engine__ur_into_cap`.
  - `?` becomes `JE_CAP_TRY` or `JE_UR_TRY`.
  - Clean-up on error paths uses `goto fail` over zero-initialised locals. A zeroed `jz_btree` or `JZ_VEC` is safe to drop.
- **Ownership.**
  - `GraphBuilder` is a `jz_rc` payload pointer, and constructors consume their inputs.
  - Rust `graph.clone()` becomes `groove_gb_clone`, a refcount bump.
  - Functions that take a `GraphBuilder` by value are documented as `consumes ...` and drop it on every path.
  - Literal enum construction, such as `GraphBuilder::SemiJoin { comparison: Exact, .. }` with `FieldRef` keys, goes through a local `gb_node` helper (`jz_rc_new` plus `GraphBuilder__drop_in_place`).
- **Borrowed views instead of clones.** Where Rust rebuilds a plan by cloning and the copy is only read, C passes a shallow view and frees no elements:
  - `aggregate_input_graph`: the plan with its last step popped becomes a length-1 view of the steps.
  - The witness step plan.
  - The filtered step lists in graph_lowering.
  - `last_join_right`'s plan.
  - `pending_order`.
  - The union terminal plan root.
  - `lower_relation_input_cached`, which borrows its source.

  This removes deep `AnalyzedQueryPlan` clones, which are the largest allocation sites in the Rust.
- **Closures and generics.**
  - Rust key-resolver closures become `{fn, ctx}` structs (`KeyResolver`, `BiKeyCtx`).
  - The collector window-key closures become a `WindowInputFn` with a `void *ctx`.
  - `impl Fn` describe callbacks become `groove_TemplateDescribeFn` plus ctx.
  - The `SourceGraphPreparer` trait becomes a vtable struct.
  - async becomes sync.
- **RefCell / OnceCell.**
  - `LoweringContext`'s `RefCell<ProgramArgumentRecipes>` is a plain mutable pointer.
  - The `OnceCell` parameter domain is a lazily filled slot with a state byte (0 = empty, 1 = Ok, 2 = Err).
- **Containers.**
  - `Vec<(String, ColumnType)>` reuses the `TypedOutputField {name, ty}` layout.
  - `ClaimParameter` has exactly `ProgramClaimParam`'s layout, so one typedef serves both.
  - The relation-input cache is keyed by the plan node's pointer, cast to `uintptr_t` (`relation_input_key`). Rust keys this cache by node identity too.
  - `accumulated_join_fields` uses an `(SourceId, String)` key struct.
- **Enums with data.**
  - `SourceMetadataFields` becomes one struct with generic `a/b/c/opt0/opt1` slots, documented per variant. `hidden_source_fields` relies on that per-variant mapping.
  - `ProgramFactKey` and `NormalizedValueRef` are flat tagged structs.
- **Derives.**
  - `PartialEq` trees were added only where they are used: the template-cache match (`ProjectField`, `ProjectExpr`, `GraphBuilder` eq in `_ext_engine.h`) and the witness-sharing proof in `shared_witness_sinks`.
  - `{:#?}` (alternate Debug) is approximated by the compact Debug output. Its text appears only in error messages.
- **Small behavioural notes.**
  - `projected_multisource_terminal` returns `Option<(SourceId, Vec<_>, bool)>`. The bool is always true when the option is Some, so C drops it.
  - `root_join_occurrence_fields` is pure, so in `lowered_terminals` C computes it once rather than twice as the Rust does.
  - `str::replace(non-alnum, "_")` in sink names is done per UTF-8 character, matching Rust's per-`char` replacement.

## Blocking

Nothing. The unit links only against mirrored external symbols from other units' crates (groove, jazz_model, jazz_types, jazz_protocol, rt).
