# Unit `model` — jazz_model (crates/jazz/layers/model/src)

All 26 C files under `src/jazz_model/` compile with `build/check.sh` with no errors and no warnings.
`jazz_model.h` holds the unit's own types and prototypes. `_ext_model.h` mirrors the groove and
jazz_types declarations this unit uses.

## Files covered (Rust non-test lines vs C lines)

"Non-test" means everything before the trailing `#[cfg(test)] mod tests`.

| Rust file | Rust non-test | C file | C lines |
|---|---:|---|---:|
| lib.rs | 42 | lib.c | 241 |
| schema.rs | 1901 | schema.c | 1194 |
| tx.rs | 1073 | tx.c | 1019 |
| model/branch.rs | 341 | model/branch.c | 528 |
| model/json_schema.rs | 133 | model/json_schema.c | 113 |
| model/metadata.rs | 209 | model/metadata.c | 24 |
| model/policy_claims.rs | 209 | model/policy_claims.c | 240 |
| model/policy_directory.rs | 333 | model/policy_directory.c | 356 |
| model/public_schema.rs | 112 | model/public_schema.c | 119 |
| model/public_schema_convert.rs | 3391 | model/public_schema_convert.c | 2769 |
| model/transaction.rs | 118 | model/transaction.c | 109 |
| model/public_api/policy.rs | 318 | model/public_api/policy.c | 657 |
| model/public_api/relation_ir.rs | 173 | model/public_api/relation_ir.c | 820 |
| model/public_api/types/policy.rs | 914 | model/public_api/types/policy.c | 125 |
| model/public_api/types/row.rs | 227 | model/public_api/types/row.c | 134 |
| model/public_api/types/schema.rs | 774 | model/public_api/types/schema.c | 845 |
| model/public_api/types/value.rs | 475 | model/public_api/types/value.c | 397 |
| query.rs + query/ast.rs | 44 + 336 | query/ast.c | 786 |
| query/builders.rs | 713 | query/builders.c | 57 |
| query/combinators.rs | 176 | query/combinators.c | 56 |
| query/canonical_request.rs | 922 | query/canonical_request.c | 813 |
| query/policy_and_request_contracts.rs | 870 | query/policy_and_request_contracts.c | 1037 |
| query/policy_limits.rs | 85 | query/policy_limits.c | 43 |
| query/relation_codec.rs | 1239 | query/relation_codec.c | 774 |
| query/relation_resolution.rs | 1451 | query/relation_resolution.c | 1197 |
| query/validation.rs | 2616 | query/validation.c | 2090 |
| (headers) | — | jazz_model.h + _ext_model.h | 1239 + 427 |

Total C: about 18.2k lines, headers included.

C lines run higher than Rust where serde derives became hand-written code: `relation_ir`, the
`public_api::policy` serde and Debug, and the `ast` derives (Clone, PartialEq, Debug, drop, postcard
and JSON). C lines run lower in builders.rs, combinators.rs, types/policy.rs and metadata.rs. Most of
those files is builder or test API that LTO drops, so only the shipped subset was ported.

## Skipped and why

- **Not in the browser CSV (LTO drops it)**:
  - `model/public_api/session.rs`, `model/public_api/types/branch.rs` and `model/schema_lens/*`: no
    symbols in the CSV.
  - Most of `model/metadata.rs`.
  - `admin_catalogue_row_format.rs`.
  - Most of `query/builders.rs` and `combinators.rs`: Gather, flat_join, reachable_via, the
    `inherits_*` builders, `join_via_column` and others.
  - `qualify_flat_join_source_predicate`.
  - The serde impls of `ValidatedQuery` and `Binding`.
  - The parts of the schema serde that are not shipped.
  - The `types/policy.rs` builders. Only Clone and drop of TablePolicies/OperationPolicy ship.
- **Test-only**: `query/tests.rs`, `model/public_api/types/tests.rs`, `gather_agreement_tests.rs`,
  `test_public_schema.rs`, `model/test_support.rs`, `query/doctest_support.rs`, and every
  `#[cfg(test)] mod tests`. The test block of `public_schema_convert.rs` alone is about 2900 lines.
- **Module glue only** (`mod`/`pub use`): `model/mod.rs`, `model/public_api/mod.rs`,
  `types/mod.rs`, `schema_lens/mod.rs`.

## Stubs

None intentional. Everything outside the unit (groove values and descriptors, jazz_types ids and
`AuthorSubject::metadata_type`) is declared only in `_ext_model.h` and links against the other units.

## Notable mappings

- **Errors**: `QueryError` and `SchemaConversionError` became `jz_err`.
  - `QueryError` is `JAZZ_MODEL_ERR_QUERY + variant` plus its Display text, built through a
    vsnprintf helper.
  - `SchemaConversionError` carries the message "`{path}: {message}`".
  - Their `Debug` impls are not ported. Where Rust formats `{error:?}`, C uses the Display text.
  - All error strings match Rust verbatim.
- **`{x:?}` in error messages**: these call the ported `Debug__fmt` of PolicyExpr, Value and
  Operation.
- **Builder inlining**: Builder chains used inside `public_schema_convert` became direct struct
  pushes (`push_join`, the `InheritsVia` push, `push_alternatives`). The fluent `Query` builders are
  therefore not separate symbols.
- **Closures**: Rust closures (`for_each_operand_mut`, retain and dedup predicates, the
  column-operand callbacks in relation_resolution) became function-pointer plus `ctx` pairs.
- **Containers**:
  - VecDeque (ParameterDomain's ready queue) is a vec with a head index.
  - BTreeMap and BTreeSet use `jz_btree` with per-type descriptors.
  - Nested `BTreeMap<String, BTreeMap<..>>` (the typed-literal ColumnTypes) is a btree of btrees.
- **Sorting**: `sort_by_key` in canonical_request precomputes the keys and runs a stable `jz_sort`.
- **Value conversion**: `serde_json::to_value(relation)` in `canonical_relation_query_key` serializes
  to JSON text and re-parses it with `jz_json_parse`.
- **Fused types**:
  - The `Wire*` mirror tree in relation_codec is fused into the codec functions instead of a
    separate type tree.
  - `RelProjectExpr` and `KeyRef` share one C struct.
- **Interned values**: `Intern<String>` (TableName/ColumnName) and `Intern<Uuid>` (ObjectId) are
  pointer handles. groove `RecordDescriptor` equality is pointer equality.
- **Statics**: Thread-locals became file statics.
- **Generics**: Generic helpers instantiated for several element types in Rust (Vec clone, eq, drop)
  are a single `jz_vec_*` routine plus element callbacks in C.
- **Option and Box**: `Option<Box<T>>` is a nullable pointer. `Option<String>` is `jazz_model_OptStr`.
  Enums with payloads are tagged structs with all variant fields inline.
