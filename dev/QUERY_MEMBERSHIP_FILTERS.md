# Query Membership Filters

## Findings

Reference PR #808 (`Support broader query membership filters`) was used only as
a requirements reference. Its intent is:

- widen typed `where` inputs so `in` works on scalar and reference columns;
- translate scalar `in` lists element-by-element;
- cover array-column `contains` for non-text element types;
- keep TypeScript as typed pass-through, with semantics owned by the Rust core.

Current branch support before implementation:

| Column type                       | `in` validation                                                            | `in` lowering/eval                                    | `contains` validation                                                                | `contains` lowering/eval                                                                                                   |
| --------------------------------- | -------------------------------------------------------------------------- | ----------------------------------------------------- | ------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------- |
| Text/String                       | accepts text values; uuid literals are coercible to strings                | lowered as OR of equality predicates                  | accepts text needle as substring search                                              | `TextContains` is not lowered in maintained core; TS/runtime path still covers text contains through existing surface      |
| Integer / numeric                 | accepts matching numeric values                                            | lowered as OR of equality predicates                  | rejected on scalar columns                                                           | n/a                                                                                                                        |
| Float                             | accepts matching float values                                              | lowered as OR of equality predicates                  | rejected on scalar columns                                                           | n/a                                                                                                                        |
| Boolean                           | accepts boolean values                                                     | lowered as OR of equality predicates                  | rejected on scalar columns                                                           | n/a                                                                                                                        |
| Uuid/reference                    | accepts uuid values; string uuid literals are coercible during lowering    | lowered as OR of equality predicates                  | rejected on scalar columns                                                           | n/a                                                                                                                        |
| Enum                              | accepts enum-compatible values; string/uuid literals are coercible         | lowered as OR of equality predicates                  | rejected on scalar columns                                                           | n/a                                                                                                                        |
| Timestamp                         | accepts timestamp values                                                   | lowered as OR of equality predicates                  | rejected on scalar columns                                                           | n/a                                                                                                                        |
| Bytea                             | accepts byte arrays as whole-column values                                 | lowered as OR of equality predicates                  | rejected by TS adapter; core validation rejects non-array/non-string scalar contains | n/a                                                                                                                        |
| Json                              | accepts JSON as whole-column values through TS adapter                     | core comparability depends on value representation    | rejected by TS adapter                                                               | n/a                                                                                                                        |
| Array<Text>                       | accepts whole-array values for `in`                                        | lowered as OR of equality predicates                  | accepts text element needle                                                          | lowered as Groove `Contains` over array membership                                                                         |
| Array<Integer/Float/Boolean/Uuid> | accepts whole-array values for `in`; accepts element values for `contains` | `in` lowered as OR of whole-array equality predicates | accepts matching element needle                                                      | Groove evaluation supports membership, but lowering coerced the needle against the array field instead of its element type |

Main implementation gap:

- `crates/jazz/src/node/query_engine/lowering.rs::lower_contains` calls
  `coerce_literal_for_source_field` for `ArrayContains`. That helper targets the
  array column type, so a scalar string UUID needle for `UUID[]` is not coerced
  to `Uuid`, and nullable array element handling would also be shaped as the
  whole field rather than the member type. The core validator already checks
  `contains` needles against the array member type.

## Change Classification

- CLEARLY-GOOD: Coerce `ArrayContains` literal needles against the array member
  type, preserving existing text substring behavior as `TextContains`.
- CLEARLY-GOOD: Add public Rust `QueryBuilder::filter_in` pass-through so
  black-box client tests can exercise core `Predicate::In` without private or
  JSON-like query construction.
- CLEARLY-GOOD: Add black-box Rust integration coverage through public
  `JazzClient` APIs, builder-created schemas, and `row_input!` inserts.
- CLEARLY-GOOD: Document the supported matrix in `crates/jazz/SPEC/6_queries.md`.
- CLEARLY-GOOD: Run the focused correctness checks and request the relevant
  CodSpeed comparison for a performance-sensitive change.
- SPECULATIVE: none so far.

## Open Questions

- Literal-vs-column coercion remains intentionally narrow. This work does not
  add broad numeric coercion or string-to-numeric coercion; if broader coercion
  is desired it needs a separate spec decision.
