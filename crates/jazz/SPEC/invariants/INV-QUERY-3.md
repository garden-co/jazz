# INV-QUERY-3

- Status: now
- Coverage: ✓

## Invariant

`BindingId` MUST be derived from canonical binding bytes in parameter-name order, and bindings MUST reject missing, unknown, or type-mismatched params.

## Enforced by (tests)

`jazz::query::tests::binding_id_uses_canonical_binding_values`; `jazz::query::tests::binding_type_mismatch_errors`

## Implementation

`query.rs::ValidatedQuery::bind`; `query.rs::canonical_binding_bytes`
