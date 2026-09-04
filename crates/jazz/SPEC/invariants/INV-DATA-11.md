# INV-DATA-11

- Status: now
- Coverage: ✓

## Invariant

A merge strategy declaration MUST name an existing user column of the containing `TableSchema`.

## Enforced by (tests)

`jazz::schema::tests::merge_strategy_rejects_unknown_user_column`

## Implementation

`schema.rs::JazzSchema::validated`
