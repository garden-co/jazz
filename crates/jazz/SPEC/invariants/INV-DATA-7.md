# INV-DATA-7

- Status: now
- Coverage: ✓

## Invariant

Canonical schema identity MUST change when a column's `MergeStrategy` changes.

## Enforced by (tests)

`jazz::schema::tests::counter_merge_strategy_changes_schema_identity`

## Implementation

`schema.rs::canonical_schema_bytes`; `schema.rs::put_merge_strategy`
