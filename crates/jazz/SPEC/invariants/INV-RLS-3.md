# INV-RLS-3

- Status: now
- Coverage: ✓

## Invariant

`Policy::owner_only(table, column)` MUST compare the named column to `claim("user")`, where `claim("user")` is bound from the canonical authenticated `AuthorSubject`; `claim("sub")` remains the admitted provider subject, and neither value comes from caller-provided query params.

## Enforced by (tests)

`jazz::db::tests::subscriptions::authorization::maintained_subscription_emits_created_by_scoped_insert_after_empty_seed`; `jazz::db::tests::subscriptions::authorization::local_propagating_subscription_emits_created_by_scoped_insert_after_empty_seed`

## Implementation

`jazz/src/schema.rs::Policy::owner_only`; `jazz/src/node/query_eval/prepared_bindings.rs::prepared_claim_value`; `jazz/src/node/query_eval/bindings.rs::default_policy_claim_values`
