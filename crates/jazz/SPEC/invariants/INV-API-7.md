# INV-API-7

- Status: now
- Coverage: ✓

## Invariant

Subscription streams MUST expose maintained-view opened/reset/delta events and MUST NOT queue facade-side full-result diffs as the normal live subscription mechanism.

## Enforced by (tests)

`jazz::db::tests::db_facade_subscription_refresh_preserves_read_tier`

## Implementation

`jazz/src/db.rs::SubscriptionStream`; `jazz/src/db.rs::SubscriptionState`; `jazz/src/db.rs::refresh_subscriptions`
