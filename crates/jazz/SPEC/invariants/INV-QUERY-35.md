# INV-QUERY-35

- Status: now
- Coverage: ✓

## Invariant

A delivered change for an aggregate result member the subscriber does not currently hold MUST be delivered as an add, not an update, since a retraction and its replacement can cross on the wire.

## Enforced by (tests)

`jazz::tools::client::tests::aggregate_replacement_for_absent_member_is_normalized_to_an_add`

## Implementation

`jazz/src/tools/client.rs::normalize_subscription_updates`
