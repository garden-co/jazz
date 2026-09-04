# INV-HIST-7

- Status: now
- Coverage: ✓

## Invariant

A merge version's transaction time MUST be strictly after the maximum made-at time of the observed heads.

## Enforced by (tests)

`jazz::node::tests::counter_merge::core_local_currency_uses_argmax_not_sender_arrival_order`

## Implementation

`jazz/src/node/ingest.rs::create_merge_version_if_needed`
