# INV-TX-17

- Status: now
- Coverage: ✓

## Invariant

Exclusive authority validation MUST reject when an absent row read has become globally present.

## Enforced by (tests)

`jazz::node::tests::exclusive_transactions::exclusive_absent_read_conflict_rejects`

## Implementation

`jazz/src/node/ingest.rs::NodeState::validate_exclusive_commit_unit`
