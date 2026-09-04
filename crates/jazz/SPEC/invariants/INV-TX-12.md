# INV-TX-12

- Status: now
- Coverage: ✓

## Invariant

Local durability MUST NOT imply upstream survival; committed local transactions that have not reached an upstream tier MAY be lost if local storage is destroyed.

## Enforced by (tests)

`jazz::node::tests::sync::undelivered_local_commits_are_lost_with_destroyed_client_storage`

## Implementation

`jazz/src/node/open_tx.rs::NodeState::commit_exclusive`; `jazz/src/node::NodeState::commit_mergeable_unit` (symbol inferred from tests; exact definition not read)
