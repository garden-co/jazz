# INV-TX-2

- Status: now
- Coverage: ✓

## Invariant

Committing an exclusive transaction MUST store the commit locally as `Fate::Pending` with `DurabilityTier::Local` and emit exactly one `SyncMessage::CommitUnit`.

## Enforced by (tests)

`jazz::node::tests::exclusive_transactions::exclusive_commit_accepts_clean_end_to_end`

## Implementation

`jazz/src/node/open_tx.rs::NodeState::commit_exclusive`
