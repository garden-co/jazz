# INV-TX-24

- Status: now
- Coverage: ✓

## Invariant

A caller-generated `OpenTransactionId` MUST name mutable work unchanged across local and worker runtimes, MUST be terminal after commit or rollback, and MUST never be accepted by an API requiring the post-commit `TransactionId`; only successful commit transitions `OpenTransactionId` to `TransactionId`.

## Enforced by (tests)

`jazz::node::tests::harness::open_batch_identity_is_unique_and_terminal`; `jazz::tools::transaction::tests::open_transaction_ids_are_canonical_uuid_v7_values`; `jazz::tools::transaction::tests::committed_transaction_id_is_stable_and_domain_derived`

## Implementation

`jazz/src/tools/transaction.rs::OpenTransactionId`; `jazz/src/tools/transaction.rs::TransactionId`; `jazz/src/node/open_tx.rs::NodeState::open_transaction`; `jazz/src/node/open_tx.rs::NodeState::commit_exclusive`; `jazz/src/node/open_tx.rs::NodeState::commit_mergeable_open`
