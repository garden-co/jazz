# INV-BVIEW-4

- Status: now
- Coverage: ✓

## Invariant

Branch columns MUST be immutable after insertion. Moving an object between branch keys requires explicit writes to both branch-local rows, which MAY share one atomic transaction.

## Enforced by (tests)

`jazz::node::tests::harness::version_parents_cannot_cross_branch_keys`; `jazz::tests::branch_views::one_mergeable_transaction_can_atomically_write_multiple_branches` ; `jazz::node::tests::harness::remote_authored_branch_keys_are_validated_atomically_before_storage`

## Implementation

`schema.rs::JazzSchema::project_branch_selector`; `db/transactions.rs::MergeableTxOps`
