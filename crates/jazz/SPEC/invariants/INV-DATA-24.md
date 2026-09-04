# INV-DATA-24

- Status: now
- Coverage: ✓

## Invariant

The frozen transaction record MUST preserve the kind-specific evidence split: mergeable transactions carry immutable row provenance/causal parents but no exclusive snapshot/read-set contract; exclusive transactions carry table-bound snapshot, point-version, absence, predicate-shape, binding, and single-parent write-CAS evidence. Mergeable parents MUST NOT be interpreted as serializable CAS evidence, and exclusive dependencies MUST NOT be inferred or discarded.

## Enforced by (tests)

`jazz::node::tests::exclusive_transactions::exclusive_write_write_first_committer_wins`; `jazz::node::tests::exclusive_transactions::exclusive_predicate_phantom_conflict_rejects`; `jazz::node::tests::exclusive_transactions::exclusive_shape_predicate_is_binding_sensitive`; `jazz::node::tests::harness::version_parents_cannot_cross_branch_keys`

## Implementation

`jazz/src/tx.rs::{Transaction,TxKind,RowRead,AbsentRead,PredicateRead}`; `jazz/src/node/codec.rs::TransactionRowRecord`; `jazz/src/node/ingest/validation.rs::NodeState::validate_exclusive_commit_unit`
