# INV-BVIEW-2

- Status: now
- Coverage: ✓

## Invariant

The logical branch-local row key MUST be `(PhysicalTableId, BranchKey, RowUuid)` while application object identity remains `RowUuid`.

## Enforced by (tests)

`jazz::tests::branch_views::one_mergeable_transaction_can_atomically_write_multiple_branches`

## Implementation

`node/state/commit.rs::NodeState::commit_mergeable_many`; `schema.rs::TableSchema::global_current_storage_tables`
