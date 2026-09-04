# INV-BVIEW-21

- Status: now
- Coverage: ✓

## Invariant

A mergeable first-head copy of an inherited branch-view row MUST carry canonical v1 non-causal source evidence. The authority MUST resolve the declared current or frozen base to the exact non-deleted source version, require the target head row to remain physically absent, and apply ordinary source-row read policy before accepting. The evidence MUST NOT become a parent, read-set, CAS condition, or merge dependency.

## Enforced by (tests)

`jazz::node::tests::harness::branch_view_copy_evidence_authorizes_exact_inherited_source_without_parent`

## Implementation

`db/transactions.rs::Db::stage_mergeable_{update,upsert}_in_branch_view`; `node/source_resolution.rs::NodeState::resolve_branch_view_copy_evidence`; `node/ingest/fates.rs::NodeState::commit_unit_satisfies_write_policies`
