# INV-TX-6

- Status: now
- Coverage: ✓

## Invariant

A commit unit MUST be rejected with `RejectionReason::CausalityViolation` if its `tx_id.time` is less than or equal to any same-row/layer history parent's `tx_id.time`, and its versions MUST NOT enter history.

## Enforced by (tests)

`jazz::node::tests::general::late_lower_hlc_child_is_rejected_at_admission`; `jazz::node::tests::general::unlawful_child_with_known_parent_rejects_before_global_state`

## Implementation

`jazz/src/node/ingest.rs::NodeState::commit_unit_satisfies_clock_condition`; `jazz/src/node/ingest.rs::NodeState::ingest_commit_unit_once`
