# INV-RLS-19

- Status: now
- Coverage: ✓

## Invariant

A required include (an `Include` with `JoinMode::Inner` or `require: true`) MUST be treated as resolvable for a non-system reader only when its target row exists as a current row AND satisfies the target table's read policy for that reader. A parent row whose required include target is missing OR unreadable to the reader MUST be dropped from the result set, so required-include membership cannot reveal the existence of a target the reader may not read. `AuthorSubject::SYSTEM` bypasses (INV-RLS-2); optional/`Holes` includes keep the parent and withhold an unreadable target (INV-RLS-5).

## Enforced by (tests)

`jazz::node::tests::policies_rls::required_include_unreadable_target_drops_parent`; `jazz::node::tests::policies_rls::holes_include_unreadable_target_keeps_parent_and_withholds_target`; `jazz::node::tests::policies_rls::system_identity_required_include_uses_existence_only_resolvability`

## Implementation

`jazz/src/node/query_eval.rs::NodeState::apply_include_modes`; `jazz/src/node/query_eval.rs::NodeState::prepare_include_modes`
