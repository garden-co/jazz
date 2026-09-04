# INV-LOWER-9

- Status: now
- Coverage: ✓

## Invariant

Query lowering MUST begin from a resolved visible-current source and apply deletion visibility before user filters, joins, or reachable traversal.

## Enforced by (tests)

`jazz::node::tests::queries::groove_current_rows_match_oracle_for_seeded_m1_commits`

## Implementation

`jazz/src/node/query_eval.rs::CurrentQuerySourceResolver::resolve_source`
