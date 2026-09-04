# INV-QUERY-10

- Status: now
- Coverage: ✓

## Invariant

Include missing-target semantics MUST be local view/API behavior: `JoinMode::Inner` drops parents with unresolvable include targets, `JoinMode::Holes` keeps them, and `require_includes` tightens holes mode by requiring include matches without broadening payload material; sync MUST NOT drop readable parents solely because included targets are absent.

## Enforced by (tests)

`jazz::db::tests::db_query_builder_expresses_s1_shaped_filters_and_include_modes`

## Implementation

`query.rs::Include`; `query.rs::JoinMode`; `node/query_eval.rs::NodeState::apply_include_modes`
