# INV-LOWER-21

- Status: planned
- Coverage: [#1777](https://github.com/garden-co/jazz/issues/1777)

## Invariant

One-shot reads, live subscriptions, sync views, and transaction-validation reads MUST consume the same lowered semantic query program; callback/reset/retry/propagation behavior MUST NOT select a second evaluator or become part of query shape identity. Runtime consumers request compiler evidence as app rows plus named terminal facts.

## Enforced by (tests)

`jazz::node::query_engine::tests::compiler_boundary_has_no_usage_or_lifecycle_mode`; `jazz::node::query_engine::tests::read_frontier_facts_are_outputs_not_delivery_profiles`

## Implementation

`jazz/src/node/query_engine/mod.rs::QueryProgramRequest`; `jazz/src/node/query_engine/mod.rs::ReadViewResolver`; `jazz/src/node/query_engine/mod.rs::RowSetNormalizer`; `jazz/src/node/query_engine/mod.rs::ProgramFactKey`; `jazz/src/db.rs::Db::subscribe`; `jazz/src/db.rs::Db::attach_query_with_opts`; `jazz/src/peer.rs::PeerState::query_update_inner`
