# INV-LOWER-16

- Status: planned
- Coverage: [#1777](https://github.com/garden-co/jazz/issues/1777)

## Invariant

Exclusive predicate validation for non-degenerate shape predicates MUST compare predicate-output-set terminal facts for the shape+binding at `base_snapshot.global_base` to the corresponding current predicate-output-set facts.

## Enforced by (tests)

(helper); `jazz::node::tests::queries::filterless_shape_and_degenerate_predicate_validation_agree`

## Implementation

`jazz/src/node/query_engine/mod.rs::PredicateOutputSetSchema`; `jazz/src/node/ingest.rs::NodeState::shape_predicate_changed_after`; `jazz/src/node/ingest.rs::NodeState::exclusive_reads_still_valid`
