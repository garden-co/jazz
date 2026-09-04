# INV-QUERY-27

- Status: now
- Coverage: ✓

## Invariant

`CollectBy` MUST be an output-terminal-only operator: validation MUST reject it as an input to every graph node, including another collector.

## Enforced by (tests)

`groove::db::tests::collect_by_rejects_join_and_nested_collector_consumers`

## Implementation

`groove/src/ivm/runtime/mod.rs::validate_collect_by_terminality`; `groove/src/ivm/graph.rs::IvmGraph::dedup_node`
