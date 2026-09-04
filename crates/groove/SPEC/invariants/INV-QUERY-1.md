# INV-QUERY-1

- Status: now
- Coverage: ✓

## Invariant

A query graph node MUST be identified by the full `NodeDescriptor` consisting of `operator`, ordered `inputs`, and `output`; two incompatible descriptors MUST NOT share a node silently.

## Enforced by (tests)

`groove::db::tests::duplicate_projected_subscriptions_share_graph_nodes_and_gc_eagerly`

## Implementation

`groove/src/ivm/graph.rs::IvmGraph::dedup_node`
