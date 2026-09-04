# INV-TICK-6

- Status: now
- Coverage: ✓

## Invariant

An arrangement MUST be a typed, hash-consed graph node whose identity includes its record-producing input, key fields, and comparison semantics. Consumers MUST depend on that node explicitly, so graph reachability governs sharing and lifecycle; runtime state adds evaluation scope to the node identity.

## Enforced by (tests)

groove::ivm::runtime::tests::similar_join_subscriptions_share_context_independent_base_arrangements

## Implementation

groove/src/ivm/runtime/compilation.rs::IvmRuntime::add_arrangement_node; groove/src/ivm/runtime/evaluator.rs::TickEvaluator::arrangement_key
