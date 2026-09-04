# INV-REC-1

- Status: now
- Coverage: ✓

## Invariant

A recursive graph MUST have a seed child and a step child whose output `RecordDescriptor`s are identical; otherwise subscription/compilation MUST fail with `GraphOutputMismatch`.

## Enforced by (tests)

`groove::db::tests::recursive_graphs_reject_seed_and_step_output_descriptor_mismatch`

## Implementation

`groove/src/ivm/runtime/mod.rs::IvmRuntime::add_dedup_graph` (`GraphBuilder::Recursive` arm)
