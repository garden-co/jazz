# INV-QUERY-28

- Status: target
- Coverage: untested

## Invariant

For each touched `CollectBy` output occurrence, the terminal MUST suppress byte-equal output. A surviving changed collect group emits exactly one old-record retraction and one new-record addition; an appearing/disappearing group its one addition/retraction. In expand mode each changed occurrence emits exactly one such replacement and each appearing/disappearing occurrence its one addition/retraction. Descriptor-complete scalar key/order inputs MUST make this deterministic.

## Enforced by (tests)

NONE-FOUND

## Implementation

planned unified `CollectBy` terminal evaluator and mode canaries
