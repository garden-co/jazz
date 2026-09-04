# INV-DISC-7

- Status: prov
- Coverage: untested

## Invariant

oracle-first testing is part of implementation discipline. The oracle is independent of groove (oracle.rs lines 1-24); seeded M3 runs compare core/global/current/subscription state, exclusive serialization, fate finality, and parking drains against oracle/model state (support.rs lines 1618-1688); harness composition keeps production logic in node submodules and model comparisons in crate::oracle (harness.rs lines 1-4). This is a guidance/process anchor, not runtime conformance.

## Enforced by (tests)

NONE-FOUND

## Implementation
