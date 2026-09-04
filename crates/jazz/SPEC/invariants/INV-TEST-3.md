# INV-TEST-3

- Status: now
- Coverage: ✓

## Invariant

Every consistency claim gets randomized oracle coverage; the oracle suite covers domination, merge convergence, exclusive validation, and sync convergence.

## Enforced by (tests)

jazz::node::tests::sync::m3_seeded_sync_interleavings_converge_against_oracle

## Implementation

crate::oracle; jazz/src/node/tests/support.rs
