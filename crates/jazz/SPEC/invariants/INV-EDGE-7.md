# INV-EDGE-7

- Status: now
- Coverage: ✓

## Invariant

Once a transaction reaches `Fate::Accepted`, later stale `Fate::Pending` updates MUST NOT regress its fate.

## Enforced by (tests)

`jazz::tests::fate_regressions::stale_pending_fate_update_cannot_regress_accepted`

## Implementation

`node/ingest.rs::NodeState::apply_fate_update`
