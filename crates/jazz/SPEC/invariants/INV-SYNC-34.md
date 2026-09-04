# INV-SYNC-34

- Status: target
- Coverage: untested

## Invariant

A subscription is settled only after its receiver has verified every class of the complete reproducible input closure for the authority-declared manifest and epoch: catalogue, authored history, branch, correlation, admission, replacement, settlement, and canonical shape/binding/read-view identity. Reconnect, exact class repair or reset, and recovery MUST re-establish that closure and local IVM quiescence before settlement.

## Enforced by (tests)

NONE-FOUND

## Implementation

planned closure completeness accounting, class-specific repair/reset, and recovery
