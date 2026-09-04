# INV-ARR-1

- Status: target
- Coverage: —

## Invariant

Every keyed structure (table store, declared index, operator state) MUST be a thin wrapper over the arrangement abstraction: one write path, one probe interface, one identity scheme (`ArrangementKey`) — no parallel paths (ch. 4 §4.6).

## Enforced by (tests)

—

## Implementation

—
