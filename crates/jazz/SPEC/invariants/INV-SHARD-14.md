# INV-SHARD-14

- Status: open
- Coverage: untested

## Invariant

Rebalancing MUST NOT flip partition ownership in the catalogue until the destination shard-core has the partition history needed to serve that ownership and the protocol has defined treatment of in-flight fates/subscriptions.

## Enforced by (tests)

NONE-FOUND

## Implementation

NONE-FOUND
