# INV-API-17

- Status: now
- Coverage: untested

## Invariant

`Db::connect_upstream` MUST carry already-registered facade subscriptions upstream immediately by placing their `(ValidatedQuery, Binding)` pairs into the connection's pending set.

## Enforced by (tests)

NONE-FOUND

## Implementation

`jazz/src/db.rs::Db::connect_upstream`
