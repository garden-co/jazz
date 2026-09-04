# INV-API-31

- Status: now
- Coverage: ✓

## Invariant

`Db::disconnect` MUST mark the `Db` intentionally offline, disconnect every schema client from its server transport, and leave the local runtime and store alive; `Db::reconnect` MUST clear that marker and reconnect every schema client. A schema client created while intentionally offline MUST remain offline until `reconnect`.

## Enforced by (tests)

`packages/jazz-tools/src/runtime/db.transport.test.ts`

## Implementation

`packages/jazz-tools/src/runtime/db.ts::Db::disconnect`; `packages/jazz-tools/src/runtime/db.ts::Db::reconnect`
