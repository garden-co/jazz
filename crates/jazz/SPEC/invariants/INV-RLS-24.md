# INV-RLS-24

- Status: target
- Coverage: untested

## Invariant

Client mutation staging MUST NOT issue a definitive read- or write-policy verdict from partial local state. Update/upsert read visibility and write policy are enforced by the fate authority against its complete admitted policy inputs.

## Enforced by (tests)

NONE-FOUND

## Implementation

planned removal of client-local transaction policy preflight
