# INV-EDGE-23

- Status: target
- Coverage: untested

## Invariant

A scope-isolated client relay MAY answer same-scope row-version repair without re-evaluating policy only for a version in that scope's previously delivered authorized payload closure or a same-scope authored pending version. Other relays MUST prove that exact closure membership or forward repair to an authority.

## Enforced by (tests)

NONE-FOUND

## Implementation

planned explicit relay repair capability
