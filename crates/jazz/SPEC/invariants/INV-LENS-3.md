# INV-LENS-3

- Status: next
- Coverage: untested

## Invariant

Catalogue mutation messages MUST be accepted only from an authenticated catalogue-admin transport/session context; serialized author fields are provenance and MUST NOT grant authority.

## Enforced by (tests)

NONE-FOUND

## Implementation

catalogue sync ingress and `jazz/src/node/ingest.rs`
