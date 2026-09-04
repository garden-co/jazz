# INV-DATA-3

- Status: now
- Coverage: ✓

## Invariant

`AuthorSubject::SYSTEM` MUST use the reserved canonical subject `["urn:jazz:system","system"]`, rather than an externally representable author encoding.

## Enforced by (tests)

`jazz::ids::tests::author_subject_is_canonical_json_and_interned`

## Implementation

`ids.rs::AuthorSubject::{SYSTEM,SYSTEM_CANONICAL}`
