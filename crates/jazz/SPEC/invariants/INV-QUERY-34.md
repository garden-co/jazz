# INV-QUERY-34

- Status: now
- Coverage: ✓

## Invariant

The non-aggregate fields of a synthetic aggregate row — row id, version, provenance, deletion state — carry no meaning. Producers MUST NOT populate them with interpretable values and consumers MUST NOT read them.

## Enforced by (tests)

`jazz::protocol::synthetic_replacement_token_tests::replacement_token_does_not_expose_a_plausible_revision_value`

## Implementation

`jazz/src/protocol.rs::SyntheticReplacementToken`; `jazz/src/protocol.rs::ResultMemberEntry::Synthetic`
