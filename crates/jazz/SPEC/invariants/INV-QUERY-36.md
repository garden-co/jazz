# INV-QUERY-36

- Status: now
- Coverage: ✓

## Invariant

Every settled program fact durable key MUST use the one versioned canonical `JPFK` codec with permanent fact tags and canonical nested Groove result-member values. Recovery MUST fail closed before resident query-state mutation on legacy postcard, malformed, truncated, unknown-version/tag, trailing, noncanonical, oversized, or over-nested bytes; add, remove, rewrite, and reopen derive the same key. Storage-freeze #2249.

## Enforced by (tests)

`jazz::node::codec::result_member_storage_codec_tests::{program_fact_storage_codec_has_permanent_tags_and_exact_fixtures,program_fact_storage_codec_rejects_legacy_unknown_trailing_and_noncanonical_bytes}`; `jazz::node::tests::harness::corrupt_settled_program_fact_recovery_does_not_publish_a_valid_prefix`

## Implementation

`jazz/src/node/codec.rs::{program_fact_storage_bytes,program_fact_from_storage_bytes}`; `jazz/src/node/{mod.rs,state/durable.rs}`
