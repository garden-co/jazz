# INV-QUERY-36

- Status: now
- Coverage: ✓

## Invariant

Every settled program fact durable key MUST use the one versioned canonical `JPFK` codec with permanent fact tags. `ResultPayload` descriptors/rows and synthetic result-member row/replacement values MUST be exact ordinary Groove record encodings, never Rust-private postcard bytes. Recovery MUST fail closed before resident query-state mutation on legacy postcard, malformed, truncated, unknown-version/tag, trailing, noncanonical, oversized, or over-nested bytes; add, remove, rewrite, and reopen derive the same key. Storage-freeze #2249.

## Enforced by (tests)

`jazz::node::codec::authority_storage_codec_tests::{peer_source_closure_storage_codec_has_permanent_tags_and_exact_fixtures,covered_input_source_codec_pins_every_role_and_rejects_malformed_paths,program_fact_storage_codec_rejects_legacy_unknown_trailing_and_noncanonical_bytes,nested_settled_result_values_are_canonical_groove_records_and_reject_corruption}`; `jazz::node::tests::sync::known_state::{settled_program_fact_add_remove_rewrite_and_reopen_use_one_durable_key_codec,corrupt_settled_program_fact_recovery_does_not_publish_a_valid_prefix}`

## Implementation

`jazz/src/node/codec.rs::{program_fact_storage_bytes,program_fact_from_storage_bytes}`; `jazz/src/node/{mod.rs,state/durable.rs}`
