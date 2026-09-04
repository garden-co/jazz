# INV-HIST-6

- Status: now
- Coverage: ✓

## Invariant

A merge version MUST dominate all of its parent heads and become the current content winner when present and accepted.

## Enforced by (tests)

`jazz::oracle::tests::merge_versions_dominate_concurrent_heads`; `jazz::node::tests::counter_merge::core_creates_merge_versions_for_concurrent_heads`

## Implementation

`jazz/src/node/codec.rs::content_head_indices`; `jazz/src/node/ingest.rs::create_merge_version_if_needed`
