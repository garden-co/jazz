# INV-HIST-5

- Status: now
- Coverage: ✓

## Invariant

An upstream node that observes two or more concurrent mergeable content heads for a row MUST create an accepted mergeable merge version with those heads as parents, unless a content version with the same sorted parent set already exists.

## Enforced by (tests)

`jazz::node::tests::counter_merge::core_creates_merge_versions_for_concurrent_heads`

## Implementation

`jazz/src/node/ingest.rs::create_merge_version_if_needed`
