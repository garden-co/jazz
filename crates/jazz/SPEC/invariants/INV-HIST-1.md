# INV-HIST-1

- Status: now
- Coverage: ✓

## Invariant

A row version that lists a parent MUST dominate that parent for content-current selection when both versions are present in the same layer.

## Enforced by (tests)

`jazz::oracle::tests::parent_versions_dominate_their_ancestors`

## Implementation

`jazz/src/node/codec.rs::content_head_indices`
