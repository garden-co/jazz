# INV-LENS-7

- Status: now
- Coverage: ✓

## Invariant

`CurrentWriteSchema` updates MUST be monotone by `revision`; stale revisions MUST leave `current_write_schema` unchanged.

## Enforced by (tests)

`jazz::node::tests::catalogue_lenses::catalogue_current_write_schema_revision_is_core_ordered`

## Implementation

`jazz/src/node/ingest.rs::NodeState::apply_set_current_write_schema`
