# INV-LOWER-25

- Status: now
- Coverage: partial: same-table column add/drop/copy/rename, no table rename/join/array

## Invariant

A lens-projected maintained source MUST emit the same net weighted current-row and witness deltas as applying the selected natural lens path to the authoritative per-version current-row delta stream, with no full-state diff except initial hydration or an explicit reset/rebuild.

## Enforced by (tests)

`jazz::node::tests::lens_projected_maintained::maintained_projected_current_picks_winner_before_lens_projection`

## Implementation

`jazz/src/node/query_eval.rs::CurrentQuerySourceResolver::projected_content_current_source_graph`; `jazz/src/node/query_eval.rs::CurrentQuerySourceResolver::projected_deletion_register_current_source_graph`
