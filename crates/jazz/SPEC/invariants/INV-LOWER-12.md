# INV-LOWER-12

- Status: now
- Coverage: ✓

## Invariant

Schema projection MUST lower as a Groove source-boundary `VariantProject`. Parameter-bound joins over projected rows MUST preserve the source descriptor and payload, including restoring any nullable wrapper removed from a join key. Plans prepared before lens publication MUST remain valid when the lens registers another projection case.

## Enforced by (tests)

`jazz::node::tests::catalogue_lenses::heterogeneous_schema_projected_reads_keep_prepared_plans_valid`; `jazz::node::tests::lens_projected_maintained::maintained_projected_current_picks_winner_before_lens_projection`

## Implementation

`jazz/src/node/query_eval.rs::CurrentQuerySourceResolver::projected_content_current_source_graph`; `jazz/src/node/query_engine/lowering.rs::lower_equality_param_filter_joins`
