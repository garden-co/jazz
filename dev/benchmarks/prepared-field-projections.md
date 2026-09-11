# Prepared field projection and the number of projection stages

This slice changes execution of a projection, not query meaning or the wire and
storage formats. It follows the [permissioned-load profile](permissioned-load-structural-profile.md).

## Execution changes

The existing MapProject output was already one BytesMut batch, frozen into
shared Bytes slices. The change removes avoidable intermediate work:

- Preparation validates copy/nullable types and resolves nested record paths.
  The execution loop only finds byte spans; it does not reconstruct descriptors
  or compare source/target types for those fields.
- Constants remain borrowed from the prepared plan. Previously cloning a field
  operation also cloned its constant Vec for every row.
- Nested record selections walk pre-resolved descriptor/index steps and copy
  only the selected encoded field, without building owned parent records.
- Nullable wrapping writes the presence byte and borrowed payload directly.
  Variable offsets are patched in the output buffer, avoiding the intermediate
  generated-payload buffer and per-row variable-field scratch.
- A field needing enum conversion uses the existing semantic conversion only
  for that field. It no longer sends neighboring copied fields through Values
  and a temporary encoded row. This is not yet a direct binary implementation
  of arbitrary recursive enum conversion.
- Hydration's MapProject path now also prepares and uses the field plan once
  for its input batch. The normal tick evaluator retains its cached plans.
- Constant encoding failures are cached during preparation but raised only
  when a row evaluates the expression. Empty input keeps the previous lazy
  semantics (covered by the existing typed enum-parameter integration test).
- When an enum mapping deliberately excludes a row, the batch writer rolls
  back the entire partially written row before proceeding. Weights and output
  order remain unchanged.

The internal tests compare encoded output with the existing semantic evaluator,
count owned field evaluations, verify shared contiguous output ownership, reject
incompatible plans before processing rows, and exercise omission after a prior
variable payload has already been written. Internal mechanism checks are needed
because correct row values alone cannot detect the allocation regression.

## Why there are so many projection stages

The prior per-operator trace counts 2,061,039 input visits during settlement.
This is neither 2M distinct rows nor one single sequential chain: it includes
branches for different terminals and repeated evaluation at Core, relay and
client. Some joined tuples repeat parent data for many children.

Jazz builds its query graph compositionally. Each component publishes a record
shape for its consumer:

1. Physical/current-row adapters select winning versions, adapt schema fields,
   and combine global/ahead candidates where that read tier requires it.
2. Authorization joins attach account claim routes, check policy relations and
   restore source-shaped rows after the join.
3. Supporting-row publication restores exact row/version/route identities,
   attaches immutable version witnesses, and adds event/table/coverage fields.
4. Application collectors rename fields into a collector namespace and select
   the user-visible result shape. Closure graphs separately carry root identity
   and related inputs needed to evaluate or authorize the query.

These stages have real semantics. Materializing every intermediate record is
not necessarily required to preserve them.

Concrete code paths:

- `query_eval/read_sources.rs::projected_branch_content_source_graph` projects
  global candidates, ahead candidates and the winner relation, then applies the
  post-winner projection. Some boundaries protect enum/schema compatibility and
  must remain logically after winner selection.
- `query_eval/authorization.rs::compose_policy_filtered_current_source_graph` and its
  related paths project joined policy data back to the authorized source shape.
- `lowering/terminals.rs::content_version_witness_graph_from_visible_graph`
  projects a witness shape, joins it against the exact visible row/version keys,
  then projects again to restore fields and attach claim routes. The visibility
  restriction must not disappear in any optimization.
- `lowering/closure.rs::lower_closure_membership` projects each contributing
  source back to its source shape plus required routes.
- `lowering/collect_layout.rs::collect_anchor_graph` adds a collector projection,
  including full-row field renames.

The important missing optimization is in Groove: `GraphBuilder::project` and
`project_fields` always construct a Project node, and
`runtime/compilation.rs::add_dedup_unary_graph` lowers each into MapProject.
`IvmGraph::dedup_node` shares exactly equal node descriptors, including their
inputs and output shape. It does not compose adjacent projections or recognize
that differently named descriptors may describe identical bytes.

The trace illustrates the consequences: the dominant child query has wide
15-field physical shapes, 20-field witness shapes with literals/nullability,
23/24-field coverage and collector shapes, and a closure-root copy. On the relay,
claim-route carriers add further variants. Its 10 x 43,000 and 22 x 23,831 large
projection visits include both source preparation and terminal branches.

## Next candidates, without changing query semantics

1. Prove byte-layout equivalence once and let identity/rename projections share
   the input Bytes. The earlier trace found 320,362 identity-shaped input visits,
   an upper-bound candidate count rather than a verified removable count.
2. Compose adjacent field projections into one prepared plan, accounting for
   shared consumers so fusion does not duplicate work. Start with total field
   selection/renaming/constant operations.
3. Project directly at join/collector output boundaries where intermediate
   tuples are immediately discarded; propagate required fields backward while
   retaining authorization routes and version evidence.

Do not move partial enum conversions across winner selection or filtering:
error/omission behavior and authored-schema semantics matter. Do not turn
supporting-row publication into an unrestricted source read. The opportunity is
fewer representation boundaries, not weaker permission or reconciliation rules.
