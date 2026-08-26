# PosterShop

PosterShop is a deliberately vendor-neutral collaborative gig-poster canvas.
It models the durable data a tldraw-like UI needs—canvases, ordered layers,
shapes, asset metadata, and immutable checkpoint markers—without coupling the
example to a rendering library. The UI is deliberately split into independently
subscribed layer, canvas, cursor, asset-metadata, and checkpoint surfaces so a
high-rate presence write does not require reading an asset shelf or re-running
a renderer-wide query. Cursors are ephemeral presence rows and are never part
of history.

The first slice only exposes settled authorization: admins bootstrap and manage
membership, editors and admins add layers and shapes, and admins add immutable
checkpoints. Asset mutation, layer/shape mutation and deletion, cursor creation,
and checkpoint deletion remain default-deny until their ownership rules are
specified in #1926. The UI keeps those surfaces read-only rather than offering
an action with an undecided authorization contract.

Shape insertion is additionally fail-closed for now. A correct rule must prove
that the editor's membership, the shape canvas, and the referenced layer canvas
are identical. The current core policy conversion cannot publish that correlated
proof, so the app deliberately does not expose shape creation until that core
receipt lands; it does not fall back to a membership-only check.

The follow-up topology receipt will run this ordinary application schema and
policy set through browser → edge → core. Its deterministic timeout/fault
plumbing comes from the shared example topology harness; it does not replace
the application's queries or policy evaluator.

`checkpoints.branch` is only a named marker at this stage. It does not expose a
branch view, choose a winner, or claim canvas-specific concurrent ordering
semantics; those requirements need a separate core contract.

Large asset bytes are intentionally not materialized by this first app slice.
`assets` carries metadata and an optional `fileId`; the conventional file table
integration remains linked to #1833, #1839, and #1844. Shape and asset metadata
must continue to work while that path is red.
