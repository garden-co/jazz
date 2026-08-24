# PosterShop

PosterShop is a deliberately vendor-neutral collaborative gig-poster canvas.
It models the durable data a tldraw-like UI needs—canvases, ordered layers,
shapes, asset metadata, and branch checkpoints—without coupling the example to
a rendering library. Cursors are ephemeral presence rows and are never part of
history.

Large asset bytes are intentionally not materialized by this first app slice.
`assets` carries metadata and an optional `fileId`; the conventional file table
integration remains linked to #1833, #1839, and #1844. Shape and asset metadata
must continue to work while that path is red.
